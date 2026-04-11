"""
DAG 3 — Calcul des KPIs d'inventaire

Déclenché automatiquement par le Dataset publié par DAG 2.

Ce DAG :
1. Lit catalogue + mouvements depuis Parquet (DuckDB)
2. Joint les deux flux et calcule le stock courant par SKU
3. Classe chaque SKU en OK / WARNING / ALERT selon min_stock
4. Produit un rapport CSV horodaté dans data/reports/
5. Log un résumé (compteurs par statut + top alertes)
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import duckdb
from airflow.datasets import Dataset
from airflow.decorators import dag, task

MOVEMENTS_DATASET = Dataset("file:///opt/airflow/data/curated/movements_history.parquet")

DATA_CURATED = Path("/opt/airflow/data/curated")
DATA_REPORTS = Path("/opt/airflow/data/reports")

INVENTORY_STATUS_SQL = """
SELECT
    p.sku,
    p.label,
    p.category,
    p.unit,
    p.min_stock,
    COALESCE(SUM(m.quantity), 0)::BIGINT AS current_stock,
    CASE
        WHEN COALESCE(SUM(m.quantity), 0) <= 0           THEN 'ALERT'
        WHEN COALESCE(SUM(m.quantity), 0) < p.min_stock  THEN 'WARNING'
        ELSE 'OK'
    END AS stock_status
FROM read_parquet('{catalogue}') p
LEFT JOIN read_parquet('{movements}') m USING (sku)
GROUP BY p.sku, p.label, p.category, p.unit, p.min_stock
ORDER BY current_stock ASC
"""


@dag(
    dag_id="inventory_analytics",
    schedule=[MOVEMENTS_DATASET],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["logistore", "analytics"],
    doc_md="""## DAG 3 — Inventory Analytics

Calcule le stock courant par SKU à partir des Parquet curés
(catalogue + mouvements) et publie un rapport avec les statuts
**OK / WARNING / ALERT**.

Le moteur OLAP utilisé est **DuckDB** : il lit directement les
Parquet sans staging intermédiaire — c'est la version "Parquet"
de la comparaison SQL vs Parquet (cf. `scripts/benchmark_queries.py`).
""",
)
def inventory_analytics():

    @task
    def compute_inventory_status() -> str:
        """
        Joint catalogue + mouvements et produit le rapport CSV.
        Retourne le chemin du rapport, ou "" si les Parquet sont absents.
        """
        catalogue_file = DATA_CURATED / "catalogue_snapshot.parquet"
        movements_file = DATA_CURATED / "movements_history.parquet"

        if not catalogue_file.exists() or not movements_file.exists():
            print(f"Parquet absents — catalogue={catalogue_file.exists()} "
                  f"movements={movements_file.exists()}. Rien à calculer.")
            return ""

        conn = duckdb.connect()
        query = INVENTORY_STATUS_SQL.format(
            catalogue=catalogue_file.as_posix(),
            movements=movements_file.as_posix(),
        )
        stock_df = conn.execute(query).df()

        DATA_REPORTS.mkdir(parents=True, exist_ok=True)
        report_date = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = DATA_REPORTS / f"inventory_report_{report_date}.csv"
        stock_df.to_csv(out_path, index=False)
        print(f"Rapport généré : {out_path} ({len(stock_df)} SKUs)")
        return str(out_path)

    @task
    def summarize_alerts(report_path: str) -> dict:
        """
        Lit le rapport, log les compteurs par statut et le top des alertes.
        Persiste un résumé JSON à côté du CSV.
        """
        if not report_path:
            return {"skipped": True}

        conn = duckdb.connect()
        summary = conn.execute(f"""
            SELECT stock_status, COUNT(*) AS nb
            FROM read_csv_auto('{report_path}')
            GROUP BY stock_status
            ORDER BY stock_status
        """).fetchall()
        counts = {row[0]: row[1] for row in summary}

        top_alerts = conn.execute(f"""
            SELECT sku, label, current_stock, min_stock
            FROM read_csv_auto('{report_path}')
            WHERE stock_status = 'ALERT'
            ORDER BY current_stock ASC
            LIMIT 10
        """).fetchall()

        total = sum(counts.values())
        print(f"\n=== Résumé inventaire ({total} SKUs) ===")
        for status in ("OK", "WARNING", "ALERT"):
            n = counts.get(status, 0)
            pct = (n / total * 100) if total else 0
            print(f"  {status:8s} : {n:6d}  ({pct:5.1f}%)")

        if top_alerts:
            print("\nTop 10 ALERT (stock le plus bas) :")
            for sku, label, stock, mn in top_alerts:
                print(f"  {sku}  stock={stock:5d}  min={mn:3d}  {label[:40]}")

        json_path = Path(report_path).with_suffix(".summary.json")
        json_path.write_text(json.dumps({
            "report": report_path,
            "total_skus": total,
            "counts": counts,
            "top_alerts": [
                {"sku": s, "label": l, "current_stock": c, "min_stock": m}
                for (s, l, c, m) in top_alerts
            ],
        }, indent=2, ensure_ascii=False))
        print(f"\nRésumé JSON : {json_path}")

        return {"total_skus": total, "counts": counts}

    summarize_alerts(compute_inventory_status())


inventory_analytics()
