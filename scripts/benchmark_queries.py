"""
Benchmark comparatif SQL (PostgreSQL) vs Parquet (DuckDB).

Mesure le temps d'exécution de requêtes analytiques identiques sur :
  - PostgreSQL (OLTP, données chargées via COPY)
  - DuckDB lisant directement des fichiers Parquet (OLAP)

Le script est autonome : il prépare les données dans les deux moteurs
à partir des CSV générés par scripts/generate_flows.py — pas besoin
d'attendre les DAGs Airflow.

Usage :
    python scripts/benchmark_queries.py --palier small
    python scripts/benchmark_queries.py --palier medium --runs 5
    python scripts/benchmark_queries.py --palier small --ci   # mode CI
"""
import argparse
import os
import statistics
import time
from io import StringIO
from pathlib import Path

import duckdb
import pandas as pd
import psycopg2

DSN = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "logistore"),
    "user": os.getenv("POSTGRES_USER", "logistore"),
    "password": os.getenv("POSTGRES_PASSWORD", "logistore"),
}

DATA_INBOX_CAT = Path("data/inbox/catalogue")
DATA_INBOX_MOV = Path("data/inbox/movements")
DATA_CURATED = Path("data/curated")


# ─────────────────────────────────────────────────────────────────────
#  Requêtes : la même logique pour les deux moteurs.
#  Seules les sources changent : tables Postgres OU read_parquet().
# ─────────────────────────────────────────────────────────────────────

INVENTORY_STATUS = """
SELECT
    p.sku,
    p.category,
    p.min_stock,
    COALESCE(SUM(m.quantity), 0) AS current_stock,
    CASE
        WHEN COALESCE(SUM(m.quantity), 0) <= 0           THEN 'ALERT'
        WHEN COALESCE(SUM(m.quantity), 0) < p.min_stock  THEN 'WARNING'
        ELSE 'OK'
    END AS stock_status
FROM {products} p
LEFT JOIN {movements} m ON m.sku = p.sku
GROUP BY p.sku, p.category, p.min_stock
ORDER BY current_stock ASC
"""

STOCK_BY_CATEGORY = """
SELECT
    p.category,
    COUNT(DISTINCT p.sku) AS nb_skus,
    COALESCE(SUM(m.quantity), 0) AS total_stock
FROM {products} p
LEFT JOIN {movements} m ON m.sku = p.sku
GROUP BY p.category
ORDER BY total_stock DESC
"""

TOP_MOVED_SKUS = """
SELECT
    sku,
    COUNT(*) AS nb_movements,
    SUM(ABS(quantity)) AS volume_total
FROM {movements}
GROUP BY sku
ORDER BY volume_total DESC
LIMIT 20
"""

QUERIES = {
    "inventory_status": INVENTORY_STATUS,
    "stock_by_category": STOCK_BY_CATEGORY,
    "top_moved_skus": TOP_MOVED_SKUS,
}


# ─────────────────────────────────────────────────────────────────────
#  Préparation des données
# ─────────────────────────────────────────────────────────────────────

def ensure_parquet(palier: str) -> tuple[Path, Path]:
    """
    Convertit les CSV en Parquet une fois pour toutes (cache disque).
    Filtre les mouvements orphelins pour aligner avec ce que ferait DAG 2.
    """
    DATA_CURATED.mkdir(parents=True, exist_ok=True)
    cat_csv = DATA_INBOX_CAT / f"catalogue_{palier}.csv"
    mov_csv = DATA_INBOX_MOV / f"movements_{palier}.csv"
    cat_parquet = DATA_CURATED / f"catalogue_{palier}.parquet"
    mov_parquet = DATA_CURATED / f"movements_{palier}.parquet"

    if not cat_csv.exists() or not mov_csv.exists():
        raise FileNotFoundError(
            f"CSV manquants pour le palier '{palier}'. "
            f"Exécutez d'abord : python scripts/generate_flows.py --palier {palier}"
        )

    if cat_parquet.exists() and mov_parquet.exists():
        return cat_parquet, mov_parquet

    print(f"  → conversion CSV → Parquet ({palier})...")
    cat_df = pd.read_csv(cat_csv)
    mov_df = pd.read_csv(mov_csv)
    valid = mov_df["sku"].isin(set(cat_df["sku"]))
    n_orph = (~valid).sum()
    mov_df = mov_df[valid].reset_index(drop=True)
    cat_df.to_parquet(cat_parquet, index=False)
    mov_df.to_parquet(mov_parquet, index=False)
    print(f"    catalogue : {len(cat_df):,} lignes → {cat_parquet.name}")
    print(f"    movements : {len(mov_df):,} lignes (filtré {n_orph:,} orphelins) → {mov_parquet.name}")
    return cat_parquet, mov_parquet


def ensure_postgres(palier: str) -> dict:
    """
    Charge catalogue + mouvements (filtrés) dans Postgres via COPY.
    Idempotent : TRUNCATE puis insertion. Saute si la table contient déjà
    le bon nombre de lignes.
    """
    cat_csv = DATA_INBOX_CAT / f"catalogue_{palier}.csv"
    mov_csv = DATA_INBOX_MOV / f"movements_{palier}.csv"
    cat_df = pd.read_csv(cat_csv)
    mov_df = pd.read_csv(mov_csv)
    valid_skus = set(cat_df["sku"])
    mov_df = mov_df[mov_df["sku"].isin(valid_skus)].reset_index(drop=True)

    conn = psycopg2.connect(**DSN)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM products")
            n_products = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM movements")
            n_movements = cur.fetchone()[0]

            if n_products == len(cat_df) and n_movements == len(mov_df):
                print(f"  → Postgres déjà chargé ({n_products} products, {n_movements} movements)")
                return {"products": n_products, "movements": n_movements, "skipped": True}

            print(f"  → chargement Postgres ({palier})...")
            cur.execute("TRUNCATE movements, products RESTART IDENTITY")

            cat_buf = StringIO()
            cat_df[["sku", "label", "category", "unit", "min_stock", "published_at"]] \
                .to_csv(cat_buf, index=False, header=False)
            cat_buf.seek(0)
            cur.copy_expert(
                "COPY products (sku, label, category, unit, min_stock, published_at) "
                "FROM STDIN WITH CSV",
                cat_buf,
            )

            mov_buf = StringIO()
            mov_df[["movement_id", "sku", "movement_type", "quantity", "reason", "occurred_at"]] \
                .to_csv(mov_buf, index=False, header=False)
            mov_buf.seek(0)
            cur.copy_expert(
                "COPY movements (movement_id, sku, movement_type, quantity, reason, occurred_at) "
                "FROM STDIN WITH CSV",
                mov_buf,
            )

            cur.execute("ANALYZE products")
            cur.execute("ANALYZE movements")

        conn.commit()
        print(f"    catalogue : {len(cat_df):,} lignes")
        print(f"    movements : {len(mov_df):,} lignes")
        return {"products": len(cat_df), "movements": len(mov_df), "skipped": False}
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────
#  Exécution + chronométrage
# ─────────────────────────────────────────────────────────────────────

def time_query(execute_fn, runs: int) -> tuple[float, int]:
    """Exécute la requête `runs` fois, retourne (médiane_ms, nb_lignes)."""
    n_rows = 0
    timings = []
    for _ in range(runs):
        start = time.perf_counter()
        n_rows = execute_fn()
        timings.append((time.perf_counter() - start) * 1000)
    return statistics.median(timings), n_rows


def bench_duckdb(cat_parquet: Path, mov_parquet: Path, runs: int) -> dict:
    conn = duckdb.connect()
    sources = {
        "products": f"read_parquet('{cat_parquet.as_posix()}')",
        "movements": f"read_parquet('{mov_parquet.as_posix()}')",
    }
    results = {}
    for name, template in QUERIES.items():
        sql = template.format(**sources)
        def run():
            return len(conn.execute(sql).fetchall())
        run()  # warmup (cache filesystem + plan)
        ms, rows = time_query(run, runs)
        results[name] = {"ms": ms, "rows": rows}
    conn.close()
    return results


def bench_postgres(runs: int) -> dict:
    conn = psycopg2.connect(**DSN)
    sources = {"products": "products", "movements": "movements"}
    results = {}
    try:
        for name, template in QUERIES.items():
            sql = template.format(**sources)
            with conn.cursor() as cur:
                def run(_cur=cur, _sql=sql):
                    _cur.execute(_sql)
                    return len(_cur.fetchall())
                run()  # warmup (shared buffers)
                ms, rows = time_query(run, runs)
            results[name] = {"ms": ms, "rows": rows}
    finally:
        conn.close()
    return results


# ─────────────────────────────────────────────────────────────────────
#  Rapport
# ─────────────────────────────────────────────────────────────────────

def print_comparison(palier: str, runs: int, duck: dict, pg: dict) -> None:
    print()
    print("=" * 76)
    print(f"  Benchmark — palier '{palier}'  ·  {runs} runs (warmup ignoré, médiane retenue)")
    print("=" * 76)
    print()
    header = f"  {'Query':<22} {'DuckDB/Parquet':>16} {'Postgres SQL':>16} {'Ratio':>10} {'Rows':>8}"
    print(header)
    print("  " + "─" * (len(header) - 2))

    duck_total = pg_total = 0.0
    for name in QUERIES:
        d = duck[name]["ms"]
        p = pg[name]["ms"]
        ratio = p / d if d > 0 else float("inf")
        winner = "DuckDB" if d < p else "Postgres"
        sign = f"{ratio:.2f}× ({winner[:1]})"
        print(f"  {name:<22} {d:>13.2f} ms {p:>13.2f} ms {sign:>10} {duck[name]['rows']:>8}")
        duck_total += d
        pg_total += p

    print("  " + "─" * (len(header) - 2))
    overall = pg_total / duck_total if duck_total > 0 else float("inf")
    fastest = "DuckDB/Parquet" if duck_total < pg_total else "Postgres SQL"
    print(f"  {'TOTAL':<22} {duck_total:>13.2f} ms {pg_total:>13.2f} ms {overall:>9.2f}×")
    print()
    print(f"  → Le moteur le plus rapide sur ce palier : {fastest}")
    print(f"    (gain global : ×{overall:.2f})")
    print()


# ─────────────────────────────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--palier", choices=["small", "medium", "large"], default="small")
    parser.add_argument("--runs", type=int, default=3, help="Nombre de runs par requête (médiane)")
    parser.add_argument("--ci", action="store_true", help="Mode CI : skip si CSV absents")
    args = parser.parse_args()

    print(f"\nBenchmark LogiStore — palier '{args.palier}'")
    print("-" * 50)

    cat_csv = DATA_INBOX_CAT / f"catalogue_{args.palier}.csv"
    if not cat_csv.exists():
        msg = f"CSV absent : {cat_csv}"
        if args.ci:
            print(f"ℹ  Mode CI : {msg} — benchmark ignoré.")
            return
        print(f"⚠  {msg}")
        print(f"   Lancez : python scripts/generate_flows.py --palier {args.palier}")
        return

    cat_parquet, mov_parquet = ensure_parquet(args.palier)
    ensure_postgres(args.palier)

    print(f"\n  Exécution des requêtes ({args.runs} runs chacune)...")
    duck = bench_duckdb(cat_parquet, mov_parquet, args.runs)
    pg = bench_postgres(args.runs)

    print_comparison(args.palier, args.runs, duck, pg)


if __name__ == "__main__":
    main()
