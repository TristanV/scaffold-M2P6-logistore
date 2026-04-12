"""
DAG 1 — Ingestion du flux CATALOGUE

Ce DAG :
1. Détecte un nouveau fichier catalogue dans data/inbox/catalogue/
2. Valide chaque ligne avec le contrat CatalogueRecordV1 (ou V2)
3. Insère les produits valides dans PostgreSQL (UPSERT)
4. Exporte le catalogue complet en Parquet (data/curated/catalogue_snapshot.parquet)
5. Publie le Dataset Airflow pour déclencher automatiquement DAG 2
"""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pydantic import ValidationError

from airflow.datasets import Dataset
from airflow.decorators import dag, task

from contracts.catalogue_contract import get_catalogue_contract

# Dataset Airflow partagé avec DAG 2
CATALOGUE_DATASET = Dataset("file:///opt/airflow/data/curated/catalogue_snapshot.parquet")

DATA_INBOX = Path("/opt/airflow/data/inbox/catalogue")
DATA_CURATED = Path("/opt/airflow/data/curated")
DATA_REJECTED = Path("/opt/airflow/data/rejected/catalogue")

DSN = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "logistore"),
    "user": os.getenv("POSTGRES_USER", "logistore"),
    "password": os.getenv("POSTGRES_PASSWORD", "logistore"),
}


@dag(
    dag_id="ingest_catalogue",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["logistore", "catalogue", "ingestion"],
    doc_md="""## DAG 1 — Ingestion Catalogue\n\nIngère les fichiers CSV catalogue et publie le Dataset pour DAG 2.""",
)
def ingest_catalogue():

    @task
    def detect_new_catalogue_file() -> str | None:
        """Retourne le chemin du dernier fichier catalogue disponible, ou None."""
        files = sorted(DATA_INBOX.glob("*.csv"), key=lambda f: f.stat().st_mtime, reverse=True)
        if not files:
            print("Aucun fichier catalogue trouvé.")
            return None
        path = str(files[0])
        print(f"Fichier détecté : {path}")
        return path

    @task
    def validate_and_upsert_catalogue(filepath: str | None) -> dict:
        """Valide le CSV catalogue avec Pydantic, UPSERT les valides dans PostgreSQL."""
        if not filepath:
            return {"valid": 0, "rejected": 0, "skipped": True}

        df = pd.read_csv(filepath)
        valid_records, rejected_records = [], []

        for _, row in df.iterrows():
            row_dict = row.to_dict()
            # pandas lit schema_version comme float (1.0) ; Pydantic attend str "1.0"
            row_dict["schema_version"] = str(row_dict.get("schema_version", "1.0"))
            try:
                version = row_dict["schema_version"]
                model_class = get_catalogue_contract(version)
                record = model_class(**row_dict)
                valid_records.append(record.model_dump())
            except (ValidationError, ValueError) as e:
                row_dict["rejection_reason"] = str(e)
                rejected_records.append(row_dict)

        # UPSERT des lignes valides
        if valid_records:
            conn = psycopg2.connect(**DSN)
            try:
                with conn.cursor() as cur:
                    sql = """
                        INSERT INTO products (sku, label, category, unit, min_stock, supplier_id, published_at)
                        VALUES %s
                        ON CONFLICT (sku) DO UPDATE SET
                            label = EXCLUDED.label,
                            category = EXCLUDED.category,
                            unit = EXCLUDED.unit,
                            min_stock = EXCLUDED.min_stock,
                            supplier_id = EXCLUDED.supplier_id,
                            published_at = EXCLUDED.published_at
                    """
                    values = [
                        (
                            r["sku"], r["label"], r["category"], r["unit"],
                            r["min_stock"], r.get("supplier_id"), r["published_at"],
                        )
                        for r in valid_records
                    ]
                    execute_values(cur, sql, values)
                conn.commit()
            finally:
                conn.close()

        # Sauvegarder les rejets
        if rejected_records:
            DATA_REJECTED.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(rejected_records).to_csv(
                DATA_REJECTED / "catalogue_rejected.csv", index=False
            )

        stats = {"valid": len(valid_records), "rejected": len(rejected_records)}
        print(f"Ingestion catalogue : {stats['valid']} insérés/mis à jour, {stats['rejected']} rejetés")
        return stats

    @task(outlets=[CATALOGUE_DATASET])
    def export_catalogue_to_parquet(stats: dict) -> None:
        """Exporte la table products complète en Parquet pour déclencher DAG 2."""
        DATA_CURATED.mkdir(parents=True, exist_ok=True)
        print(f"Stats ingestion : {stats}")

        conn = psycopg2.connect(**DSN)
        try:
            df = pd.read_sql("SELECT * FROM products", conn)
        finally:
            conn.close()

        parquet_path = DATA_CURATED / "catalogue_snapshot.parquet"
        df.to_parquet(parquet_path, index=False)
        print(f"Export Parquet : {len(df)} produits → {parquet_path}")

    # Chaînage des tâches
    filepath = detect_new_catalogue_file()
    stats = validate_and_upsert_catalogue(filepath)
    export_catalogue_to_parquet(stats)


ingest_catalogue()
