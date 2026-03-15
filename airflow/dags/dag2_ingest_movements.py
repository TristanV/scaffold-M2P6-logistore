"""
DAG 2 — Ingestion du flux MOUVEMENT

Ce DAG est déclenché automatiquement par le Dataset Airflow
publié par DAG 1 (catalogue_snapshot.parquet).

Ce DAG :
1. Charge le fichier mouvements depuis data/inbox/movements/
2. Valide le schéma Pydantic (MovementRecordV1)
3. Vérifie l'existence du SKU dans la table products (PostgreSQL)
   - SKU connu → insertion dans movements
   - SKU inconnu → insertion dans rejected_movements + rapport de rejet
4. Exporte les mouvements valides en Parquet
5. Génère un rapport JSON de rejet si nécessaire

TODO étudiant : implémenter les fonctions marquées TODO ci-dessous.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow.datasets import Dataset
from airflow.decorators import dag, task

CATALOGUE_DATASET = Dataset("file:///opt/airflow/data/curated/catalogue_snapshot.parquet")
MOVEMENTS_DATASET = Dataset("file:///opt/airflow/data/curated/movements_history.parquet")

DATA_INBOX = Path("/opt/airflow/data/inbox/movements")
DATA_CURATED = Path("/opt/airflow/data/curated")
DATA_REJECTED = Path("/opt/airflow/data/rejected")


@dag(
    dag_id="ingest_movements",
    schedule=[CATALOGUE_DATASET],  # Déclenché par DAG 1
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["logistore", "movements", "ingestion"],
    doc_md="""## DAG 2 — Ingestion Mouvements\n\nIngère les mouvements de stock, rejette ceux sur SKU inconnus.""",
)
def ingest_movements():

    @task
    def load_movements_file() -> str | None:
        """Retourne le chemin du dernier fichier mouvements disponible."""
        files = sorted(DATA_INBOX.glob("*.csv"), key=lambda f: f.stat().st_mtime, reverse=True)
        if not files:
            print("Aucun fichier mouvements trouvé.")
            return None
        return str(files[0])

    @task
    def validate_schema(filepath: str | None) -> dict:
        """
        TODO étudiant :
        1. Charger le fichier CSV
        2. Valider chaque ligne avec MovementRecordV1
        3. Retourner {valid_rows: [...], invalid_rows: [...], filepath: filepath}
        """
        if not filepath:
            return {"valid_rows": [], "invalid_rows": [], "filepath": None}
        # TODO : implémenter
        raise NotImplementedError("validate_schema non implémenté")

    @task
    def check_sku_and_route(validation_result: dict) -> dict:
        """
        TODO étudiant :
        1. Pour chaque ligne valide (schema OK), vérifier que le SKU existe dans products
        2. Lignes avec SKU connu → accepted_rows
        3. Lignes avec SKU inconnu → rejected_rows avec rejection_reason='unknown_sku'
        4. Insérer rejected_rows dans la table rejected_movements
        5. Retourner {accepted: [...], rejected_count: N, total: M}

        C'est le coeur du projet : gérer l'asynchronisme entre les deux flux.
        """
        # TODO : implémenter
        raise NotImplementedError("check_sku_and_route non implémenté")

    @task(outlets=[MOVEMENTS_DATASET])
    def persist_valid_movements(routing_result: dict) -> dict:
        """
        TODO étudiant :
        1. Insérer les mouvements acceptés dans la table movements (PostgreSQL)
        2. Append dans data/curated/movements_history.parquet
        3. Générer le rapport de rejet JSON si rejected_count > 0
        4. Le décorateur outlets=[MOVEMENTS_DATASET] déclenche DAG 3
        """
        # TODO : implémenter
        raise NotImplementedError("persist_valid_movements non implémenté")

    filepath = load_movements_file()
    validation = validate_schema(filepath)
    routing = check_sku_and_route(validation)
    persist_valid_movements(routing)


ingest_movements()
