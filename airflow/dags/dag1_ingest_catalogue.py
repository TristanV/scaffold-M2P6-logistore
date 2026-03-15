"""
DAG 1 — Ingestion du flux CATALOGUE

Ce DAG :
1. Détecte un nouveau fichier catalogue dans data/inbox/catalogue/
2. Valide chaque ligne avec le contrat CatalogueRecordV1 (ou V2)
3. Insère les produits valides dans PostgreSQL (UPSERT)
4. Exporte le catalogue complet en Parquet (data/curated/catalogue_snapshot.parquet)
5. Publie le Dataset Airflow pour déclencher automatiquement DAG 2

TODO étudiant : implémenter les fonctions marquées TODO ci-dessous.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow.datasets import Dataset
from airflow.decorators import dag, task

# Dataset Airflow partagé avec DAG 2
CATALOGUE_DATASET = Dataset("file:///opt/airflow/data/curated/catalogue_snapshot.parquet")

DATA_INBOX = Path("/opt/airflow/data/inbox/catalogue")
DATA_CURATED = Path("/opt/airflow/data/curated")


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
        """
        TODO étudiant :
        1. Charger le fichier CSV filepath avec pandas
        2. Pour chaque ligne, déterminer la version du contrat (schema_version)
           et valider avec get_catalogue_contract(version)
        3. Séparer les lignes valides des lignes invalides
        4. Insérer/mettre à jour (UPSERT) les lignes valides dans PostgreSQL
        5. Sauvegarder les lignes invalides dans data/rejected/catalogue/
        6. Retourner un dict de stats : {valid: N, rejected: M}
        """
        if not filepath:
            return {"valid": 0, "rejected": 0, "skipped": True}
        # TODO : implémenter
        raise NotImplementedError("validate_and_upsert_catalogue non implémenté")

    @task(outlets=[CATALOGUE_DATASET])
    def export_catalogue_to_parquet(stats: dict) -> None:
        """
        TODO étudiant :
        1. Lire tous les produits depuis PostgreSQL
        2. Exporter en Parquet dans data/curated/catalogue_snapshot.parquet
        3. Le décorateur outlets=[CATALOGUE_DATASET] déclenche automatiquement DAG 2
        """
        DATA_CURATED.mkdir(parents=True, exist_ok=True)
        print(f"Stats ingestion : {stats}")
        # TODO : implémenter
        raise NotImplementedError("export_catalogue_to_parquet non implémenté")

    # Chaînage des tâches
    filepath = detect_new_catalogue_file()
    stats = validate_and_upsert_catalogue(filepath)
    export_catalogue_to_parquet(stats)


ingest_catalogue()
