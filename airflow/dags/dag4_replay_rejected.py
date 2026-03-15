"""
DAG 4 — Rejeu des mouvements rejetés

Déclenché MANUELLEMENT par un opérateur, après :
1. Vérification que de nouveaux produits ont été intégrés dans le catalogue.
2. Identification des mouvements rejetés dont le SKU est maintenant connu.

Ce DAG :
1. Lit la table rejected_movements (status='PENDING') depuis PostgreSQL
2. Vérifie quels SKUs sont désormais présents dans products
3. Reinsère ces mouvements dans la table movements
4. Met à jour leur statut en 'REPLAYED'
5. Génère un rapport de rejeu

TODO étudiant : implémenter les fonctions marquées TODO ci-dessous.
"""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="replay_rejected_movements",
    schedule=None,  # Déclenché uniquement manuellement
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["logistore", "replay", "rejected"],
    doc_md="""## DAG 4 — Rejeu des mouvements rejetés

Déclencher ce DAG manuellement après avoir intégré de nouveaux produits
dans le catalogue. Il tentera de réintégrer les mouvements en attente
dont le SKU est désormais connu.""",
)
def replay_rejected_movements():

    @task
    def fetch_pending_rejected() -> list[dict]:
        """
        TODO étudiant :
        1. Requête PostgreSQL : SELECT * FROM rejected_movements WHERE status='PENDING'
        2. Retourner la liste des mouvements rejetés en attente
        """
        # TODO : implémenter
        raise NotImplementedError("fetch_pending_rejected non implémenté")

    @task
    def filter_now_known_skus(rejected: list[dict]) -> dict:
        """
        TODO étudiant :
        1. Extraire la liste des SKUs uniques des rejected
        2. Vérifier lesquels existent maintenant dans products
        3. Séparer : replayable (SKU maintenant connu) vs still_pending
        4. Retourner {replayable: [...], still_pending: [...], counts: {...}}
        """
        if not rejected:
            print("Aucun mouvement en attente de rejeu.")
            return {"replayable": [], "still_pending": [], "counts": {}}
        # TODO : implémenter
        raise NotImplementedError("filter_now_known_skus non implémenté")

    @task
    def replay_movements(result: dict) -> dict:
        """
        TODO étudiant :
        1. Insérer result['replayable'] dans la table movements
        2. UPDATE rejected_movements SET status='REPLAYED' pour ces lignes
        3. Générer un rapport de rejeu
        4. Retourner les stats
        """
        replayable = result.get("replayable", [])
        if not replayable:
            print("Aucun mouvement à rejouer.")
            return {"replayed": 0, "still_pending": len(result.get("still_pending", []))}
        # TODO : implémenter
        raise NotImplementedError("replay_movements non implémenté")

    pending = fetch_pending_rejected()
    filtered = filter_now_known_skus(pending)
    replay_movements(filtered)


replay_rejected_movements()
