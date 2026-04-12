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
"""
from __future__ import annotations

import os
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values

from airflow.decorators import dag, task

DSN = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "logistore"),
    "user": os.getenv("POSTGRES_USER", "logistore"),
    "password": os.getenv("POSTGRES_PASSWORD", "logistore"),
}


def _get_conn():
    return psycopg2.connect(**DSN)


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
        Récupère tous les mouvements rejetés en attente de rejeu.
        """
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, movement_id, sku, movement_type, quantity, "
                    "reason, occurred_at, rejection_reason, rejected_at, status "
                    "FROM rejected_movements WHERE status = 'PENDING'"
                )
                cols = [desc[0] for desc in cur.description]
                rows = cur.fetchall()

        results = []
        for row in rows:
            record = dict(zip(cols, row))
            # Convertir les datetime en chaîne pour la sérialisation XCom
            for key in ("occurred_at", "rejected_at"):
                if record.get(key) is not None:
                    record[key] = record[key].isoformat()
            results.append(record)

        print(f"{len(results)} mouvement(s) PENDING récupéré(s).")
        return results

    @task
    def filter_now_known_skus(rejected: list[dict]) -> dict:
        """
        Vérifie quels SKUs rejetés sont désormais présents dans products.
        Sépare les mouvements rejouables de ceux encore en attente.
        """
        if not rejected:
            print("Aucun mouvement en attente de rejeu.")
            return {"replayable": [], "still_pending": [], "counts": {}}

        # Extraire les SKUs uniques
        unique_skus = list({m["sku"] for m in rejected})

        # Vérifier lesquels existent maintenant dans products
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT sku FROM products WHERE sku = ANY(%s)",
                    (unique_skus,),
                )
                known_skus = {row[0] for row in cur.fetchall()}

        replayable = [m for m in rejected if m["sku"] in known_skus]
        still_pending = [m for m in rejected if m["sku"] not in known_skus]

        counts = {
            "total_pending": len(rejected),
            "replayable": len(replayable),
            "still_pending": len(still_pending),
            "known_skus": list(known_skus),
        }
        print(f"Rejeu possible : {counts['replayable']}/{counts['total_pending']} "
              f"({len(known_skus)} SKU(s) désormais connu(s)).")

        return {"replayable": replayable, "still_pending": still_pending, "counts": counts}

    @task
    def replay_movements(result: dict) -> dict:
        """
        Insère les mouvements rejouables dans movements et met à jour
        leur statut dans rejected_movements à 'REPLAYED'.
        """
        replayable = result.get("replayable", [])
        if not replayable:
            print("Aucun mouvement à rejouer.")
            return {"replayed": 0, "still_pending": len(result.get("still_pending", []))}

        with _get_conn() as conn:
            with conn.cursor() as cur:
                # 1. Insérer les mouvements rejouables dans la table movements
                insert_sql = (
                    "INSERT INTO movements "
                    "(movement_id, sku, movement_type, quantity, reason, occurred_at) "
                    "VALUES %s "
                    "ON CONFLICT (movement_id) DO NOTHING"
                )
                values = [
                    (
                        m["movement_id"],
                        m["sku"],
                        m["movement_type"],
                        m["quantity"],
                        m.get("reason"),
                        m["occurred_at"],
                    )
                    for m in replayable
                ]
                execute_values(cur, insert_sql, values)

                # 2. Mettre à jour le statut dans rejected_movements
                replayed_ids = [m["id"] for m in replayable]
                cur.execute(
                    "UPDATE rejected_movements SET status = 'REPLAYED' "
                    "WHERE id = ANY(%s)",
                    (replayed_ids,),
                )

            conn.commit()

        stats = {
            "replayed": len(replayable),
            "still_pending": len(result.get("still_pending", [])),
        }
        print(f"Rejeu terminé : {stats['replayed']} mouvement(s) rejoué(s), "
              f"{stats['still_pending']} encore en attente.")
        return stats

    pending = fetch_pending_rejected()
    filtered = filter_now_known_skus(pending)
    replay_movements(filtered)


replay_rejected_movements()
