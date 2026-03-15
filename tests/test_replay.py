"""
Tests du mécanisme de rejeu.

Ces tests vérifient le scénario complet :
1. Des mouvements sont rejetés car le SKU est inconnu.
2. Le SKU est ensuite publié dans le catalogue.
3. Le rejeu réintègre correctement les mouvements.

TODO étudiant : compléter le test d'intégration test_full_replay_scenario
en implémentant la logique de rejeu dans les DAGs.
"""
import pytest
from datetime import datetime


def simulate_replay(rejected: list[dict], now_known_skus: set) -> tuple[list, list]:
    """
    Simule le rejeu : pour chaque rejeté, vérifie si son SKU est maintenant connu.
    Retourne (replayed, still_pending).
    TODO étudiant : cette fonction sera remplacée par l'appel au DAG 4 réel.
    """
    replayed = [m for m in rejected if m["sku"] in now_known_skus]
    still_pending = [m for m in rejected if m["sku"] not in now_known_skus]
    return replayed, still_pending


REJECTED_MOVEMENTS = [
    {"movement_id": "rej-001", "sku": "SKU-99001", "rejection_reason": "unknown_sku",
     "status": "PENDING", "quantity": 10, "movement_type": "IN",
     "occurred_at": datetime(2024, 3, 1)},
    {"movement_id": "rej-002", "sku": "SKU-99002", "rejection_reason": "unknown_sku",
     "status": "PENDING", "quantity": 5, "movement_type": "IN",
     "occurred_at": datetime(2024, 3, 2)},
    {"movement_id": "rej-003", "sku": "SKU-99003", "rejection_reason": "unknown_sku",
     "status": "PENDING", "quantity": -3, "movement_type": "OUT",
     "occurred_at": datetime(2024, 3, 3)},
]


class TestReplayMechanism:

    def test_replay_with_newly_known_sku(self):
        """SKU-99001 est maintenant connu : son mouvement doit être rejoué."""
        now_known = {"SKU-99001"}
        replayed, still_pending = simulate_replay(REJECTED_MOVEMENTS, now_known)
        replayed_ids = {m["movement_id"] for m in replayed}
        assert "rej-001" in replayed_ids
        assert len(replayed) == 1
        assert len(still_pending) == 2

    def test_replay_all_now_known(self):
        """Tous les SKUs sont maintenant connus : tout doit être rejoué."""
        all_known = {"SKU-99001", "SKU-99002", "SKU-99003"}
        replayed, still_pending = simulate_replay(REJECTED_MOVEMENTS, all_known)
        assert len(replayed) == 3
        assert len(still_pending) == 0

    def test_replay_none_known(self):
        """Aucun SKU n'est encore connu : rien n'est rejoué."""
        replayed, still_pending = simulate_replay(REJECTED_MOVEMENTS, set())
        assert len(replayed) == 0
        assert len(still_pending) == 3

    def test_replay_preserves_movement_data(self):
        """Les données du mouvement rejoué doivent être intactes."""
        now_known = {"SKU-99001"}
        replayed, _ = simulate_replay(REJECTED_MOVEMENTS, now_known)
        r = replayed[0]
        assert r["movement_id"] == "rej-001"
        assert r["quantity"] == 10
        assert r["movement_type"] == "IN"

    def test_full_replay_scenario(self):
        """
        Scénario complet :
        - T0 : 3 mouvements rejetés (SKU inconnus)
        - T1 : 2 SKUs sont publiés dans le catalogue
        - T2 : le rejeu intègre les 2 mouvements replayables, 1 reste en attente
        """
        # T0 — Rejets initiaux
        rejected = REJECTED_MOVEMENTS.copy()
        assert all(m["status"] == "PENDING" for m in rejected)

        # T1 — Nouveaux produits publiés
        newly_published = {"SKU-99001", "SKU-99002"}

        # T2 — Rejeu
        replayed, still_pending = simulate_replay(rejected, newly_published)

        assert len(replayed) == 2
        assert len(still_pending) == 1
        assert still_pending[0]["sku"] == "SKU-99003"
