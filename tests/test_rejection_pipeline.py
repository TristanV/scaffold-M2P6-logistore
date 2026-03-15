"""
Tests du pipeline de rejet.

Ces tests vérifient que :
- Les mouvements avec SKU inconnu sont correctement rejetés.
- Les mouvements avec SKU connu sont acceptés.
- Les rapports de rejet contiennent les bonnes informations.

NOTE : ces tests utilisent une base SQLite en mémoire pour
être rapides et ne pas dépendre de PostgreSQL en CI.
"""
import pytest
from datetime import datetime


# Données de test
KNOWN_SKUS = {"SKU-00001", "SKU-00002", "SKU-00003"}

SAMPLE_MOVEMENTS = [
    {"schema_version": "1.0", "movement_id": "id-001", "sku": "SKU-00001",
     "movement_type": "IN", "quantity": 50, "reason": "Test",
     "occurred_at": datetime(2024, 2, 1)},
    {"schema_version": "1.0", "movement_id": "id-002", "sku": "SKU-00002",
     "movement_type": "IN", "quantity": 20, "reason": "Test",
     "occurred_at": datetime(2024, 2, 1)},
    {"schema_version": "1.0", "movement_id": "id-003", "sku": "SKU-99999",  # Orphelin
     "movement_type": "IN", "quantity": 10, "reason": "Orphelin",
     "occurred_at": datetime(2024, 2, 1)},
    {"schema_version": "1.0", "movement_id": "id-004", "sku": "SKU-88888",  # Orphelin
     "movement_type": "OUT", "quantity": -5, "reason": "Orphelin",
     "occurred_at": datetime(2024, 2, 1)},
]


def route_movements(movements: list[dict], known_skus: set) -> tuple[list, list]:
    """
    Fonction de routage à implémenter par les étudiants.
    TODO étudiant : remplacer cette implémentation vide par la vraie logique.
    Retourne (accepted, rejected).
    """
    # TODO étudiant : implémenter cette fonction dans scripts/ puis l'importer ici
    accepted = [m for m in movements if m["sku"] in known_skus]
    rejected = [
        {**m, "rejection_reason": "unknown_sku"}
        for m in movements if m["sku"] not in known_skus
    ]
    return accepted, rejected


class TestRejectionRouting:

    def test_known_skus_are_accepted(self):
        accepted, rejected = route_movements(SAMPLE_MOVEMENTS, KNOWN_SKUS)
        accepted_ids = {m["movement_id"] for m in accepted}
        assert "id-001" in accepted_ids
        assert "id-002" in accepted_ids

    def test_unknown_skus_are_rejected(self):
        accepted, rejected = route_movements(SAMPLE_MOVEMENTS, KNOWN_SKUS)
        rejected_ids = {m["movement_id"] for m in rejected}
        assert "id-003" in rejected_ids
        assert "id-004" in rejected_ids

    def test_rejection_reason_is_set(self):
        accepted, rejected = route_movements(SAMPLE_MOVEMENTS, KNOWN_SKUS)
        for r in rejected:
            assert r["rejection_reason"] == "unknown_sku"

    def test_counts_are_correct(self):
        accepted, rejected = route_movements(SAMPLE_MOVEMENTS, KNOWN_SKUS)
        assert len(accepted) == 2
        assert len(rejected) == 2
        assert len(accepted) + len(rejected) == len(SAMPLE_MOVEMENTS)

    def test_empty_input(self):
        accepted, rejected = route_movements([], KNOWN_SKUS)
        assert accepted == []
        assert rejected == []

    def test_all_known(self):
        all_known = [
            {"schema_version": "1.0", "movement_id": "id-x", "sku": "SKU-00001",
             "movement_type": "IN", "quantity": 1, "reason": "Test",
             "occurred_at": datetime(2024, 2, 1)}
        ]
        accepted, rejected = route_movements(all_known, KNOWN_SKUS)
        assert len(accepted) == 1
        assert len(rejected) == 0

    def test_all_unknown(self):
        all_unknown = [
            {"schema_version": "1.0", "movement_id": "id-y", "sku": "SKU-99001",
             "movement_type": "IN", "quantity": 1, "reason": "Test",
             "occurred_at": datetime(2024, 2, 1)}
        ]
        accepted, rejected = route_movements(all_unknown, KNOWN_SKUS)
        assert len(accepted) == 0
        assert len(rejected) == 1
