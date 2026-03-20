"""
Tests des contrats d'interface Pydantic.
Ces tests vérifient que les modèles de validation fonctionnent
correctement pour les cas nominaux et les cas d'erreur.
"""
import pytest
import os
from datetime import datetime
from pydantic import ValidationError

from contracts.catalogue_contract import (
    CatalogueRecordV1, CatalogueRecordV2, get_catalogue_contract
)
from contracts.movement_contract import MovementRecordV1, MovementTypeEnum
from scripts.load_to_postgres import validate_flow


DATA_DIR = os.path.join("tests","data")

# ─── Tests CatalogueRecordV1 ───────────────────────────────────────────────

class TestCatalogueV1:

    def test_valid_record(self):
        record = CatalogueRecordV1(
            schema_version="1.0",
            sku="SKU-00042",
            label="Clé à molette",
            category="TOOLS",
            unit="PCS",
            min_stock=5,
            published_at=datetime(2024, 1, 15)
        )
        assert record.sku == "SKU-00042"
        assert record.category.value == "TOOLS"

    def test_invalid_sku_format(self):
        with pytest.raises(ValidationError) as exc_info:
            CatalogueRecordV1(
                schema_version="1.0",
                sku="BADFORMAT",  # Pas de format SKU-XXXXX
                label="Produit test",
                category="TOOLS",
                unit="PCS",
                min_stock=0,
                published_at=datetime(2024, 1, 1)
            )
        assert "sku" in str(exc_info.value)

    def test_invalid_category(self):
        with pytest.raises(ValidationError):
            CatalogueRecordV1(
                schema_version="1.0",
                sku="SKU-00001",
                label="Produit test",
                category="UNKNOWN_CATEGORY",  # Valeur hors enum
                unit="PCS",
                min_stock=0,
                published_at=datetime(2024, 1, 1)
            )

    def test_negative_min_stock_rejected(self):
        with pytest.raises(ValidationError):
            CatalogueRecordV1(
                schema_version="1.0",
                sku="SKU-00001",
                label="Produit test",
                category="TOOLS",
                unit="PCS",
                min_stock=-1,  # Doit être >= 0
                published_at=datetime(2024, 1, 1)
            )


# ─── Tests CatalogueRecordV2 ───────────────────────────────────────────────

class TestCatalogueV2:

    def test_v2_with_supplier_id(self):
        record = CatalogueRecordV2(
            schema_version="2.0",
            sku="SKU-00001",
            label="Produit V2",
            category="ELEC",
            unit="PCS",
            min_stock=2,
            published_at=datetime(2024, 6, 1),
            supplier_id="SUPP-001"
        )
        assert record.supplier_id == "SUPP-001"

    def test_v2_without_supplier_id_is_valid(self):
        """V2 reste valide sans supplier_id (champ optionnel)."""
        record = CatalogueRecordV2(
            schema_version="2.0",
            sku="SKU-00001",
            label="Produit V2",
            category="ELEC",
            unit="PCS",
            min_stock=2,
            published_at=datetime(2024, 6, 1)
        )
        assert record.supplier_id is None


# ─── Tests registre des versions ──────────────────────────────────────────

class TestContractVersionRegistry:

    def test_get_v1(self):
        contract_class = get_catalogue_contract("1.0")
        assert contract_class is CatalogueRecordV1

    def test_get_v2(self):
        contract_class = get_catalogue_contract("2.0")
        assert contract_class is CatalogueRecordV2

    def test_unknown_version_raises(self):
        with pytest.raises(ValueError, match="inconnue"):
            get_catalogue_contract("99.0")


# ─── Tests MovementRecordV1 ────────────────────────────────────────────────

class TestMovementV1:

    def test_valid_in_movement(self):
        record = MovementRecordV1(
            schema_version="1.0",
            sku="SKU-00001",
            movement_type="IN",
            quantity=50,
            reason="Réapprovisionnement",
            occurred_at=datetime(2024, 2, 1)
        )
        assert record.quantity == 50
        assert record.movement_type == MovementTypeEnum.IN

    def test_valid_out_movement_with_negative_quantity(self):
        record = MovementRecordV1(
            schema_version="1.0",
            sku="SKU-00001",
            movement_type="OUT",
            quantity=-10,
            reason="Livraison client",
            occurred_at=datetime(2024, 2, 1)
        )
        assert record.quantity == -10

    def test_out_with_positive_quantity_rejected(self):
        """Règle métier : OUT doit avoir quantity < 0."""
        with pytest.raises(ValidationError, match="OUT doit avoir quantity < 0"):
            MovementRecordV1(
                schema_version="1.0",
                sku="SKU-00001",
                movement_type="OUT",
                quantity=10,  # Devrait être négatif
                reason="Erreur de saisie",
                occurred_at=datetime(2024, 2, 1)
            )

    def test_zero_quantity_rejected(self):
        with pytest.raises(ValidationError, match="zéro"):
            MovementRecordV1(
                schema_version="1.0",
                sku="SKU-00001",
                movement_type="IN",
                quantity=0,
                reason="Mouvement vide",
                occurred_at=datetime(2024, 2, 1)
            )

    def test_invalid_sku_format(self):
        with pytest.raises(ValidationError):
            MovementRecordV1(
                schema_version="1.0",
                sku="MAUVAIS_FORMAT",
                movement_type="IN",
                quantity=10,
                reason="Test",
                occurred_at=datetime(2024, 2, 1)
            )

# ─── Tests Flows ────────────────────────────────────────────────

class TestValidateFlow:

    def test_validate_catalogue_v1_flow(self):
        valid_df, rejected_df = validate_flow(
            os.path.join(DATA_DIR, "catalogue_test.csv"),
            CatalogueRecordV2
        )
        # Tous les enregistrements V1 du fichier de test sont valides
        assert len(valid_df) == 5
        assert rejected_df.empty
        assert set(valid_df["sku"]) == {
            "SKU-00001",
            "SKU-00002",
            "SKU-00003",
            "SKU-00004",
            "SKU-00005",
        }
        assert set(valid_df["schema_version"]) == {"1.0"}

    def test_validate_catalogue_v2_flow(self):
        valid_df, rejected_df = validate_flow(
            os.path.join(DATA_DIR , "catalogue_test_v2.csv"),
            CatalogueRecordV2
        )
        # Deux lignes valides, une ligne rejetée (SKU mal formaté + min_stock négatif)
        assert len(valid_df) == 2
        assert all(valid_df["sku"].isin({"SKU-10001", "SKU-10002"}))
        assert len(rejected_df) == 1
        rejected_row = rejected_df.iloc[0]
        assert rejected_row["sku"] == "SKU-1000X"
        assert rejected_row["min_stock"] == -3
        assert "sku" in rejected_row["rejection_reason"]
        assert "min_stock" in rejected_row["rejection_reason"]

    def test_validate_movement_v1_flow(self):
        valid_df, rejected_df = validate_flow(
            os.path.join(DATA_DIR, "movements_test.csv"),
            MovementRecordV1
        )
        # Cinq mouvements respectent le contrat, un mouvement OUT avec quantity positive est rejeté
        assert len(valid_df) == 5
        assert len(rejected_df) == 1
        rejected_row = rejected_df.iloc[0]
        assert rejected_row["movement_id"] == "550e8400-e29b-41d4-a716-446655440006"
        assert rejected_row["movement_type"] == "OUT"
        assert rejected_row["quantity"] == 5
        assert "quantity < 0" in rejected_row["rejection_reason"]