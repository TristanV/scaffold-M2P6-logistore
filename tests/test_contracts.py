"""
Tests des contrats d'interface Pydantic.
Ces tests vérifient que les modèles de validation fonctionnent
correctement pour les cas nominaux et les cas d'erreur.
"""
import pytest
from datetime import datetime
from pydantic import ValidationError

from contracts.catalogue_contract import (
    CatalogueRecordV1, CatalogueRecordV2, get_catalogue_contract
)
from contracts.movement_contract import MovementRecordV1, MovementTypeEnum


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
