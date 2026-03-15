"""
Contrat d'interface — Flux CATALOGUE
Versions disponibles : V1, V2

Ce fichier définit le schéma attendu pour chaque ligne
du flux de nouvelles références produits publié par le fournisseur A.
Il constitue le 'contrat' entre le producteur et le pipeline d'ingestion.
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator


class CategoryEnum(str, Enum):
    FOOD = "FOOD"
    ELEC = "ELEC"
    TOOLS = "TOOLS"
    CLOTHING = "CLOTHING"
    OTHER = "OTHER"


class UnitEnum(str, Enum):
    PCS = "PCS"
    KG = "KG"
    L = "L"
    BOX = "BOX"


class CatalogueRecordV1(BaseModel):
    """
    Contrat d'interface — Flux CATALOGUE — Version 1.0

    Règles de validation :
    - sku : format strict SKU-XXXXX (5 chiffres)
    - label : entre 3 et 200 caractères
    - category : valeur parmi FOOD, ELEC, TOOLS, CLOTHING, OTHER
    - unit : valeur parmi PCS, KG, L, BOX
    - min_stock : entier >= 0
    - published_at : datetime ISO 8601
    """

    schema_version: Literal["1.0"]
    sku: str = Field(..., pattern=r"^SKU-\d{5}$",
                     description="Identifiant unique produit, format SKU-XXXXX")
    label: str = Field(..., min_length=3, max_length=200)
    category: CategoryEnum
    unit: UnitEnum
    min_stock: int = Field(..., ge=0)
    published_at: datetime

    class Config:
        json_schema_extra = {
            "example": {
                "schema_version": "1.0",
                "sku": "SKU-00042",
                "label": "Clé à molette 12mm",
                "category": "TOOLS",
                "unit": "PCS",
                "min_stock": 5,
                "published_at": "2024-03-01T08:00:00"
            }
        }


class CatalogueRecordV2(BaseModel):
    """
    Contrat d'interface — Flux CATALOGUE — Version 2.0

    Évolution rétrocompatible : ajout du champ optionnel supplier_id.
    Les enregistrements V1 (sans supplier_id) restent valides.
    """

    schema_version: Literal["2.0"]
    sku: str = Field(..., pattern=r"^SKU-\d{5}$")
    label: str = Field(..., min_length=3, max_length=200)
    category: CategoryEnum
    unit: UnitEnum
    min_stock: int = Field(..., ge=0)
    published_at: datetime
    # Champ ajouté en V2 — optionnel pour rétrocompatibilité
    supplier_id: Optional[str] = Field(
        None,
        description="Identifiant fournisseur (nouveau en V2, optionnel)"
    )


# Registre des versions disponibles
CATALOGUE_CONTRACT_VERSIONS = {
    "1.0": CatalogueRecordV1,
    "2.0": CatalogueRecordV2,
}


def get_catalogue_contract(version: str):
    """Retourne la classe de contrat correspondant à la version."""
    if version not in CATALOGUE_CONTRACT_VERSIONS:
        raise ValueError(
            f"Version de contrat catalogue inconnue : '{version}'. "
            f"Versions disponibles : {list(CATALOGUE_CONTRACT_VERSIONS.keys())}"
        )
    return CATALOGUE_CONTRACT_VERSIONS[version]
