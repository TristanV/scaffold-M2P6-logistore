"""
Contrat d'interface — Flux MOUVEMENT
Version disponible : V1

Ce fichier définit le schéma attendu pour chaque ligne
du flux de mouvements de stock publié par le fournisseur B.
"""
from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field, model_validator


class MovementTypeEnum(str, Enum):
    IN = "IN"
    OUT = "OUT"
    ADJUST = "ADJUST"


class MovementRecordV1(BaseModel):
    """
    Contrat d'interface — Flux MOUVEMENT — Version 1.0

    Règles de validation :
    - movement_id : UUID v4
    - sku : format strict SKU-XXXXX
    - movement_type : IN | OUT | ADJUST
    - quantity : != 0 ; négatif si OUT
    - reason : texte libre 1-500 caractères
    - occurred_at : datetime ISO 8601

    Règle métier :
    - Les mouvements de type OUT doivent avoir une quantity < 0.
    """

    schema_version: Literal["1.0"]
    movement_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    sku: str = Field(..., pattern=r"^SKU-\d{5}$")
    movement_type: MovementTypeEnum
    quantity: int = Field(..., description="Positif pour IN/ADJUST, négatif pour OUT")
    reason: str = Field(..., min_length=1, max_length=500)
    occurred_at: datetime

    @model_validator(mode="after")
    def check_quantity_not_zero(self) -> "MovementRecordV1":
        if self.quantity == 0:
            raise ValueError("La quantité ne peut pas être zéro.")
        return self

    @model_validator(mode="after")
    def check_out_quantity_negative(self) -> "MovementRecordV1":
        """Règle métier : mouvement OUT => quantity strictement négative."""
        if self.movement_type == MovementTypeEnum.OUT and self.quantity > 0:
            raise ValueError(
                f"Mouvement OUT doit avoir quantity < 0, reçu : {self.quantity}"
            )
        return self

    class Config:
        json_schema_extra = {
            "example": {
                "schema_version": "1.0",
                "movement_id": "550e8400-e29b-41d4-a716-446655440000",
                "sku": "SKU-00042",
                "movement_type": "IN",
                "quantity": 50,
                "reason": "Réapprovisionnement fournisseur",
                "occurred_at": "2024-06-01T10:30:00"
            }
        }
