"""
Initialisation et chargement de la base PostgreSQL LogiStore.

Usage :
    python scripts/load_to_postgres.py --init         # Créer les tables
    python scripts/load_to_postgres.py --load-catalogue data/inbox/catalogue/catalogue_small.csv
    python scripts/load_to_postgres.py --load-movements data/inbox/movements/movements_small.csv
"""
import argparse
import os
import sys

import psycopg2
import pandas as pd
from pydantic import ValidationError
from psycopg2.extras import execute_values

# Ajouter la racine du projet au path pour importer contracts
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from contracts.catalogue_contract import get_catalogue_contract
from contracts.movement_contract import MovementRecordV1

DSN = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "logistore"),
    "user": os.getenv("POSTGRES_USER", "logistore"),
    "password": os.getenv("POSTGRES_PASSWORD", "logistore"),
}

CREATE_PRODUCTS = """
CREATE TABLE IF NOT EXISTS products (
    sku         VARCHAR(20)  PRIMARY KEY,
    label       VARCHAR(200) NOT NULL,
    category    VARCHAR(20)  NOT NULL,
    unit        VARCHAR(10)  NOT NULL,
    min_stock   INTEGER      NOT NULL DEFAULT 0,
    supplier_id VARCHAR(50),
    published_at TIMESTAMP   NOT NULL,
    inserted_at  TIMESTAMP   DEFAULT NOW()
);
"""

CREATE_MOVEMENTS = """
CREATE TABLE IF NOT EXISTS movements (
    movement_id   VARCHAR(36)  PRIMARY KEY,
    sku           VARCHAR(20)  NOT NULL REFERENCES products(sku),
    movement_type VARCHAR(10)  NOT NULL,
    quantity      INTEGER      NOT NULL,
    reason        TEXT,
    occurred_at   TIMESTAMP    NOT NULL,
    inserted_at   TIMESTAMP    DEFAULT NOW()
);
"""

CREATE_REJECTED_MOVEMENTS = """
CREATE TABLE IF NOT EXISTS rejected_movements (
    id              SERIAL       PRIMARY KEY,
    movement_id     VARCHAR(36),
    sku             VARCHAR(20)  NOT NULL,
    movement_type   VARCHAR(10),
    quantity        INTEGER,
    reason          TEXT,
    occurred_at     TIMESTAMP,
    rejection_reason TEXT        NOT NULL,
    rejected_at     TIMESTAMP    DEFAULT NOW(),
    status          VARCHAR(20)  DEFAULT 'PENDING'  -- PENDING | REPLAYED | ABANDONED
);
"""


def get_conn():
    return psycopg2.connect(**DSN)


def init_db():
    """Crée les tables si elles n'existent pas."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_PRODUCTS)
            cur.execute(CREATE_MOVEMENTS)
            cur.execute(CREATE_REJECTED_MOVEMENTS)
        conn.commit()
    print("✅ Tables créées (ou déjà existantes) : products, movements, rejected_movements")


def validate_flow(csv_path: str, flow_type: str) -> tuple[list[dict], list[dict]]:
    """Valide un CSV ligne par ligne avec le contrat Pydantic approprié.

    Args:
        csv_path: chemin vers le fichier CSV.
        flow_type: 'catalogue' ou 'movement'.

    Returns:
        (valid_records, rejected_records) — chaque rejeté contient un champ 'rejection_reason'.
    """
    df = pd.read_csv(csv_path)
    valid, rejected = [], []

    for idx, row in df.iterrows():
        row_dict = row.to_dict()
        # pandas lit schema_version comme float (1.0) ; Pydantic attend str "1.0"
        row_dict["schema_version"] = str(row_dict.get("schema_version", "1.0"))
        try:
            if flow_type == "catalogue":
                version = row_dict["schema_version"]
                model_class = get_catalogue_contract(version)
                record = model_class(**row_dict)
            elif flow_type == "movement":
                record = MovementRecordV1(**row_dict)
            else:
                raise ValueError(f"flow_type inconnu : {flow_type}")
            valid.append(record.model_dump())
        except (ValidationError, ValueError) as e:
            row_dict["rejection_reason"] = str(e)
            rejected.append(row_dict)

    print(f"Validation '{flow_type}' : {len(valid)} valides, {len(rejected)} rejetés")
    return valid, rejected


def upsert_catalogue(valid_records: list[dict]):
    """UPSERT des enregistrements catalogue valides dans la table products."""
    if not valid_records:
        return
    with get_conn() as conn:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO products (sku, label, category, unit, min_stock, supplier_id, published_at)
                VALUES %s
                ON CONFLICT (sku) DO UPDATE SET
                    label = EXCLUDED.label,
                    category = EXCLUDED.category,
                    unit = EXCLUDED.unit,
                    min_stock = EXCLUDED.min_stock,
                    supplier_id = EXCLUDED.supplier_id,
                    published_at = EXCLUDED.published_at
            """
            values = [
                (
                    r["sku"], r["label"], r["category"], r["unit"],
                    r["min_stock"], r.get("supplier_id"), r["published_at"],
                )
                for r in valid_records
            ]
            execute_values(cur, sql, values)
        conn.commit()
    print(f"✅ UPSERT catalogue : {len(valid_records)} produits")


def load_catalogue(csv_path: str):
    """Charge un CSV catalogue avec validation Pydantic + UPSERT."""
    valid, rejected = validate_flow(csv_path, "catalogue")
    upsert_catalogue(valid)
    if rejected:
        os.makedirs("data/rejected", exist_ok=True)
        rej_df = pd.DataFrame(rejected)
        rej_path = "data/rejected/catalogue_rejected.csv"
        rej_df.to_csv(rej_path, index=False)
        print(f"⚠️  {len(rejected)} lignes rejetées → {rej_path}")


def load_movements(csv_path: str):
    """Charge un CSV mouvements avec validation + routage SKU connu/inconnu."""
    valid, rejected = validate_flow(csv_path, "movement")

    # Vérifier l'existence des SKUs dans products
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT sku FROM products")
            known_skus = {r[0] for r in cur.fetchall()}

    accepted = [m for m in valid if m["sku"] in known_skus]
    orphans = [m for m in valid if m["sku"] not in known_skus]

    # Insérer les mouvements acceptés
    if accepted:
        with get_conn() as conn:
            with conn.cursor() as cur:
                sql = """
                    INSERT INTO movements (movement_id, sku, movement_type, quantity, reason, occurred_at)
                    VALUES %s ON CONFLICT (movement_id) DO NOTHING
                """
                values = [
                    (m["movement_id"], m["sku"], m["movement_type"],
                     m["quantity"], m["reason"], m["occurred_at"])
                    for m in accepted
                ]
                execute_values(cur, sql, values)
            conn.commit()
        print(f"✅ {len(accepted)} mouvements insérés")

    # Insérer les orphelins dans rejected_movements
    if orphans:
        with get_conn() as conn:
            with conn.cursor() as cur:
                sql = """
                    INSERT INTO rejected_movements
                        (movement_id, sku, movement_type, quantity, reason, occurred_at, rejection_reason)
                    VALUES %s
                """
                values = [
                    (m["movement_id"], m["sku"], m["movement_type"],
                     m["quantity"], m["reason"], m["occurred_at"], "unknown_sku")
                    for m in orphans
                ]
                execute_values(cur, sql, values)
            conn.commit()
        print(f"⚠️  {len(orphans)} mouvements rejetés (SKU inconnu)")

    if rejected:
        print(f"⚠️  {len(rejected)} mouvements rejetés (validation Pydantic)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--init", action="store_true", help="Créer les tables")
    parser.add_argument("--load-catalogue", metavar="FILE", help="Charger un CSV catalogue")
    parser.add_argument("--load-movements", metavar="FILE", help="Charger un CSV mouvements")
    args = parser.parse_args()

    if args.init:
        init_db()

    if args.load_catalogue:
        load_catalogue(args.load_catalogue)

    if args.load_movements:
        load_movements(args.load_movements)


if __name__ == "__main__":
    main()
