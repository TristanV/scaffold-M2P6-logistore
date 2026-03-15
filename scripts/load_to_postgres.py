"""
Initialisation et chargement de la base PostgreSQL LogiStore.

Usage :
    python scripts/load_to_postgres.py --init         # Créer les tables
    python scripts/load_to_postgres.py --load-catalogue data/inbox/catalogue/catalogue_small.csv
    python scripts/load_to_postgres.py --load-movements data/inbox/movements/movements_small.csv
"""
import argparse
import os

import psycopg2
import pandas as pd
from psycopg2.extras import execute_values

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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--init", action="store_true", help="Créer les tables")
    parser.add_argument("--load-catalogue", metavar="FILE", help="Charger un CSV catalogue")
    parser.add_argument("--load-movements", metavar="FILE", help="Charger un CSV mouvements")
    args = parser.parse_args()

    if args.init:
        init_db()

    if args.load_catalogue:
        # TODO étudiant : implémenter le chargement du catalogue avec validation Pydantic
        print(f"TODO : charger le catalogue depuis {args.load_catalogue}")

    if args.load_movements:
        # TODO étudiant : implémenter le chargement des mouvements avec gestion des rejets
        print(f"TODO : charger les mouvements depuis {args.load_movements}")


if __name__ == "__main__":
    main()
