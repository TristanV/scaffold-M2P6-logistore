"""
Génération des flux de données synthétiques avec Faker.

Usage :
    python scripts/generate_flows.py --palier small
    python scripts/generate_flows.py --palier medium
    python scripts/generate_flows.py --palier large
    python scripts/generate_flows.py --palier small --orphan-ratio 0.20

Paliers :
    small  : 200 produits, 10 000 mouvements (5% orphelins)
    medium : 500 produits, 100 000 mouvements (8% orphelins)
    large  : 1 000 produits, 1 000 000 mouvements (10% orphelins)
"""
import argparse
import os
import random
import uuid
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker

fake = Faker("fr_FR")

CATEGORIES = ["FOOD", "ELEC", "TOOLS", "CLOTHING", "OTHER"]
UNITS = ["PCS", "KG", "L", "BOX"]
MOVEMENT_TYPES = ["IN", "OUT", "ADJUST"]

PALIERS = {
    "small":  {"n_products": 200,   "n_movements": 10_000,   "orphan_ratio": 0.05},
    "medium": {"n_products": 500,   "n_movements": 100_000,  "orphan_ratio": 0.08},
    "large":  {"n_products": 1_000, "n_movements": 1_000_000, "orphan_ratio": 0.10},
}


def generate_catalogue(n_products: int, seed: int = 42) -> pd.DataFrame:
    """Génère n_products références produits."""
    Faker.seed(seed)
    random.seed(seed)
    base_date = datetime(2024, 1, 1)
    products = []
    for i in range(n_products):
        products.append({
            "schema_version": "1.0",
            "sku": f"SKU-{i:05d}",
            "label": fake.catch_phrase()[:200],
            "category": random.choice(CATEGORIES),
            "unit": random.choice(UNITS),
            "min_stock": random.randint(0, 50),
            "published_at": (
                base_date + timedelta(days=random.randint(0, 30))
            ).isoformat(),
        })
    return pd.DataFrame(products)


def generate_movements(
    n_movements: int,
    n_known_skus: int,
    orphan_ratio: float = 0.05,
    seed: int = 42,
) -> pd.DataFrame:
    """
    Génère n_movements mouvements de stock.
    orphan_ratio : proportion de mouvements avec un SKU inconnu (à rejeter).
    """
    Faker.seed(seed)
    random.seed(seed)
    movements = []
    for _ in range(n_movements):
        is_orphan = random.random() < orphan_ratio
        if is_orphan:
            sku = f"SKU-{random.randint(90000, 99999):05d}"  # Hors catalogue
        else:
            sku = f"SKU-{random.randint(0, n_known_skus - 1):05d}"

        movement_type = random.choice(MOVEMENT_TYPES)
        quantity = random.randint(1, 100)
        if movement_type == "OUT":
            quantity = -quantity

        movements.append({
            "schema_version": "1.0",
            "movement_id": str(uuid.uuid4()),
            "sku": sku,
            "movement_type": movement_type,
            "quantity": quantity,
            "reason": fake.sentence(nb_words=4)[:500],
            "occurred_at": fake.date_time_between(
                start_date="-6m", end_date="now"
            ).isoformat(),
        })
    return pd.DataFrame(movements)


def main():
    parser = argparse.ArgumentParser(description="Génération de flux LogiStore")
    parser.add_argument(
        "--palier",
        choices=["small", "medium", "large"],
        default="small",
        help="Palier de volume à générer",
    )
    parser.add_argument(
        "--orphan-ratio",
        type=float,
        default=None,
        help="Forcer un ratio de SKUs orphelins (ex: 0.20 pour 20%%)",
    )
    parser.add_argument("--seed", type=int, default=42, help="Graine aléatoire")
    args = parser.parse_args()

    cfg = PALIERS[args.palier].copy()
    if args.orphan_ratio is not None:
        cfg["orphan_ratio"] = args.orphan_ratio

    os.makedirs("data/inbox/catalogue", exist_ok=True)
    os.makedirs("data/inbox/movements", exist_ok=True)

    print(f"\n🏗  Génération palier '{args.palier}'...")
    catalogue_df = generate_catalogue(cfg["n_products"], seed=args.seed)
    out_cat = f"data/inbox/catalogue/catalogue_{args.palier}.csv"
    catalogue_df.to_csv(out_cat, index=False)
    print(f"✅ Catalogue : {len(catalogue_df)} produits → {out_cat}")

    movements_df = generate_movements(
        cfg["n_movements"], cfg["n_products"],
        orphan_ratio=cfg["orphan_ratio"],
        seed=args.seed,
    )
    out_mov = f"data/inbox/movements/movements_{args.palier}.csv"
    movements_df.to_csv(out_mov, index=False)
    n_orphans = (movements_df["sku"].str.startswith("SKU-9")).sum()
    print(f"✅ Mouvements : {len(movements_df)} lignes → {out_mov}")
    print(f"   dont ~{n_orphans} avec SKU orphelin ({n_orphans/len(movements_df)*100:.1f}%)")


if __name__ == "__main__":
    main()
