"""
Benchmark comparatif SQL (PostgreSQL) vs Parquet (DuckDB).

Usage :
    python scripts/benchmark_queries.py --palier small
    python scripts/benchmark_queries.py --palier small --ci   # mode CI : sorties réduites
"""
import argparse
import os
import time
from pathlib import Path

import duckdb
import pandas as pd


QUERIES_PARQUET = {
    "stock_by_sku": """
        SELECT sku, SUM(quantity) AS current_stock
        FROM read_parquet('{movements_file}')
        GROUP BY sku
        ORDER BY current_stock DESC
    """,
    "movements_by_month": """
        SELECT strftime(occurred_at, '%Y-%m') AS month,
               movement_type, COUNT(*) AS nb, SUM(quantity) AS total_qty
        FROM read_parquet('{movements_file}')
        GROUP BY 1, 2
        ORDER BY 1
    """,
}


def run_parquet_benchmark(movements_file: str, ci_mode: bool = False):
    """Exécute les requêtes DuckDB sur un fichier Parquet et mesure le temps."""
    if not Path(movements_file).exists():
        print(f"⚠️  Fichier Parquet introuvable : {movements_file}")
        print("   Générez d'abord les données avec generate_flows.py puis exportez en Parquet.")
        return {}

    conn = duckdb.connect()
    results = {}
    for name, query in QUERIES_PARQUET.items():
        q = query.format(movements_file=movements_file)
        start = time.perf_counter()
        df = conn.execute(q).df()
        elapsed = (time.perf_counter() - start) * 1000
        results[name] = {"engine": "DuckDB/Parquet", "rows": len(df), "ms": round(elapsed, 2)}
        if not ci_mode:
            print(f"  [{name}] DuckDB/Parquet : {elapsed:.2f} ms ({len(df)} lignes)")
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--palier", choices=["small", "medium", "large"], default="small")
    parser.add_argument("--ci", action="store_true", help="Mode CI : skip si fichiers absents")
    args = parser.parse_args()

    movements_parquet = f"data/curated/movements_{args.palier}.parquet"

    print(f"\n📊 Benchmark — palier '{args.palier}'")
    print("-" * 50)

    parquet_results = run_parquet_benchmark(movements_parquet, ci_mode=args.ci)

    # TODO étudiant : ajouter les benchmarks PostgreSQL ici
    # et comparer les résultats dans un tableau

    if parquet_results:
        print("\n✅ Benchmark Parquet/DuckDB terminé.")
        print("TODO : compléter avec les benchmarks PostgreSQL pour comparaison.")
    elif args.ci:
        print("ℹ️  Mode CI : fichiers Parquet absents, benchmark ignoré.")


if __name__ == "__main__":
    main()
