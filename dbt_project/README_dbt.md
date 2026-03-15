# (Bonus) dbt pour les transformations analytiques

Ce dossier est prévu pour l'option bonus dbt.

## Installation

```bash
pip install dbt-duckdb
cd dbt_project
dbt init logistore_analytics
# Choisir 'duckdb' comme adaptateur
```

## Intérêt par rapport aux requêtes DuckDB inline dans les DAGs

- Séparation claire : dbt gère les transformations, Airflow orchestre
- Documentation automatique des modèles (`dbt docs generate`)
- Tests sur les résultats analytiques (unicité, non-nullité, cohérence)
- Lineage graph automatique (qui dépend de quoi)
- Intégration `dbt run && dbt test` dans DAG 3

## Démarrage

```bash
dbt run
dbt test
dbt docs generate && dbt docs serve
```
