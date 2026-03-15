# Projet 6 — Gestionnaire d'inventaire d'entrepôt LogiStore
## Pipeline d'ingestion de flux asynchrones avec contrats d'interface

> **RNCP 40573 — Expert en informatique et systèmes d'information (Niveau 7)**  
> Unité d'enseignement : *Data Engineering dans les systèmes d'information et Applications de l'IA à l'échelle*  
> Blocs mobilisés : BC01 · BC02 · BC03 · BC05 · BC06

---

## 🎯 Objectifs du projet

Vous êtes une petite équipe data mandatée par **LogiStore**, une PME gérant un entrepôt logistique multi-fournisseurs. Deux flux de données arrivent de façon **asynchrone et indépendante** :

- **Flux CATALOGUE** : un fournisseur publie périodiquement de nouvelles références produits.
- **Flux MOUVEMENT** : un autre fournisseur publie des mouvements de stock (entrées / sorties / ajustements).

Le SI actuel est un script monolithique sans validation ni gestion des erreurs. Votre mission :

1. **Définir des contrats d'interface versionnés** (Pydantic) pour chaque flux.
2. **Orchestrer un pipeline d'ingestion** en 4 DAGs Airflow coordonnés.
3. **Gérer les rejets et le rejeu** : un mouvement sur un produit inconnu doit être rejeté, tracé, et rejoué automatiquement dès que le produit est intégré.
4. **Comparer les performances SQL vs Parquet** (PostgreSQL vs DuckDB/Parquet) sur 3 paliers de volume.
5. **(Bonus)** Exploiter dbt pour les transformations analytiques.

### Compétences validées

| Bloc | Ce que vous allez faire |
|------|------------------------|
| BC01 | Comparer SQL vs Parquet : performance, scalabilité, éco-conception |
| BC02 | Note de cadrage, matrice des risques, plan de migration |
| BC03 | Diagrammes d'architecture, tests de non-régression |
| BC05 | DAGs Airflow, contrats Pydantic, gestion des rejets/rejeu, montée en volume |
| BC06 | Docker Compose, CI/CD GitHub Actions |

---

## 🗂 Structure du dépôt

```
scaffold-M2P6-logistore/
├── docker-compose.yml              # PostgreSQL + Airflow prêts à l'emploi
├── requirements.txt                # Dépendances Python
├── .gitignore                      # Exclut les gros CSV de data/
├── .github/
│   └── workflows/ci.yml            # Pipeline CI/CD GitHub Actions
├── airflow/
│   └── dags/
│       ├── dag1_ingest_catalogue.py   # TODO étudiant
│       ├── dag2_ingest_movements.py   # TODO étudiant
│       ├── dag3_inventory_analytics.py
│       └── dag4_replay_rejected.py
├── contracts/
│   ├── catalogue_contract.py       # CatalogueRecordV1 + V2
│   └── movement_contract.py        # MovementRecordV1
├── data/
│   ├── inbox/
│   │   ├── catalogue/              # Déposer vos CSV catalogue ici
│   │   └── movements/              # Déposer vos CSV mouvements ici
│   ├── raw/
│   ├── curated/
│   └── rejected/
│       └── reports/
├── scripts/
│   ├── generate_flows.py           # Génération Faker (3 paliers)
│   ├── load_to_postgres.py         # Chargement initial PostgreSQL
│   └── benchmark_queries.py        # Comparaison SQL vs Parquet
├── tests/
│   ├── data/
│   │   ├── catalogue_test.csv
│   │   └── movements_test.csv
│   ├── test_contracts.py
│   ├── test_rejection_pipeline.py
│   └── test_replay.py
├── docs/
│   └── SUJET.md                    # Énoncé complet du projet
└── dbt_project/                    # (Bonus) Structure dbt-duckdb
    └── README_dbt.md
```

---

## 🚀 Démarrage en 10 étapes

### Prérequis
- Docker Desktop installé et démarré
- Python 3.11+
- Git

### 1. Forker ce dépôt

Cliquez sur **Fork** en haut à droite de cette page GitHub, puis clonez votre fork :

```bash
git clone https://github.com/<votre-username>/scaffold-M2P6-logistore.git
cd scaffold-M2P6-logistore
```

### 2. Créer l'environnement virtuel

```bash
python -m venv .venv
source .venv/bin/activate       # Linux/Mac
# ou
.venv\Scripts\activate          # Windows
pip install -r requirements.txt
```

### 3. Démarrer l'infrastructure Docker

```bash
docker-compose up -d
```

Services démarrés :
- **Airflow UI** → http://localhost:8080 (admin / admin)
- **PostgreSQL** → localhost:5432 (logistore / logistore)

### 4. Initialiser la base de données

```bash
python scripts/load_to_postgres.py --init
```

### 5. Générer les données de test (palier Small)

```bash
python scripts/generate_flows.py --palier small
# → data/inbox/catalogue/catalogue_small.csv
# → data/inbox/movements/movements_small.csv
```

### 6. Vérifier les contrats

```bash
python -c "from contracts.catalogue_contract import CatalogueRecordV1; print('Contrats OK')"
```

### 7. Lancer les tests

```bash
pytest tests/ -v
```

### 8. Activer et déclencher les DAGs dans Airflow

- Ouvrir http://localhost:8080
- Activer les 4 DAGs
- Déclencher manuellement `ingest_catalogue`, observer la propagation vers `ingest_movements`

### 9. Tester le scénario de rejet

```bash
# Générer un flux avec 20% de SKUs inconnus
python scripts/generate_flows.py --palier small --orphan-ratio 0.20
# Re-déclencher DAG 2, observer les rejets
```

### 10. Lancer le benchmark SQL vs Parquet

```bash
python scripts/benchmark_queries.py --palier small
```

---

## 📋 Livrables attendus

| # | Livrable | Format |
|---|---------|--------|
| L1 | Dossier d'architecture + pilotage (note de cadrage, diagrammes, analyse SQL vs Parquet) | Markdown/PDF |
| L2 | Rapport de benchmark (3 paliers, analyse argumentée) | Markdown |
| L3 | Code source (4 DAGs, contrats, scripts, tests) | Dépôt Git (fork) |
| L4 | Tests et CI/CD opérationnels | pytest + GitHub Actions |
| L5 | REX + soutenance | Slides + démo live |

---

## ⚠️ Règles du projet

- **Ne jamais pousser de gros CSV** dans `data/inbox/`, `data/raw/` ou `data/curated/` — ils sont ignorés par `.gitignore`.
- Les tests CI s'appuient sur les petits fichiers dans `tests/data/` (quelques dizaines de lignes).
- Chaque fonctionnalité doit passer par une **Pull Request** vers votre branche `main` — la CI doit être verte avant le merge.
- Le dossier `contracts/` est votre référence partagée — versionnez les évolutions de contrat avec un `CHANGELOG.md`.

---

## 📚 Références

- [Apache Airflow — Dataset API](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/datasets.html)
- [Pydantic v2 — Documentation](https://docs.pydantic.dev/latest/)
- [DuckDB — Documentation](https://duckdb.org/docs/)
- [dbt-duckdb (bonus)](https://github.com/duckdb/dbt-duckdb)
- [Faker — Documentation](https://faker.readthedocs.io/)
