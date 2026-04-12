# LogiStore — M2 Projet 6 | Pipeline Data Engineering
## Mission individuelle — DAG 1 Ingestion Catalogue

---

## 1. Prérequis — Lancer une seule fois

```bash
# 1. Créer et activer le virtualenv
python -m venv .venv
.venv\Scripts\Activate.ps1          # Windows PowerShell

# 2. Installer les dépendances
pip install -r requirements.txt

# 3. Lancer l'infrastructure Docker
docker-compose up -d

# 4. Initialiser les tables PostgreSQL
python scripts/load_to_postgres.py --init
```

**Vérifications :**
- Airflow UI accessible → http://localhost:8080 (`admin` / `admin`)
- PostgreSQL accessible → `localhost:5432` (db: `logistore`, user: `logistore`)

---

## 2. Génération des données de test

```bash
# Palier small  : 200 produits, 10 000 mouvements (5% orphelins)
python scripts/generate_flows.py --palier small

# Palier medium : 500 produits, 100 000 mouvements (8% orphelins)
python scripts/generate_flows.py --palier medium

# Palier large  : 1 000 produits, 1 000 000 mouvements (10% orphelins)
python scripts/generate_flows.py --palier large
```

**Sorties produites :**

| Fichier | Contenu |
|---|---|
| `data/inbox/catalogue/catalogue_small.csv` | 200 références produits |
| `data/inbox/catalogue/catalogue_medium.csv` | 500 références produits |
| `data/inbox/catalogue/catalogue_large.csv` | 1 000 références produits |
| `data/inbox/movements/movements_small.csv` | 10 000 mouvements (dont ~5% SKU orphelins) |
| `data/inbox/movements/movements_medium.csv` | 100 000 mouvements (dont ~8% SKU orphelins) |
| `data/inbox/movements/movements_large.csv` | 1 000 000 mouvements (dont ~10% SKU orphelins) |

---

## 3. Validation des contrats Pydantic (tests)

```bash
pytest tests/ -v
```

**Sortie attendue :** `26 passed` — couvre les contrats V1/V2, pipeline de rejet et replay.

---

## 4. Chargement manuel dans PostgreSQL (optionnel / debug)

```bash
# Charger le catalogue small
python scripts/load_to_postgres.py --load-catalogue data/inbox/catalogue/catalogue_small.csv

# Charger les mouvements small
python scripts/load_to_postgres.py --load-movements data/inbox/movements/movements_small.csv
```

**Sorties produites :**

| Destination | Contenu |
|---|---|
| Table PostgreSQL `products` | Produits valides (UPSERT) |
| Table PostgreSQL `movements` | Mouvements avec SKU connu |
| Table PostgreSQL `rejected_movements` | Mouvements avec SKU inconnu (status: PENDING) |
| `data/rejected/catalogue_rejected.csv` | Lignes catalogue invalides avec motif de rejet |

---

## 5. DAG 1 — Ingestion Catalogue (mission individuelle)

**Dans l'UI Airflow → http://localhost:8080 :**
1. Activer le DAG `ingest_catalogue` (toggle ON)
2. Cliquer sur ▶ **Trigger DAG** (ou attendre le déclenchement `@hourly`)

**Ou via CLI :**
```bash
docker-compose exec airflow-scheduler airflow dags unpause ingest_catalogue
docker-compose exec airflow-scheduler airflow dags trigger ingest_catalogue
```

**Ce que le DAG exécute (dans l'ordre) :**

```
detect_new_catalogue_file
        ↓
validate_and_upsert_catalogue
        ↓
export_catalogue_to_parquet  ──→ déclenche automatiquement DAG 2
```

**Sorties produites :**

| Fichier / Table | Contenu |
|---|---|
| Table PostgreSQL `products` | Catalogue complet (UPSERT à chaque run) |
| `data/curated/catalogue_snapshot.parquet` | Snapshot Parquet de toute la table `products` |
| `data/rejected/catalogue/catalogue_rejected.csv` | Lignes rejetées par validation Pydantic |

---

## 6. Livrables à transmettre aux collègues

| Livrable | Chemin | Utilisé par |
|---|---|---|
| Snapshot catalogue | `data/curated/catalogue_snapshot.parquet` | DAG 2 (mouvements) |
| Catalogue rejeté | `data/rejected/catalogue/catalogue_rejected.csv` | Équipe qualité |
| CSV catalogue small | `data/inbox/catalogue/catalogue_small.csv` | Tests DAG 2 |
| CSV mouvements small | `data/inbox/movements/movements_small.csv` | Tests DAG 2 |
| Résultats tests | `pytest tests/ -v` (26 passed) | Validation CI |

---

## 7. Architecture du pipeline complet

```
[Fournisseur A]                    [Fournisseur B]
catalogue_*.csv                    movements_*.csv
      │                                  │
      ▼                                  │
 DAG 1 (ma mission)                      │
 ├── Validation Pydantic V1/V2           │
 ├── UPSERT → products (PG)              │
 └── Export → catalogue_snapshot.parquet │
              │                          │
              │ Dataset trigger          ▼
              └──────────────► DAG 2 (collègues)
                                ├── Validation MovementV1
                                ├── Vérif SKU dans products
                                ├── accepted → movements (PG)
                                └── rejected → rejected_movements (PG)
                                              │
                                              ▼
                                         DAG 4 (replay)
                                   re-traite les PENDING
                                   quand SKU enfin publié
```

---

## Variables d'environnement (pas de credentials en dur)

| Variable | Valeur par défaut |
|---|---|
| `POSTGRES_HOST` | `postgres` (dans Docker) / `localhost` (local) |
| `POSTGRES_PORT` | `5432` |
| `POSTGRES_DB` | `logistore` |
| `POSTGRES_USER` | `logistore` |
| `POSTGRES_PASSWORD` | `logistore` |
