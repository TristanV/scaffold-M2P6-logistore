# Changelog des contrats d'interface LogiStore

## Flux CATALOGUE

### Version 2.0 — 2024-06-01
- **Ajout (optionnel)** : champ `supplier_id` (str, nullable).
- Rétrocompatible : les enregistrements V1 sans `supplier_id` restent valides.
- Action requise côté fournisseur : aucune (champ optionnel).

### Version 1.0 — 2024-01-01
- Version initiale.
- Champs : `schema_version`, `sku`, `label`, `category`, `unit`, `min_stock`, `published_at`.

---

## Flux MOUVEMENT

### Version 1.0 — 2024-01-01
- Version initiale.
- Champs : `schema_version`, `movement_id`, `sku`, `movement_type`, `quantity`, `reason`, `occurred_at`.
- Règle métier : `OUT` → `quantity < 0`.
