# SNCF Data Warehouse & Analytics Pipeline

Entrepôt de données (Data Warehouse) et pipeline ETL analysant l'activité
d'une compagnie ferroviaire fictive : circulation des trains et vente de
billets, construit de bout en bout avec des outils modernes de Data
Engineering.

## Objectif

- Obtenir un **DW opérationnel** interrogeable pour des indicateurs métier
  (chiffre d'affaires, taux de remplissage, ponctualité, canaux de vente...).
- Servir de **projet de portfolio** démontrant une chaîne complète :
  modélisation dimensionnelle, ETL, orchestration, contrôle qualité,
  visualisation.

## Origine des données

13 fichiers CSV générés de façon synthétique (`data/raw/`), représentant une
base opérationnelle (OLTP) :

| Fichier | Volumétrie | Rôle |
|---|---|---|
| `CLIENT_OLTP_SNCF_100K.csv` | 100 000 lignes | Référentiel clients |
| `GARE_OLTP_SNCF_198.csv` | 198 lignes | Référentiel gares |
| `TRAIN_OLTP_SNCF_2500.csv` | 2 500 lignes | Référentiel trains |
| `TRAJET_2021.csv` … `TRAJET_2025.csv` | ~6 000 000 lignes au total | Un train qui circule un jour donné |
| `RESERVATION_2021_2M.csv` … `RESERVATION_2025_2M.csv` | ~10 000 000 lignes au total | Un billet vendu |

## Architecture

```
CSV (data/raw/)
      │  \copy
      ▼
PostgreSQL "source" (schéma raw)      ← copie fidèle des CSV
      │  Python (extract / transform)
      ▼
PostgreSQL "DW" (schéma dw)           ← constellation de faits
      │
      ▼
Power BI / dashboard
```

Deux bases PostgreSQL, dans des conteneurs **Docker** distincts, orchestrées
avec **Docker Compose** — la base opérationnelle et l'entrepôt sont hébergés
séparément, comme dans un environnement réel.

## Modèle de données du DW

Une **constellation de faits** (deux tables de faits partageant les mêmes
dimensions) plutôt qu'une simple étoile, car deux processus métier distincts
coexistent : les trains qui circulent, et les billets qui se vendent.

```
                    dim_date
                   /    |    \
                  /     |     \
        fact_trajet     |      fact_reservation
        /    |    \     |       /    |    \
  dim_train  |  dim_gare        dim_client  (trajet_key → fact_trajet)
             |  (x2 : départ/arrivée)
```

| Table | Grain | Mesures / attributs clés |
|---|---|---|
| `dim_date` | un jour | année, trimestre, mois, jour de semaine, week-end |
| `dim_client` | un client | nom, ville, type de client, statut du compte |
| `dim_gare` | une gare | ville, région, nombre de quais, année de mise en service |
| `dim_train` | un train | type, capacités par classe, énergie |
| `fact_trajet` | un train circulant un jour donné | distance, statut de circulation |
| `fact_reservation` | un billet vendu | prix unitaire, nb passagers, montant total |

## Stack technique

| Composant | Rôle |
|---|---|
| PostgreSQL | Stockage (source + DW) |
| Docker / Docker Compose | Environnement reproductible |
| Python (pandas / SQLAlchemy) | ETL : extraction, transformation, chargement |
| Airflow *(à venir)* | Orchestration du pipeline |
| pytest *(à venir)* | Contrôles de qualité des données |
| Power BI *(à venir)* | Visualisation et tableaux de bord |

## Structure du dépôt

```
sncf-dw/
├── docker-compose.yml
├── .env                 # non versionné — voir .env.example
├── data/
│   └── raw/              # CSV sources (non versionnés si volumineux)
├── sql/
│   ├── source/           # DDL du schéma raw (base source)
│   └── dw/               # DDL du schéma dw (constellation de faits)
├── src/
│   ├── extract/
│   ├── transform/
│   └── load/
├── tests/
└── dags/                 # Airflow, à venir
```

## Démarrage

```bash
# 1. Configurer les identifiants
cp .env.example .env      # puis éditer les mots de passe

# 2. Démarrer les deux bases PostgreSQL
docker compose up -d
docker compose ps          # attendre le statut "healthy"

# 3. Charger les CSV dans le schéma raw (base source)
#    voir sql/source/ et les commandes \copy documentées ci-dessous
```

Chargement des tables simples :

```bash
docker exec sncf_source psql -U northwind_user -d sncf_oltp \
  -c "\copy raw.client FROM '/data/raw/CLIENT_OLTP_SNCF_100K.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');"
```

Puis, pour `trajet` et `reservation`, répéter sur chaque année (2021 à 2025).

## Qualité des données identifiée au profilage

Le profilage initial des CSV a révélé plusieurs incohérences à corriger dans
la couche de transformation (`raw` → `dw`), volontairement laissées telles
quelles dans `raw` pour garder une copie fidèle des sources :

- deux colonnes redondantes dans `GARE` (`annee_mise_en_service` /
  `annee_mise_service`) avec des valeurs divergentes ;
- valeurs de `sexe` incohérentes (`F` / `Femme`) ;
- casse de `ville` non uniforme (`Lille`, `STRASBOURG`, `nantes`) ;
- léger chevauchement de dates entre fichiers annuels de `RESERVATION`.

## Feuille de route

- [x] Environnement Docker (deux bases PostgreSQL)
- [x] Profilage des CSV
- [x] Conception des schémas `raw` et `dw`
- [x] Chargement des CSV dans `raw.*`
- [ ] Script Python de transformation `raw` → `dw`
- [ ] Contrôles de qualité (pytest)
- [ ] Orchestration Airflow
- [ ] Containerisation complète du pipeline
- [ ] Tableau de bord Power BI
- [ ] Comparaison avec une implémentation SQL Server équivalente

## Licence des données

Données synthétiques, générées à des fins pédagogiques. Aucune donnée
personnelle réelle n'est utilisée.
