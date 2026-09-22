-- =====================================================================
-- Landing zone : copie quasi brute des CSV, dans la base "source".
-- Chargée avec \copy (psql) depuis data/raw/*.csv.
-- Aucune transformation métier ici : on garde les colonnes redondantes
-- ou incohérentes telles quelles (ex: les deux colonnes "année de mise
-- en service" de GARE), le nettoyage se fait au chargement du DW.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.client (
    id_client                    INTEGER PRIMARY KEY,
    nom                          VARCHAR(50),
    prenom                       VARCHAR(50),
    date_naissance               DATE,
    sexe                         VARCHAR(10),
    type_client                  VARCHAR(30),
    ville                        VARCHAR(50),
    code_postal                  VARCHAR(10),
    pays                         VARCHAR(50),
    email                        VARCHAR(100),
    telephone                    VARCHAR(20),
    date_creation_compte         DATE,
    date_derniere_modification   DATE,
    statut_compte                VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS raw.gare (
    id_gare                 INTEGER PRIMARY KEY,
    nom_gare                VARCHAR(60),
    ville                   VARCHAR(50),
    region                  VARCHAR(50),
    pays                    VARCHAR(50),
    nb_quais                SMALLINT,
    type_gare               VARCHAR(30),
    taille_gare             VARCHAR(20),
    electrification         VARCHAR(20),
    annee_mise_en_service   SMALLINT,   -- colonne retenue
    latitude                NUMERIC(9,6),
    longitude               NUMERIC(9,6),
    annee_mise_service      SMALLINT,   -- doublon incohérent : conservé mais ignoré au DW
    categorie_strategique   VARCHAR(30)
);

CREATE TABLE IF NOT EXISTS raw.train (
    id_train                 INTEGER PRIMARY KEY,
    code_train                VARCHAR(20),
    type_train                VARCHAR(20),
    capacite_totale           SMALLINT,
    capacite_classe1          SMALLINT,
    capacite_classe2          SMALLINT,
    id_gare_depart            INTEGER,   -- gare "de base" du train (pas de FK stricte : réf. purement descriptive)
    id_gare_arrivee           INTEGER,
    ville_depart               VARCHAR(50),
    ville_arrivee              VARCHAR(50),
    annee_mise_en_service     SMALLINT,
    statut_train              VARCHAR(20),
    duree_estimee_minutes     SMALLINT,
    energie                   VARCHAR(20)
);

-- Alimentée par les 5 fichiers TRAJET_2021..2025 (IDs globalement uniques)
CREATE TABLE IF NOT EXISTS raw.trajet (
    id_trajet             INTEGER PRIMARY KEY,
    id_train              INTEGER,
    id_gare_depart        INTEGER,
    id_gare_arrivee       INTEGER,
    date_depart           DATE,
    heure_depart          TIME,
    heure_arrivee_prevue  TIME,
    distance_km           INTEGER,
    statut_circulation    VARCHAR(20)
);

-- Alimentée par les 5 fichiers RESERVATION_2021..2025_2M (IDs globalement uniques)
CREATE TABLE IF NOT EXISTS raw.reservation (
    id_reservation        INTEGER PRIMARY KEY,
    id_client             INTEGER,
    id_trajet             INTEGER,
    date_reservation      DATE,
    tarif_type            VARCHAR(20),
    classe_reservee       VARCHAR(10),
    prix_unitaire         NUMERIC(10,2),
    nb_passagers          SMALLINT,
    montant_total         NUMERIC(12,2),
    canal_vente           VARCHAR(20),
    mode_paiement         VARCHAR(30),
    statut_reservation    VARCHAR(20)
);
