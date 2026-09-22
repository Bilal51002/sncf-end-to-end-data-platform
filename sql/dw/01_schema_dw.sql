-- =====================================================================
-- Data Warehouse SNCF : constellation de faits (PostgreSQL)
-- fact_trajet      : grain = un train qui circule un jour donné
-- fact_reservation : grain = une réservation (billet vendu)
-- Les deux faits partagent dim_date et dim_train ; fact_trajet référence
-- dim_gare deux fois (départ / arrivée) ; fact_reservation référence
-- fact_trajet pour connaître le voyage concerné par le billet.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS dw;

-- ---------------------------------------------------------------------
-- DIM_DATE
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_key      INTEGER      PRIMARY KEY,        -- format yyyymmdd
    full_date     DATE         NOT NULL UNIQUE,
    year          SMALLINT     NOT NULL,
    quarter       SMALLINT     NOT NULL,
    month         SMALLINT     NOT NULL,
    month_name    VARCHAR(10)  NOT NULL,
    day_of_month  SMALLINT     NOT NULL,
    day_of_week   SMALLINT     NOT NULL,            -- 1 = lundi ... 7 = dimanche
    day_name      VARCHAR(10)  NOT NULL,
    is_weekend    BOOLEAN      NOT NULL
);

-- Génère le calendrier en une seule requête (pas besoin de Talend ici)
INSERT INTO dw.dim_date
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER,
    d,
    EXTRACT(YEAR FROM d)::SMALLINT,
    EXTRACT(QUARTER FROM d)::SMALLINT,
    EXTRACT(MONTH FROM d)::SMALLINT,
    TO_CHAR(d, 'TMMonth'),
    EXTRACT(DAY FROM d)::SMALLINT,
    EXTRACT(ISODOW FROM d)::SMALLINT,
    TO_CHAR(d, 'TMDay'),
    EXTRACT(ISODOW FROM d) IN (6, 7)
FROM generate_series('2019-01-01'::DATE, '2026-12-31'::DATE, '1 day') AS d
ON CONFLICT (date_key) DO NOTHING;

-- ---------------------------------------------------------------------
-- DIM_CLIENT (type 1 : une seule version, mise à jour en place)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.dim_client (
    client_key             INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_client               INTEGER      NOT NULL UNIQUE,
    nom                     VARCHAR(50),
    prenom                  VARCHAR(50),
    date_naissance          DATE,
    sexe                    VARCHAR(1),             -- normalisé : 'F' ou 'H'
    type_client             VARCHAR(30),
    ville                   VARCHAR(50),             -- normalisée : casse titre
    code_postal             VARCHAR(10),
    pays                    VARCHAR(50),
    email                   VARCHAR(100),
    telephone               VARCHAR(20),
    date_creation_compte    DATE,
    statut_compte           VARCHAR(20),
    load_ts                 TIMESTAMP    NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------
-- DIM_GARE
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.dim_gare (
    gare_key                INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_gare                 INTEGER      NOT NULL UNIQUE,
    nom_gare                VARCHAR(60)  NOT NULL,
    ville                   VARCHAR(50),
    region                  VARCHAR(50),
    pays                    VARCHAR(50),
    nb_quais                SMALLINT,
    type_gare               VARCHAR(30),
    taille_gare             VARCHAR(20),
    electrification         BOOLEAN,
    annee_mise_en_service   SMALLINT,
    latitude                NUMERIC(9,6),
    longitude               NUMERIC(9,6),
    categorie_strategique   VARCHAR(30),
    load_ts                 TIMESTAMP    NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------
-- DIM_TRAIN
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.dim_train (
    train_key               INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_train                INTEGER      NOT NULL UNIQUE,
    code_train               VARCHAR(20)  NOT NULL,
    type_train               VARCHAR(20),
    capacite_totale          SMALLINT,
    capacite_classe1         SMALLINT,
    capacite_classe2         SMALLINT,
    ville_depart_base        VARCHAR(50),
    ville_arrivee_base       VARCHAR(50),
    annee_mise_en_service    SMALLINT,
    statut_train             VARCHAR(20),
    duree_estimee_minutes    SMALLINT,
    energie                  VARCHAR(20),
    load_ts                  TIMESTAMP    NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------
-- FACT_TRAJET : un train qui circule un jour donné
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.fact_trajet (
    trajet_key            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_trajet             INTEGER      NOT NULL UNIQUE,   -- clé dégénérée (référencée par fact_reservation)

    date_key              INTEGER      NOT NULL REFERENCES dw.dim_date (date_key),
    train_key             INTEGER      NOT NULL REFERENCES dw.dim_train (train_key),
    gare_depart_key       INTEGER      NOT NULL REFERENCES dw.dim_gare (gare_key),
    gare_arrivee_key      INTEGER      NOT NULL REFERENCES dw.dim_gare (gare_key),

    heure_depart          TIME,
    heure_arrivee_prevue  TIME,
    distance_km           INTEGER      CHECK (distance_km >= 0),
    statut_circulation    VARCHAR(20),

    load_ts               TIMESTAMP    NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_fact_trajet_date   ON dw.fact_trajet (date_key);
CREATE INDEX IF NOT EXISTS ix_fact_trajet_train  ON dw.fact_trajet (train_key);
CREATE INDEX IF NOT EXISTS ix_fact_trajet_gared  ON dw.fact_trajet (gare_depart_key);
CREATE INDEX IF NOT EXISTS ix_fact_trajet_garea  ON dw.fact_trajet (gare_arrivee_key);

-- ---------------------------------------------------------------------
-- FACT_RESERVATION : un billet vendu
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.fact_reservation (
    reservation_key      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_reservation        INTEGER      NOT NULL UNIQUE,

    date_key              INTEGER      NOT NULL REFERENCES dw.dim_date (date_key),
    client_key            INTEGER      NOT NULL REFERENCES dw.dim_client (client_key),
    trajet_key            BIGINT       NOT NULL REFERENCES dw.fact_trajet (trajet_key),

    tarif_type            VARCHAR(20),
    classe_reservee       VARCHAR(10),
    prix_unitaire         NUMERIC(10,2) NOT NULL CHECK (prix_unitaire >= 0),
    nb_passagers          SMALLINT      NOT NULL CHECK (nb_passagers > 0),
    montant_total         NUMERIC(12,2) NOT NULL CHECK (montant_total >= 0),
    canal_vente           VARCHAR(20),
    mode_paiement         VARCHAR(30),
    statut_reservation    VARCHAR(20),

    load_ts               TIMESTAMP    NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_fact_reservation_date     ON dw.fact_reservation (date_key);
CREATE INDEX IF NOT EXISTS ix_fact_reservation_client   ON dw.fact_reservation (client_key);
CREATE INDEX IF NOT EXISTS ix_fact_reservation_trajet   ON dw.fact_reservation (trajet_key);
