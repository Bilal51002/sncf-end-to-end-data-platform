-- ============================================================
-- ETL raw -> dw
-- A executer sur le container sncf_dw (base sncf_dw, user dw_user)
-- ============================================================

-- 1) Mise en place du Foreign Data Wrapper vers sncf_source
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

CREATE SERVER IF NOT EXISTS sncf_source_srv
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host 'sncf_source', port '5432', dbname 'sncf_oltp');

CREATE USER MAPPING IF NOT EXISTS FOR dw_user
    SERVER sncf_source_srv
    OPTIONS (user 'northwind_user', password 'changeme');

CREATE SCHEMA IF NOT EXISTS raw_remote;

-- Importe la structure des tables raw.* de sncf_source dans le schema local raw_remote
IMPORT FOREIGN SCHEMA raw
    FROM SERVER sncf_source_srv
    INTO raw_remote;

-- ============================================================
-- 2) Chargement des dimensions
-- ============================================================

-- dim_client
INSERT INTO dw.dim_client (
    id_client, nom, prenom, date_naissance, sexe, type_client,
    ville, code_postal, pays, email, telephone,
    date_creation_compte, statut_compte
)
SELECT
    id_client, nom, prenom, date_naissance, LEFT(sexe, 1), type_client,
    ville, code_postal, pays, email, telephone,
    date_creation_compte, statut_compte
FROM raw_remote.client
ON CONFLICT (id_client) DO NOTHING;

-- dim_gare
INSERT INTO dw.dim_gare (
    id_gare, nom_gare, ville, region, pays, nb_quais,
    type_gare, taille_gare, electrification,
    annee_mise_en_service, latitude, longitude, categorie_strategique
)
SELECT
    id_gare, nom_gare, ville, region, pays, nb_quais,
    type_gare, taille_gare,
    CASE WHEN electrification = 'Electrifiée' THEN true
         WHEN electrification = 'Non électrifiée' THEN false
         ELSE NULL END,
    COALESCE(annee_mise_en_service, annee_mise_service),
    latitude, longitude, categorie_strategique
FROM raw_remote.gare
ON CONFLICT (id_gare) DO NOTHING;

-- dim_train
INSERT INTO dw.dim_train (
    id_train, code_train, type_train, capacite_totale,
    capacite_classe1, capacite_classe2,
    ville_depart_base, ville_arrivee_base,
    annee_mise_en_service, statut_train, duree_estimee_minutes, energie
)
SELECT
    id_train, code_train, type_train, capacite_totale,
    capacite_classe1, capacite_classe2,
    ville_depart, ville_arrivee,
    annee_mise_en_service, statut_train, duree_estimee_minutes, energie
FROM raw_remote.train
ON CONFLICT (id_train) DO NOTHING;

-- ============================================================
-- 3) Chargement de fact_trajet
-- ============================================================

INSERT INTO dw.fact_trajet (
    id_trajet, date_key, train_key, gare_depart_key, gare_arrivee_key,
    heure_depart, heure_arrivee_prevue, distance_km, statut_circulation
)
SELECT
    t.id_trajet,
    TO_CHAR(t.date_depart, 'YYYYMMDD')::int,
    dtr.train_key,
    dgd.gare_key,
    dga.gare_key,
    t.heure_depart,
    t.heure_arrivee_prevue,
    t.distance_km,
    t.statut_circulation
FROM raw_remote.trajet t
JOIN dw.dim_train dtr ON dtr.id_train = t.id_train
JOIN dw.dim_gare  dgd ON dgd.id_gare  = t.id_gare_depart
JOIN dw.dim_gare  dga ON dga.id_gare  = t.id_gare_arrivee
ON CONFLICT (id_trajet) DO NOTHING;

-- ============================================================
-- 4) Chargement de fact_reservation
-- ============================================================

INSERT INTO dw.fact_reservation (
    id_reservation, date_key, client_key, trajet_key,
    tarif_type, classe_reservee, prix_unitaire, nb_passagers,
    montant_total, canal_vente, mode_paiement, statut_reservation
)
SELECT
    r.id_reservation,
    TO_CHAR(r.date_reservation, 'YYYYMMDD')::int,
    dc.client_key,
    ft.trajet_key,
    r.tarif_type, r.classe_reservee, r.prix_unitaire, r.nb_passagers,
    r.montant_total, r.canal_vente, r.mode_paiement, r.statut_reservation
FROM raw_remote.reservation r
JOIN dw.dim_client dc ON dc.id_client = r.id_client
JOIN dw.fact_trajet ft ON ft.id_trajet = r.id_trajet
ON CONFLICT (id_reservation) DO NOTHING;

-- ============================================================
-- 5) Verification des volumes
-- ============================================================

SELECT 'dim_client' t, count(*) FROM dw.dim_client
UNION ALL SELECT 'dim_gare', count(*) FROM dw.dim_gare
UNION ALL SELECT 'dim_train', count(*) FROM dw.dim_train
UNION ALL SELECT 'fact_trajet', count(*) FROM dw.fact_trajet
UNION ALL SELECT 'fact_reservation', count(*) FROM dw.fact_reservation;
