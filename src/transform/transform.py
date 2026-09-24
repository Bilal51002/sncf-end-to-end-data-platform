
import pandas as pd


def _to_date_key(date_series: pd.Series) -> pd.Series:
    """Convertit une colonne date en cle entiere YYYYMMDD (int), NaT -> NA."""
    dt = pd.to_datetime(date_series)
    return dt.dt.strftime("%Y%m%d").astype("Int64")


def _normalize_sexe(sexe_series: pd.Series) -> pd.Series:

    mapping = {
        "homme": "H",
        "femme": "F",
        "h": "H",
        "f": "F",
        "m": "H",  # 'M' pour Masculin, distinct du 'M' anglais
    }
    normalized = sexe_series.astype(str).str.strip().str.lower().map(mapping)
    # repli : si valeur non reconnue, on garde la premiere lettre en majuscule
    fallback = sexe_series.astype(str).str.strip().str[:1].str.upper()
    return normalized.fillna(fallback)


def _normalize_ville(ville_series: pd.Series) -> pd.Series:
    """Uniformise la casse des noms de ville (Lille, Strasbourg, Nantes...)."""
    return ville_series.astype(str).str.strip().str.title()



def transform_dim_client(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["sexe"] = _normalize_sexe(out["sexe"])
    out["ville"] = _normalize_ville(out["ville"])
    cols = [
        "id_client", "nom", "prenom", "date_naissance", "sexe", "type_client",
        "ville", "code_postal", "pays", "email", "telephone",
        "date_creation_compte", "statut_compte",
    ]
    return out[cols]


def transform_dim_gare(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["ville"] = _normalize_ville(out["ville"])
    out["electrification"] = out["electrification"].map(
        {"Electrifiée": True, "Non électrifiée": False}
    )
    # doublon de colonne dans raw : on garde annee_mise_en_service, on comble
    # les trous avec annee_mise_service si besoin
    out["annee_mise_en_service"] = out["annee_mise_en_service"].fillna(
        out["annee_mise_service"]
    )
    cols = [
        "id_gare", "nom_gare", "ville", "region", "pays", "nb_quais",
        "type_gare", "taille_gare", "electrification",
        "annee_mise_en_service", "latitude", "longitude", "categorie_strategique",
    ]
    return out[cols]


def transform_dim_train(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["ville_depart_base"] = _normalize_ville(out["ville_depart"])
    out["ville_arrivee_base"] = _normalize_ville(out["ville_arrivee"])
    cols = [
        "id_train", "code_train", "type_train", "capacite_totale",
        "capacite_classe1", "capacite_classe2",
        "ville_depart_base", "ville_arrivee_base",
        "annee_mise_en_service", "statut_train", "duree_estimee_minutes", "energie",
    ]
    return out[cols]


# ------------------------------------------------------------------
# Faits
# ------------------------------------------------------------------

def transform_fact_trajet(
    df: pd.DataFrame, dim_train_map: pd.DataFrame, dim_gare_map: pd.DataFrame
) -> pd.DataFrame:
    """
    dim_train_map : colonnes [id_train, train_key]
    dim_gare_map  : colonnes [id_gare, gare_key]
    """
    out = df.copy()
    out["date_key"] = _to_date_key(out["date_depart"])

    out = out.merge(dim_train_map, on="id_train", how="inner")

    gare_depart = dim_gare_map.rename(
        columns={"id_gare": "id_gare_depart", "gare_key": "gare_depart_key"}
    )
    out = out.merge(gare_depart, on="id_gare_depart", how="inner")

    gare_arrivee = dim_gare_map.rename(
        columns={"id_gare": "id_gare_arrivee", "gare_key": "gare_arrivee_key"}
    )
    out = out.merge(gare_arrivee, on="id_gare_arrivee", how="inner")

    cols = [
        "id_trajet", "date_key", "train_key", "gare_depart_key", "gare_arrivee_key",
        "heure_depart", "heure_arrivee_prevue", "distance_km", "statut_circulation",
    ]
    return out[cols]


def transform_fact_reservation(
    df: pd.DataFrame, dim_client_map: pd.DataFrame, fact_trajet_map: pd.DataFrame
) -> pd.DataFrame:
    """
    dim_client_map  : colonnes [id_client, client_key]
    fact_trajet_map : colonnes [id_trajet, trajet_key]
    """
    out = df.copy()
    out["date_key"] = _to_date_key(out["date_reservation"])

    out = out.merge(dim_client_map, on="id_client", how="inner")
    out = out.merge(fact_trajet_map, on="id_trajet", how="inner")

    cols = [
        "id_reservation", "date_key", "client_key", "trajet_key",
        "tarif_type", "classe_reservee", "prix_unitaire", "nb_passagers",
        "montant_total", "canal_vente", "mode_paiement", "statut_reservation",
    ]
    return out[cols]
