import sys
import json
import warnings
import traceback
import joblib
import numpy as np
import pandas as pd
from pathlib import Path

warnings.filterwarnings("ignore")

BASE_DIR      = Path(__file__).parent
MODEL_PATH    = BASE_DIR / "model_regression.pkl"
FEATURES_PATH = BASE_DIR / "feature_columns_regression.pkl"

try:
    model         = joblib.load(MODEL_PATH)
    expected_cols = joblib.load(FEATURES_PATH)
except FileNotFoundError as e:
    print(json.dumps({"error": str(e)}))
    sys.exit(1)

def build_features(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw.copy()
    df.columns = df.columns.str.strip()

    cat_cols = [
        "ville_client", "canal_produit", "categorie",
        "type_paiement", "Trimestre", "Semestre",
    ]
    for col in cat_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    num_cols = [
        "prix_catalogue", "En_Promo", "a_code_promo",
        "Annee", "Mois", "est_saison_haute",
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    drop_cols = [
        "Montant_de_la_commande", "Numero_Commande",
        "Type_client", "type_vente", "classe_prix",
        "Quantite_totale", "Prix_Unité_moyen",
        "Montant_Remise", "Montant_Livraison",
        "remise_pct", "ratio_prix_catalogue",
    ]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns])
    df = df.reindex(columns=expected_cols, fill_value=0)
    return df

def predict(payload: list[dict]) -> list[dict]:
    try:
        df_raw = pd.DataFrame(payload)

        if "Numero_Commande" in df_raw.columns:
            ids = df_raw["Numero_Commande"].astype(str).tolist()
        else:
            ids = list(range(len(df_raw)))

        prix_cat = pd.to_numeric(
            df_raw.get("prix_catalogue", pd.Series([0] * len(df_raw))),
            errors="coerce"
        ).fillna(0).values

        X     = build_features(df_raw)
        preds = model.predict(X)

        results = []
        for cid, p, pc in zip(ids, preds, prix_cat):
            montant = round(float(p), 2)
            qty_est = max(1, round(montant / pc)) if pc > 0 else 1
            results.append({
                "commande_id":    cid,
                "montant_predit": montant,
                "quantite_estimee": qty_est,
                "prix_catalogue": round(float(pc), 2),
            })
        return results

    except Exception as e:
        raise ValueError(traceback.format_exc())

if __name__ == "__main__":
    raw_input = sys.argv[1] if len(sys.argv) > 1 else sys.stdin.read()
    try:
        payload = json.loads(raw_input)
        if isinstance(payload, dict):
            payload = [payload]
    except Exception as e:
        print(json.dumps({"error": str(e)}))
        sys.exit(1)
    results = predict(payload)
    print(json.dumps(results, ensure_ascii=False, indent=2))
