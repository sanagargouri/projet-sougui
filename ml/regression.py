# =============================================================================
# regression.py
# B2C Order Amount Prediction — Random Forest Regressor
# Input  : JSON payload (from n8n / FastAPI)
# Output : JSON → stdout + predictions_regression.json
# =============================================================================

import sys
import json
import warnings
import joblib
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ── 1. LOAD MODEL & FEATURE SCHEMA ────────────────────────────────────────────
from pathlib import Path
BASE_DIR      = Path(__file__).parent
MODEL_PATH    = BASE_DIR / "model_regression.pkl"
FEATURES_PATH = BASE_DIR / "feature_columns_regression.pkl"

try:
    model         = joblib.load(MODEL_PATH)
    expected_cols = joblib.load(FEATURES_PATH)
except FileNotFoundError as e:
    print(json.dumps({"error": f"Model file not found: {e}"}))
    sys.exit(1)


# ── 2. FEATURE ENGINEERING ────────────────────────────────────────────────────
def build_features(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Reproduces the exact same feature engineering as the training notebook.
    Input  : raw DataFrame with original columns from DW query
    Output : feature DataFrame ready for prediction (no target column)
    """
    df = df_raw.copy()

    # Derived features
    if "Montant_Remise" in df.columns and "prix_catalogue" in df.columns:
        df["remise_pct"] = (
            df["Montant_Remise"] /
            df["prix_catalogue"].replace(0, np.nan)
        ).fillna(0).clip(0, 1)

    if "Prix_Unité_moyen" in df.columns and "prix_catalogue" in df.columns:
        df["ratio_prix_catalogue"] = (
            df["Prix_Unité_moyen"] /
            df["prix_catalogue"].replace(0, np.nan)
        ).fillna(1).clip(0, 2)

    # Drop columns not used as features
    drop_cols = ["Numero_Commande", "Montant_de_la_commande"]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns])

    # Align columns with training schema
    # Note: pipeline handles encoding internally (ColumnTransformer)
    # We just need to ensure the raw columns match what was seen at training
    df = df.reindex(columns=expected_cols, fill_value=0)

    return df


# ── 3. PREDICT ────────────────────────────────────────────────────────────────
def predict(payload: list[dict]) -> list[dict]:
    """
    payload : list of B2C order records (raw, as extracted from DW)
    returns : list of {commande_id, montant_predit}
    """
    df_raw = pd.DataFrame(payload)

    # Keep order ID aside before feature engineering
    commande_ids = (
        df_raw["Numero_Commande"].tolist()
        if "Numero_Commande" in df_raw.columns
        else list(range(len(df_raw)))
    )

    X = build_features(df_raw)

    predictions = model.predict(X)

    results = [
        {
            "commande_id":    str(cid),
            "montant_predit": round(float(pred), 2),
        }
        for cid, pred in zip(commande_ids, predictions)
    ]

    return results


# ── 4. ENTRY POINT ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    """
    Usage:
        python regression.py '[{...}, {...}]'
        python regression.py  (reads stdin if no arg)

    Output:
        - Prints JSON array to stdout  → consumed by n8n / FastAPI
        - Writes predictions_regression.json
    """
    if len(sys.argv) > 1:
        raw_input = sys.argv[1]
    else:
         raw_input = sys.stdin.read()  # KEEP THIS UNCOMMENTED

    try:
        payload = json.loads(raw_input)
        if isinstance(payload, dict):
            payload = [payload]
    except json.JSONDecodeError as e:
        print(json.dumps({"error": f"Invalid JSON input: {e}"}))
        sys.exit(1)

    results = predict(payload)

    print(json.dumps(results, ensure_ascii=False, indent=2))

    pd.DataFrame(results).to_json(
        "predictions_regression.json",
        orient="records",
        force_ascii=False,
        indent=2,
    )
