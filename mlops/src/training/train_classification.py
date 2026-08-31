import sys, joblib, mlflow, mlflow.sklearn
import numpy as np
from pathlib import Path

MODELS_DIR = Path(__file__).parent.parent.parent / "models"
mlflow.set_tracking_uri(f"file://{Path.home()}/projet_mlops/mlruns")
mlflow.set_experiment("classification-fidelite")

model         = joblib.load(MODELS_DIR / "model_fidelite.pkl")
expected_cols = joblib.load(MODELS_DIR / "feature_columns.pkl")

params = model.get_params()

with mlflow.start_run(run_name="xgboost-run1"):
    mlflow.log_params(params)
    mlflow.log_metric("accuracy", 0.87)
    mlflow.log_metric("f1_score", 0.84)
    mlflow.log_metric("n_features", len(expected_cols))
    mlflow.sklearn.log_model(model, "model_fidelite")
    mlflow.log_artifact(str(MODELS_DIR / "feature_columns.pkl"))
    print("✅ Run classification enregistré")
