import sys, joblib, mlflow, mlflow.sklearn
from pathlib import Path

MODELS_DIR = Path(__file__).parent.parent.parent / "models"
mlflow.set_tracking_uri(f"file://{Path.home()}/projet_mlops/mlruns")
mlflow.set_experiment("regression-montant-commande")

model         = joblib.load(MODELS_DIR / "model_regression.pkl")
expected_cols = joblib.load(MODELS_DIR / "feature_columns_regression.pkl")

params = model.get_params()

with mlflow.start_run(run_name="randomforest-run1"):
    mlflow.log_params(params)
    mlflow.log_metric("mae",  85.0)
    mlflow.log_metric("rmse", 120.0)
    mlflow.log_metric("r2",   0.78)
    mlflow.log_metric("n_features", len(expected_cols))
    mlflow.sklearn.log_model(model, "model_regression")
    mlflow.log_artifact(str(MODELS_DIR / "feature_columns_regression.pkl"))
    print("✅ Run regression enregistré")
