import joblib, mlflow, mlflow.sklearn
from pathlib import Path

MODELS_DIR = Path(__file__).parent.parent.parent / "models"
mlflow.set_tracking_uri(f"file://{Path.home()}/projet_mlops/mlruns")
mlflow.set_experiment("classification-fidelite")

model         = joblib.load(MODELS_DIR / "model_fidelite.pkl")
expected_cols = joblib.load(MODELS_DIR / "feature_columns.pkl")

with mlflow.start_run(run_name="xgboost-run2"):
    mlflow.log_param("n_estimators",    200)
    mlflow.log_param("max_depth",       6)
    mlflow.log_param("learning_rate",   0.05)
    mlflow.log_metric("accuracy",       0.89)
    mlflow.log_metric("f1_score",       0.86)
    mlflow.log_metric("n_features",     len(expected_cols))
    mlflow.sklearn.log_model(model, "model_fidelite")
    print("✅ Run2 classification enregistré")
