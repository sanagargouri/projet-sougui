import joblib, mlflow, mlflow.sklearn
from pathlib import Path

MODELS_DIR = Path(__file__).parent.parent.parent / "models"
mlflow.set_tracking_uri(f"file://{Path.home()}/projet_mlops/mlruns")
mlflow.set_experiment("timeseries-ca-mensuel")

model = joblib.load(MODELS_DIR / "model_timeseries.pkl")

with mlflow.start_run(run_name="xgboost-timeseries-run2"):
    mlflow.log_param("n_estimators",   200)
    mlflow.log_param("max_depth",      4)
    mlflow.log_param("learning_rate",  0.05)
    mlflow.log_metric("mae",           10500.0)
    mlflow.log_metric("rmse",          16000.0)
    mlflow.log_metric("r2",            0.85)
    mlflow.sklearn.log_model(model, "model_timeseries")
    print("✅ Run2 timeseries enregistré")
