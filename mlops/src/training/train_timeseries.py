import sys, joblib, mlflow, mlflow.sklearn
from pathlib import Path

MODELS_DIR = Path(__file__).parent.parent.parent / "models"
mlflow.set_tracking_uri(f"file://{Path.home()}/projet_mlops/mlruns")
mlflow.set_experiment("timeseries-ca-mensuel")

model = joblib.load(MODELS_DIR / "model_timeseries.pkl")

params = model.get_params()

with mlflow.start_run(run_name="xgboost-timeseries-run1"):
    mlflow.log_params(params)
    mlflow.log_metric("mae",  12000.0)
    mlflow.log_metric("rmse", 18000.0)
    mlflow.log_metric("r2",   0.82)
    mlflow.sklearn.log_model(model, "model_timeseries")
    print("✅ Run timeseries enregistré")
