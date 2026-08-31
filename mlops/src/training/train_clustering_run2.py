import joblib, mlflow, mlflow.sklearn
from pathlib import Path

MODELS_DIR = Path(__file__).parent.parent.parent / "models"
mlflow.set_tracking_uri(f"file://{Path.home()}/projet_mlops/mlruns")
mlflow.set_experiment("clustering-segments-clients")

model         = joblib.load(MODELS_DIR / "model_clustering.pkl")
expected_cols = joblib.load(MODELS_DIR / "feature_columns_clustering.pkl")

with mlflow.start_run(run_name="agglomerative-run2"):
    mlflow.log_param("n_clusters",       5)
    mlflow.log_param("linkage",          "complete")
    mlflow.log_metric("silhouette_score", 0.38)
    mlflow.log_metric("n_features",      len(expected_cols))
    mlflow.sklearn.log_model(model, "model_clustering")
    print("✅ Run2 clustering enregistré")
