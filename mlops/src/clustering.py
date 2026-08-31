# =============================================================================
# clustering.py
# Endpoint : POST /predict/clustering → segment client (Agglomerative n=5)
# Stratégie : nearest centroid filtré par type client (B2B/B2C)
# =============================================================================
import joblib
import numpy as np
import pandas as pd
from pathlib import Path
import warnings
from sklearn.exceptions import InconsistentVersionWarning
warnings.filterwarnings("ignore", category=InconsistentVersionWarning)

# ── Chargement des artefacts ──────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent.parent / "models"
model    = joblib.load(BASE_DIR / "model_clustering.pkl")
scaler   = joblib.load(BASE_DIR / "scaler_clustering.pkl")
features = joblib.load(BASE_DIR / "feature_columns_clustering.pkl")

SEGMENT_LABELS = {
    0: "B2C Dormants",
    1: "B2B Actifs Moyens",
    2: "B2C Remisés (à risque)",
    3: "B2B Grands Comptes",
    4: "B2C Récents Actifs",
}

B2C_CLUSTERS = [0, 2, 4]
B2B_CLUSTERS = [1, 3]


def _nearest_centroid_filtered(X_scaled: np.ndarray, centroids: np.ndarray, is_entreprise: list) -> np.ndarray:
    """Assigne chaque point au centroïde le plus proche dans le bon sous-ensemble B2B/B2C."""
    dists = np.linalg.norm(X_scaled[:, None, :] - centroids[None, :, :], axis=2)
    results = []
    for i in range(len(X_scaled)):
        pool = B2B_CLUSTERS if is_entreprise[i] == 1 else B2C_CLUSTERS
        best = min(pool, key=lambda k: dists[i, k])
        results.append(best)
    return np.array(results)


def predict(payload: list[dict]) -> list[dict]:
    df = pd.DataFrame(payload)[features].fillna(0)
    is_entreprise = df['Is_Entreprise'].tolist()
    X  = scaler.transform(df)
    centroids = model.centroids_
    clusters  = _nearest_centroid_filtered(X, centroids, is_entreprise)

    results = []
    for i, cluster in enumerate(clusters):
        results.append({
            **payload[i],
            "Cluster_ID":    int(cluster),
            "Segment_Label": SEGMENT_LABELS.get(int(cluster), "Inconnu"),
        })
    return results
