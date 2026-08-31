import sys, time, logging, os, secrets, smtplib, random, string
from pathlib import Path
from typing import Any, Optional
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import bcrypt
import jwt
import pyodbc
import httpx
from fastapi import FastAPI, HTTPException, Depends, UploadFile, File
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, EmailStr

from classification import predict as predict_classification
from regression     import predict as predict_regression
from timeseries     import predict as predict_timeseries
from clustering     import predict as predict_clustering

from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────

JWT_SECRET = "s0ugu1_jwt_s3cret_k3y_2026!xKz"
JWT_ALGORITHM = "HS256"
JWT_EXPIRY_HOURS = 8

DB_CONNECTION_STRING = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=192.168.240.1,1434;"
    "DATABASE=souguiDB;"
    "UID=sa;"
    "PWD=sana;"
    "TrustServerCertificate=yes;"
    "Encrypt=yes;"
)

GMAIL_ADDRESS = "sana.gargouri1@gmail.com"
GMAIL_APP_PASSWORD = "kowo aqro mhqp vtej"

# ── App ─────────────────────────────────────────

app = FastAPI(title="E-Commerce ML API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

security = HTTPBearer()

# ── DB Helper ───────────────────────────────────

def get_db():
    conn = pyodbc.connect(DB_CONNECTION_STRING)
    try:
        yield conn
    finally:
        conn.close()

def get_db_connection():
    return pyodbc.connect(DB_CONNECTION_STRING)

# ── JWT Helper ──────────────────────────────────

def create_token(user_id: int, username: str, role: str) -> str:
    payload = {
        "sub": str(user_id),
        "username": username,
        "role": role,
        "exp": datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRY_HOURS),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

def decode_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        payload = jwt.decode(credentials.credentials, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expiré")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Token invalide")

# ── Email Helper ────────────────────────────────

def send_reset_email(to_email: str, code: str):
    msg = MIMEMultipart()
    msg["From"] = GMAIL_ADDRESS
    msg["To"] = to_email
    msg["Subject"] = "Sougui - Code de réinitialisation"

    body = f"""
    <html>
    <body style="font-family: Arial, sans-serif; padding: 20px;">
        <h2 style="color: #2c3e50;">Sougui - Réinitialisation du mot de passe</h2>
        <p>Votre code de réinitialisation est :</p>
        <h1 style="color: #e74c3c; letter-spacing: 5px; font-size: 36px;">{code}</h1>
        <p>Ce code expire dans <strong>15 minutes</strong>.</p>
        <p>Si vous n'avez pas demandé cette réinitialisation, ignorez cet email.</p>
        <hr>
        <p style="color: #999; font-size: 12px;">Sougui - Artisanat Tunisien</p>
    </body>
    </html>
    """
    msg.attach(MIMEText(body, "html"))

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
            server.send_message(msg)
        logger.info(f"Reset email sent to {to_email}")
    except Exception as e:
        logger.error(f"Email error: {e}")
        raise HTTPException(status_code=500, detail="Erreur d'envoi de l'email")

# ── Auth Models ─────────────────────────────────

class RegisterRequest(BaseModel):
    username: str
    email: str
    password: str
    role: Optional[str] = "marketing"

class LoginRequest(BaseModel):
    email: str
    password: str

class ForgotPasswordRequest(BaseModel):
    email: str

class ResetPasswordRequest(BaseModel):
    email: str
    code: str
    new_password: str

# ── Auth Endpoints ──────────────────────────────

@app.post("/auth/register")
def register(req: RegisterRequest):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Check if email already exists
        cursor.execute("SELECT Id FROM Users WHERE Email = ?", req.email)
        if cursor.fetchone():
            raise HTTPException(status_code=400, detail="Cet email est déjà utilisé")

        # Check if username already exists
        cursor.execute("SELECT Id FROM Users WHERE Username = ?", req.username)
        if cursor.fetchone():
            raise HTTPException(status_code=400, detail="Ce nom d'utilisateur est déjà pris")

        # Hash password
        hashed = bcrypt.hashpw(req.password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

        # Insert user
        cursor.execute(
            """INSERT INTO Users (Username, Email, PasswordHash, Role, CreatedAt)
               VALUES (?, ?, ?, ?, ?)""",
            req.username, req.email, hashed, req.role, datetime.now()
        )
        conn.commit()

        return {"message": "Inscription réussie"}
    finally:
        conn.close()

@app.post("/auth/login")
def login(req: LoginRequest):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT Id, Username, Email, PasswordHash, Role FROM Users WHERE Email = ?",
            req.email
        )
        user = cursor.fetchone()

        if not user:
            raise HTTPException(status_code=401, detail="Email ou mot de passe incorrect")

        if not bcrypt.checkpw(req.password.encode("utf-8"), user.PasswordHash.encode("utf-8")):
            raise HTTPException(status_code=401, detail="Email ou mot de passe incorrect")

        token = create_token(user.Id, user.Username, user.Role)

        return {
            "token": token,
            "user": {
                "id": user.Id,
                "username": user.Username,
                "email": user.Email,
                "role": user.Role,
            }
        }
    finally:
        conn.close()

@app.post("/auth/forgot-password")
def forgot_password(req: ForgotPasswordRequest):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT Id FROM Users WHERE Email = ?", req.email)
        user = cursor.fetchone()

        if not user:
            # Don't reveal if email exists
            return {"message": "Si cet email existe, un code de réinitialisation a été envoyé"}

        # Generate 6-digit code
        code = ''.join(random.choices(string.digits, k=6))
        expiry = datetime.now() + timedelta(minutes=15)

        cursor.execute(
            "UPDATE Users SET ResetCode = ?, ResetCodeExpiry = ? WHERE Email = ?",
            code, expiry, req.email
        )
        conn.commit()

        send_reset_email(req.email, code)

        return {"message": "Si cet email existe, un code de réinitialisation a été envoyé"}
    finally:
        conn.close()

@app.post("/auth/reset-password")
def reset_password(req: ResetPasswordRequest):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT Id, ResetCode, ResetCodeExpiry FROM Users WHERE Email = ?",
            req.email
        )
        user = cursor.fetchone()

        if not user or not user.ResetCode:
            raise HTTPException(status_code=400, detail="Code invalide")

        if user.ResetCode != req.code:
            raise HTTPException(status_code=400, detail="Code incorrect")

        if user.ResetCodeExpiry < datetime.now():
            raise HTTPException(status_code=400, detail="Code expiré")

        hashed = bcrypt.hashpw(req.new_password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

        cursor.execute(
            "UPDATE Users SET PasswordHash = ?, ResetCode = NULL, ResetCodeExpiry = NULL WHERE Email = ?",
            hashed, req.email
        )
        conn.commit()

        return {"message": "Mot de passe réinitialisé avec succès"}
    finally:
        conn.close()

@app.get("/auth/me")
def get_me(payload: dict = Depends(decode_token)):
    return {
        "id": payload["sub"],
        "username": payload["username"],
        "role": payload["role"],
    }

# ── Admin Endpoints ─────────────────────────────

@app.get("/admin/users")
def get_users(payload: dict = Depends(decode_token)):
    if payload.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Accès réservé aux administrateurs")
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT Id, Username, Email, Role, CreatedAt FROM Users")
        users = []
        for row in cursor.fetchall():
            users.append({
                "id": row.Id,
                "username": row.Username,
                "email": row.Email,
                "role": row.Role,
                "created_at": row.CreatedAt.strftime("%d/%m/%Y %H:%M") if row.CreatedAt else ""
            })
        return users
    finally:
        conn.close()

@app.post("/admin/users")
def admin_create_user(req: RegisterRequest, payload: dict = Depends(decode_token)):
    if payload.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Accès réservé aux administrateurs")
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT Id FROM Users WHERE Email = ?", req.email)
        if cursor.fetchone():
            raise HTTPException(status_code=400, detail="Cet email est déjà utilisé")
        cursor.execute("SELECT Id FROM Users WHERE Username = ?", req.username)
        if cursor.fetchone():
            raise HTTPException(status_code=400, detail="Ce nom d'utilisateur est déjà pris")
        hashed = bcrypt.hashpw(req.password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
        cursor.execute(
            "INSERT INTO Users (Username, Email, PasswordHash, Role, CreatedAt) VALUES (?, ?, ?, ?, ?)",
            req.username, req.email, hashed, req.role, datetime.now()
        )
        conn.commit()
        return {"message": "Compte créé avec succès"}
    finally:
        conn.close()
# ── Admin Endpoints ─────────────────────────────
@app.delete("/admin/users/{user_id}")
def delete_user(user_id: int, payload: dict = Depends(decode_token)):
    if payload.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Accès réservé aux administrateurs")
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM Users WHERE Id = ?", user_id)
        conn.commit()
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Utilisateur non trouvé")
        return {"message": "Utilisateur supprimé"}
    finally:
        conn.close()
# ── ETL Endpoint ────────────────────────────────

TALEND_URL = "http://localhost:5001/run/etl"

@app.post("/run/etl")
def run_etl():
    logger.info("[ETL] Lancement des jobs Talend")
    start = time.time()
    try:
        response = httpx.post(TALEND_URL, timeout=600)
        result = response.json()
        duration = round((time.time() - start) * 1000, 2)
        logger.info(f"[ETL] Terminé en {duration} ms")
        return result
    except httpx.ConnectError:
        raise HTTPException(status_code=503, detail="Conteneur Talend inaccessible")
    except Exception as e:
        logger.error(f"[ETL] Erreur : {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ── Prediction models ───────────────────────────

class PredictRequest(BaseModel):
    data: list[dict[str, Any]]

class PredictResponse(BaseModel):
    model:       str
    n_records:   int
    duration_ms: float
    predictions: list[dict[str, Any]]

# ── Chat models ─────────────────────────────────

class ChatMessage(BaseModel):
    role: str
    content: str

class ChatRequest(BaseModel):
    messages: list[ChatMessage]

class ChatResponse(BaseModel):
    reply: str

# ── System prompt for Sougui chatbot ────────────

SOUGUI_SYSTEM_PROMPT = """Tu es l'assistant IA de Sougui, une entreprise tunisienne de e-commerce spécialisée dans l'artisanat tunisien (céramiques, bois d'olivier, cuivre, foutas, coffrets cadeaux, art de la table).

Ton rôle :
- Répondre aux questions business sur Sougui : produits, clients, stratégie commerciale, positionnement marché
- Donner des conseils stratégiques adaptés au contexte tunisien et au secteur de l'artisanat
- Aider à interpréter les tendances commerciales et proposer des actions concrètes
- Répondre en français, de manière concise et professionnelle

Ce que tu sais sur Sougui :
- Vend de l'artisanat tunisien en ligne et en magasin
- Clientèle B2C (particuliers) et B2B (entreprises)
- Présent dans les principales villes tunisiennes (Tunis, Sfax, Sousse, etc.)
- Catégories : céramiques, bois d'olivier, articles en cuivre, couffins & foutas, coffrets cadeaux, carafes, art de la table
- Modes de paiement : carte bancaire, espèces, virement, Paymee, paiement à la livraison
- La plateforme utilise des modèles ML pour : prédiction de fidélité client, estimation de valeur commande, segmentation client, prévision du chiffre d'affaires

Règles :
- Sois concis (3-5 phrases max sauf si la question demande plus)
- Propose toujours des actions concrètes quand c'est pertinent
- Si tu ne sais pas, dis-le honnêtement
- Ne réponds pas aux questions sans rapport avec Sougui ou le business/e-commerce
"""

# ── Endpoints ───────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}

def run_prediction(model_name, predict_fn, payload):
    start = time.time()
    try:
        results = predict_fn(payload)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return PredictResponse(
        model=model_name,
        n_records=len(results),
        duration_ms=round((time.time() - start) * 1000, 2),
        predictions=results
    )

@app.post("/predict/classification", response_model=PredictResponse)
def classification_endpoint(request: PredictRequest):
    return run_prediction("classification", predict_classification, request.data)

@app.post("/predict/regression", response_model=PredictResponse)
def regression_endpoint(request: PredictRequest):
    return run_prediction("regression", predict_regression, request.data)

@app.post("/predict/timeseries", response_model=PredictResponse)
def timeseries_endpoint(request: PredictRequest):
    return run_prediction("timeseries", predict_timeseries, request.data)

@app.post("/predict/clustering", response_model=PredictResponse)
def clustering_endpoint(request: PredictRequest):
    return run_prediction("clustering", predict_clustering, request.data)

@app.post("/chat", response_model=ChatResponse)
async def chat_endpoint(request: ChatRequest):
    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="GROQ_API_KEY non configurée. Exécutez: export GROQ_API_KEY=votre-clé")

    messages = [
        {"role": "system", "content": SOUGUI_SYSTEM_PROMPT},
        *[{"role": m.role, "content": m.content} for m in request.messages]
    ]

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "llama-3.3-70b-versatile",
                    "messages": messages,
                    "max_tokens": 1024,
                    "temperature": 0.7,
                },
            )
            response.raise_for_status()
            data = response.json()
            reply = data["choices"][0]["message"]["content"]
            return ChatResponse(reply=reply)
        except httpx.HTTPStatusError as e:
            logger.error(f"Groq API error: {e.response.text}")
            raise HTTPException(status_code=502, detail="Erreur de l'API Groq.")
        except Exception as e:
            logger.error(f"Chat error: {e}")
            raise HTTPException(status_code=500, detail="Erreur interne du chatbot.")
# ── Extraction Facture ──────────────────────────────
from fastapi.responses import FileResponse
import subprocess
import shutil

@app.post("/extract/facture")
def extract_facture(
    file: UploadFile = File(...),
    payload: dict = Depends(decode_token)
):
    if payload.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Accès réservé aux administrateurs")
    
    import tempfile
    tmp_dir = tempfile.mkdtemp()
    pdf_path = os.path.join(tmp_dir, file.filename)
    xlsx_path = os.path.join(tmp_dir, file.filename.replace(".pdf", ".xlsx"))
    
    try:
        with open(pdf_path, "wb") as f:
            f.write(file.file.read())
        
        result = subprocess.run(
            [sys.executable, str(Path(__file__).parent.parent / "src" / "extraction.py"), pdf_path, xlsx_path],
            capture_output=True, text=True, timeout=60
        )
        
        if not os.path.exists(xlsx_path):
            raise HTTPException(status_code=500, detail=f"Extraction échouée: {result.stderr}")
        
        return FileResponse(
            xlsx_path,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=file.filename.replace(".pdf", ".xlsx")
        )
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=500, detail="Extraction trop longue (timeout 60s)")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
