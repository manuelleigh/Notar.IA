import os
import hashlib
import secrets
import datetime as dt
import smtplib
from email.mime.text import MIMEText
from fastapi import FastAPI, Depends, UploadFile, File, HTTPException, Request
from auth import verify_token, create_token
from template_engine import render_html, html_to_pdf
from keynua_client import send_to_keynua, handle_webhook
from models import Usuario, Contrato, Evidencia, db

app = FastAPI()
os.makedirs(os.getenv("VIDEOS_DIR"), exist_ok=True)

VIDEOS_DIR = os.getenv("VIDEOS_DIR", "./videos")

# ---------------------------
# Utilidades
# ---------------------------
def generate_sha256(file_path: str) -> str:
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256.update(chunk)
    return sha256.hexdigest()

def send_email(to_email: str, subject: str, body: str):
    smtp_server = os.getenv("SMTP_HOST")
    smtp_port = os.getenv("SMTP_PORT")
    from_email = os.getenv("SMTP_EMAIL")
    password = os.getenv("SMTP_PASSWORD")

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = to_email

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(from_email, password)
        server.sendmail(from_email, [to_email], msg.as_string())

# ---------------------------
# Endpoints
# ---------------------------
@app.post("/login")
async def login(data: dict):
    user_id = data.get("user_id")
    if not user_id:
        return {"error": "Credenciales inválidas"}
    token = create_token(user_id)
    return {"access_token": token, "token_type": "bearer"}

@app.post("/next-turn")
async def next_turn(data: dict, user=Depends(verify_token)):
    contract_type = data["contract_type"]
    message = data["message"]
    current_slots = data.get("filled_slots", {})
    result = process_message(user["sub"], contract_type, current_slots, message)
    return result

@app.post("/preview")
async def preview_contract(data: dict, user=Depends(verify_token)):
    html = render_html(f"{data['contract_type']}_template.html", data["filled_slots"])
    return {"html": html}

@app.post("/confirm")
async def confirm_contract(data: dict, user=Depends(verify_token)):
    contrato = Contrato.query.get(data.get("contract_id"))
    if not contrato:
        raise HTTPException(status_code=404, detail="Contrato no encontrado")

    html = render_html(f"{data['contract_type']}_template.html", data['filled_slots'])
    pdf_path = html_to_pdf(html, f"./output/{data['contract_type']}.pdf")

    hash_doc = generate_sha256(pdf_path)
    codigo_probatorio = secrets.token_hex(4)
    token = secrets.token_urlsafe(16)
    link_expiration = dt.datetime.utcnow() + dt.timedelta(minutes=15)

    evidencia = Evidencia(
        contrato_id=contrato.id,
        tipo_id=1,
        metadatos={
            "hash_documento": hash_doc,
            "codigo_probatorio": codigo_probatorio,
            "link_temporal": token,
            "link_expiration": link_expiration
        }
    )
    db.session.add(evidencia)
    db.session.commit()

    base_front = os.getenv("URL_BASE_FRONTEND")
    enlace = f"{base_front}/video/{token}"

    # Enviar correo (SMTP o simulado)
    send_email(data.get("email"), "Enlace para video probatorio",
               f"Ingrese al siguiente enlace: {enlace}\nCódigo: {codigo_probatorio}")

    return {
        "evidence_id": evidencia.id,
        "token": token,
        "link": enlace,
        "codigo_probatorio": codigo_probatorio
    }

@app.get("/validate-link")
async def validate_link(token: str):
    evidencia = Evidencia.query.filter_by(metadatos={"link_temporal": token}).first()
    if not evidencia:
        raise HTTPException(status_code=404, detail="Token no encontrado")
    if evidencia.metadatos.get("estado_link") != "activo" or dt.datetime.utcnow() > evidencia.metadatos.get("link_expiration"):
        return {"valid": False, "reason": "Link expirado o bloqueado"}

    contrato = Contrato.query.get(evidencia.contrato_id)
    usuario = Usuario.query.get(contrato.creador_id)

    return {
        "valid": True,
        "datos_personales": {"nombre": usuario.nombre, "email": usuario.correo},
        "detalle_contrato": {"tipo": contrato.tipo_contrato_id, "filled_slots": contrato.contenido},
        "codigo_probatorio": evidencia.metadatos.get("codigo_probatorio")
    }

@app.post("/upload-video")
async def upload_video(token: str, file: UploadFile = File(...), user=Depends(verify_token)):
    evidencia = Evidencia.query.filter(Evidencia.metadatos["link_temporal"] == token).first()
    if not evidencia:
        raise HTTPException(status_code=404, detail="Token no encontrado")

    video_path = os.path.join(VIDEOS_DIR, f"{token}.mp4")
    with open(video_path, "wb") as f:
        f.write(await file.read())

    evidencia.metadatos["video_url"] = video_path
    evidencia.metadatos["intentos_video"] = evidencia.metadatos.get("intentos_video", 0) + 1
    if evidencia.metadatos["intentos_video"] >= 2:
        evidencia.metadatos["estado_link"] = "bloqueado"
    db.session.commit()

    return {
        "status": "bloqueado" if evidencia.metadatos["intentos_video"] >= 2 else "activo",
        "attempts": evidencia.metadatos["intentos_video"]
    }

@app.post("/webhook-keynua")
async def webhook_keynua(request: Request):
    data = await request.json()
    pdf_signed_path = data.get("signed_pdf_path")
    hash_final = generate_sha256(pdf_signed_path)
    tsa_timestamp = dt.datetime.utcnow()
    blockchain_hash = f"bc_{hash_final[:16]}"

    evidencia = Evidencia.query.get(data.get("evidence_id"))
    if evidencia:
        evidencia.metadatos.update({
            "hash_documento": hash_final,
            "blockchain_hash": blockchain_hash,
            "tsa_timestamp": tsa_timestamp,
            "estado_link": "firmado"
        })
        db.session.commit()

    return {"ok": True}

@app.get("/chats/history")
async def chat_history(user=Depends(verify_token)):
    history = []
    return {"history": history}

@app.get("/metrics")
async def metrics():
    tiempo_promedio = db.session.query(
        db.func.avg(db.func.extract('epoch', Contrato.fecha_actualizacion - Contrato.fecha_creacion))
    ).filter(Contrato.estado == 'firmado').scalar()

    return {"tiempo_promedio_segundos": tiempo_promedio}
