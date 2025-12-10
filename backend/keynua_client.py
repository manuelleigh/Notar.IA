import requests
import os
from models import Contrato, Evidencia, db

KEYNUA_API = os.getenv("KEYNUA_API", "https://api.keynua.com")
KEYNUA_TOKEN = os.getenv("KEYNUA_TOKEN", "your_api_token")

def send_to_keynua(pdf_path: str, signer_data: dict) -> dict:
    headers = {"Authorization": f"Bearer {KEYNUA_TOKEN}"}
    with open(pdf_path, "rb") as file:
        files = {"file": file}
        payload = {"signer": signer_data}
        r = requests.post(f"{KEYNUA_API}/sign", headers=headers, files=files, data=payload)
        if r.status_code == 200:
            response = r.json()
            # Registrar evidencia de envío
            evidencia = Evidencia(
                contrato_id=signer_data.get("contrato_id"),
                tipo_id=1,  # Asumimos que 1 corresponde a "Envío a Keynua"
                metadatos=response
            )
            db.session.add(evidencia)
            db.session.commit()
            return response
        else:
            r.raise_for_status()

def handle_webhook(data: dict):
    transaction_id = data.get("transaction_id")
    status = data.get("status")
    contrato = Contrato.query.filter_by(codigo=transaction_id).first()
    if contrato:
        contrato.estado = status
        db.session.commit()
        return {"ok": True}
    return {"error": "Contrato no encontrado"}