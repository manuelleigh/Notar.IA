import random
from typing import Dict, Optional
from ner_engine import fill_slots_from_text, missing_message, audit_log
from contracts_data import CONTRACTS
from models import Usuario, Contrato, db

QUESTION_VARIANTS = {
    "default": [
        "¿Podrías darme este dato, por favor?",
        "Por favor, indícame la información solicitada.",
        "¿Me ayudas con este detalle?"
    ]
}

def cordialize(text: str) -> str:
    replacements = {
        "Indique": "Por favor, indícame",
        "Especifique": "¿Podrías darme",
        "Defina": "Por favor, señala"
    }
    for k, v in replacements.items():
        text = text.replace(k, v)
    return text

def get_next_question(contract_type: str, filled_slots: Dict[str, str]) -> Optional[str]:
    schema = CONTRACTS[contract_type]["preguntas"]
    for slot in schema:
        if slot["key"] not in filled_slots or not filled_slots[slot["key"]]:
            return cordialize(slot["texto"])
    return None

def process_message(user_id: int, contract_type: str, current_slots: Dict[str, str], message: str):
    user = Usuario.query.get(user_id)
    if not user:
        raise ValueError("Usuario no encontrado")

    schema = CONTRACTS[contract_type]["preguntas"]
    next_slot = None
    for slot in schema:
        if slot["key"] not in current_slots or not current_slots[slot["key"]]:
            next_slot = slot
            break

    if not next_slot:
        return {"status": "complete", "filled": current_slots}

    new_slots = fill_slots_from_text(contract_type, next_slot["tipo_dato"], message, current_slots)

    # Registrar auditoría
    audit_entry = {
        "usuario_id": user.id,
        "accion": "slot_fill",
        "detalle": {"message": message, "new_slots": new_slots}
    }
    db.session.add(audit_entry)
    db.session.commit()

    next_q = get_next_question(contract_type, new_slots)
    if next_q:
        return {"status": "incomplete", "ask": next_q, "filled": new_slots}

    return {"status": "preview", "filled": new_slots}