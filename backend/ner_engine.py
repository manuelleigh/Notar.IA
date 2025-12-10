# Motor NER para IA Notarial + normalización/validación de entidades.
# - Carga del modelo model-best.
# - Extracción estructurada por etiqueta.
# - Normalización y validaciones.
# - Mapeo a "slots" esperados por contrato.
# - Trazabilidad/Auditoría.

from __future__ import annotations
import os
import re
import json
import datetime as dt
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple, Any

import spacy

MODEL_PATH = os.getenv("NER_MODEL_PATH", "./output/model-best")

# -----------------------------------------------------------------------------
# CARGA DEL MODELO
# -----------------------------------------------------------------------------
_nlp = None

def load_model() -> spacy.language.Language:
    global _nlp
    if _nlp is None:
        _nlp = spacy.load(MODEL_PATH)
        _nlp.max_length = max(_nlp.max_length, 2_000_000)
    return _nlp

# -----------------------------------------------------------------------------
# ESTRUCTURAS DE DATOS
# -----------------------------------------------------------------------------
@dataclass
class EntitySpan:
    text: str
    label: str
    start: int
    end: int

@dataclass
class NormalizedEntity:
    label: str
    original: str
    value: Any
    valid: bool
    notes: Optional[str] = None

@dataclass
class ExtractionResult:
    text: str
    entities: List[EntitySpan]                 
    by_label: Dict[str, List[EntitySpan]]
    normalized: List[NormalizedEntity]
    missing_expected: List[str]

# -----------------------------------------------------------------------------
# NORMALIZADORES Y VALIDACIONES
# -----------------------------------------------------------------------------
_DNI_RE = re.compile(r"^\d{8}$")
_RUC_RE = re.compile(r"^\d{11}$")
_CCI_RE = re.compile(r"^\d{20}$")
_CURRENCY_SYMBOLS = {"S/": "PEN", "S/.": "PEN", "USD": "USD", "US$": "USD"}

def normalize_dni(text: str) -> NormalizedEntity:
    digits = re.sub(r"\D", "", text)
    valid = bool(_DNI_RE.match(digits))
    notes = None if valid else "DNI debe tener 8 dígitos."
    return NormalizedEntity(label="DNI", original=text, value=digits if valid else digits, valid=valid, notes=notes)

def normalize_ruc(text: str) -> NormalizedEntity:
    digits = re.sub(r"\D", "", text)
    valid = bool(_RUC_RE.match(digits)) and digits[:2] in {"10", "20", "17"}
    notes = None if valid else "RUC inválido (11 dígitos, prefijo usual 10/20/17)."
    return NormalizedEntity(label="RUC", original=text, value=digits, valid=valid, notes=notes)

def normalize_cci(text: str) -> NormalizedEntity:
    digits = re.sub(r"\D", "", text)
    valid = bool(_CCI_RE.match(digits))
    notes = None if valid else "CCI debe tener 20 dígitos."
    return NormalizedEntity(label="CCI", original=text, value=digits, valid=valid, notes=notes)

def normalize_cuenta(text: str) -> NormalizedEntity:
    digits = re.sub(r"\D", "", text)
    valid = len(digits) >= 12
    notes = None if valid else "Cuenta bancaria demasiado corta."
    return NormalizedEntity(label="CUENTA_BANCARIA", original=text, value={"digits": digits, "raw": text}, valid=valid, notes=notes)

def _detect_currency(s: str) -> Optional[str]:
    for sym, code in _CURRENCY_SYMBOLS.items():
        if sym in s:
            return code
    # Si contiene "soles" o "dólares"
    if re.search(r"\bsol(es)?\b", s.lower()):
        return "PEN"
    if re.search(r"\bdólar(es)?\b", s.lower()):
        return "USD"
    return None

def _parse_amount_number(s: str) -> Optional[float]:
    cleaned = s.replace(" ", "").replace(",", ".")
    cleaned = re.sub(r"[^\d\.]", "", cleaned)
    if cleaned.count(".") > 1:
        parts = cleaned.split(".")
        cleaned = "".join(parts[:-1]) + "." + parts[-1]
    try:
        return float(cleaned)
    except:
        return None

def normalize_monto(text: str) -> NormalizedEntity:
    currency = _detect_currency(text)
    amount = _parse_amount_number(text)
    valid = amount is not None
    if valid and currency is None:
        currency = "PEN"
    notes = None if valid else "No se pudo parsear el monto."
    formatted = None
    if valid:
        if currency == "PEN":
            formatted = f"S/ {amount:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        elif currency == "USD":
            formatted = f"US$ {amount:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return NormalizedEntity(label="MONTO", original=text, value={"amount": amount, "currency": currency, "formatted": formatted}, valid=valid, notes=notes)

def normalize_fecha(text: str) -> NormalizedEntity:
    # Validación muy básica:
    valid = bool(re.search(r"\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{1,2}\s+de\s+\w+\s+de\s+\d{4})\b", text.lower()))
    notes = None if valid else "Formato de fecha no reconocido; se conservará en texto."
    return NormalizedEntity(label="FECHA", original=text, value={"text": text}, valid=valid, notes=notes)

def normalize_direccion(text: str) -> NormalizedEntity:    
    clean = re.sub(r"\s+", " ", text).strip()
    valid = len(clean) >= 8
    notes = None if valid else "Dirección demasiado corta."
    return NormalizedEntity(label="DIRECCION", original=text, value={"text": clean}, valid=valid, notes=notes)

def normalize_persona(text: str) -> NormalizedEntity:
    clean = re.sub(r"\s+", " ", text).strip()
    valid = len(clean.split()) >= 2
    notes = None if valid else "Nombre incompleto."
    return NormalizedEntity(label="PERSONA", original=text, value={"text": clean}, valid=valid, notes=notes)

def normalize_empresa(text: str) -> NormalizedEntity:
    clean = re.sub(r"\s+", " ", text).strip()
    valid = len(clean) >= 3
    notes = None if valid else "Razón social muy corta."
    return NormalizedEntity(label="EMPRESA", original=text, value={"text": clean}, valid=valid, notes=notes)

def normalize_inmueble(text: str) -> NormalizedEntity:
    clean = re.sub(r"\s+", " ", text).strip().lower()
    valid = clean in {"departamento", "casa", "local comercial", "oficina", "almacén", "almacen"}
    notes = None if valid else "Tipo de inmueble poco claro."
    return NormalizedEntity(label="INMUEBLE", original=text, value={"text": text}, valid=valid, notes=notes)

def normalize_plazo(text: str) -> NormalizedEntity:
    clean = re.sub(r"\s+", " ", text).strip()
    valid = bool(re.search(r"\b(\d+\s*(días|meses|años)|\d{1,2}\s*mes(es)?)\b", clean.lower()))
    notes = None if valid else "Plazo no reconocido; se conservará en texto."
    return NormalizedEntity(label="PLAZO", original=text, value={"text": clean}, valid=valid, notes=notes)

def normalize_interes(text: str) -> NormalizedEntity:
    m = re.search(r"(\d{1,3}(?:[.,]\d{1,2})?)\s*%", text)
    valid = m is not None
    value = float(m.group(1).replace(",", ".")) if valid else None
    notes = None if valid else "Interés no reconocido."
    return NormalizedEntity(label="INTERES", original=text, value={"percent": value}, valid=valid, notes=notes)

def normalize_penalidad(text: str) -> NormalizedEntity:
    if "%" in text:
        ni = normalize_interes(text)
        ni.label = "PENALIDAD"
        return ni
    else:
        nm = normalize_monto(text)
        nm.label = "PENALIDAD"
        return nm

def normalize_ciudad(text: str) -> NormalizedEntity:
    clean = re.sub(r"\s+", " ", text).strip()
    valid = len(clean) >= 3
    return NormalizedEntity(label="CIUDAD", original=text, value={"text": clean}, valid=valid, notes=None if valid else "Ciudad muy corta.")

def normalize_banco(text: str) -> NormalizedEntity:
    clean = re.sub(r"\s+", " ", text).strip()
    valid = clean in {"BCP", "BBVA", "Interbank", "Scotiabank"}
    return NormalizedEntity(label="BANCO", original=text, value={"name": clean}, valid=valid, notes=None if valid else "Banco no reconocido.")

# -----------------------------------------------------------------------------
# DISPATCH DE NORMALIZACIÓN SEGÚN ETIQUETA
# -----------------------------------------------------------------------------
_NORMALIZERS = {
    "DNI": normalize_dni,
    "RUC": normalize_ruc,
    "CCI": normalize_cci,
    "CUENTA_BANCARIA": normalize_cuenta,
    "MONTO": normalize_monto,
    "FECHA": normalize_fecha,
    "DIRECCION": normalize_direccion,
    "PERSONA": normalize_persona,
    "EMPRESA": normalize_empresa,
    "INMUEBLE": normalize_inmueble,
    "PLAZO": normalize_plazo,
    "INTERES": normalize_interes,
    "PENALIDAD": normalize_penalidad,
    "CIUDAD": normalize_ciudad,
    "BANCO": normalize_banco,
}

# -----------------------------------------------------------------------------
# EXTRACCIÓN PRINCIPAL
# -----------------------------------------------------------------------------
def extract(text: str, expected_labels: Optional[List[str]] = None) -> ExtractionResult:
    nlp = load_model()
    doc = nlp(text)

    entities = [EntitySpan(e.text, e.label_, e.start_char, e.end_char) for e in doc.ents]
    by_label: Dict[str, List[EntitySpan]] = {}
    for ent in entities:
        by_label.setdefault(ent.label, []).append(ent)

    normalized: List[NormalizedEntity] = []
    for ent in entities:
        norm_fn = _NORMALIZERS.get(ent.label)
        if norm_fn:
            normalized.append(norm_fn(ent.text))
        else:
            normalized.append(NormalizedEntity(label=ent.label, original=ent.text, value={"text": ent.text}, valid=True))

    missing_expected: List[str] = []
    if expected_labels:
        for lab in expected_labels:
            if lab not in by_label:
                missing_expected.append(lab)

    return ExtractionResult(
        text=text,
        entities=entities,
        by_label=by_label,
        normalized=normalized,
        missing_expected=missing_expected
    )

# -----------------------------------------------------------------------------
# MAPE0 BÁSICO A SLOTS
# -----------------------------------------------------------------------------

TIPO_DATO_TO_EXPECTED = {
    "persona_dni": ["PERSONA", "DNI"],
    "persona_empresa": ["PERSONA", "EMPRESA", "DNI", "RUC"],
    "inmueble": ["INMUEBLE", "DIRECCION", "DISTRITO", "PROVINCIA", "DEPARTAMENTO"],  
    "monto_simple": ["MONTO"],
    "monto_condiciones": ["MONTO", "BANCO", "CUENTA_BANCARIA"],
    "plazo": ["PLAZO"],
    "interes": ["INTERES"],
    "lugar_fecha": ["CIUDAD", "FECHA"],
    "arrendador": ["PERSONA", "DNI", "DIRECCION"],
    "arrendatario": ["PERSONA", "DNI", "DIRECCION"],
}

def fill_slots_from_text(contract_type: str, tipo_dato: str, text: str, current_slots: Dict[str, str]) -> Dict[str, str]:
    expected = TIPO_DATO_TO_EXPECTED.get(tipo_dato, [])
    result = extract(text, expected_labels=expected)

    out = dict(current_slots) if current_slots else {}
    for lab in expected:
        spans = result.by_label.get(lab, [])
        if spans:
            value_text = spans[0].text
            norm = _NORMALIZERS.get(lab)
            if norm:
                nval = norm(value_text)
                if isinstance(nval.value, dict) and "formatted" in nval.value and nval.value["formatted"]:
                    out[f"{tipo_dato}.{lab.lower()}"] = nval.value["formatted"]
                elif isinstance(nval.value, dict) and "text" in nval.value:
                    out[f"{tipo_dato}.{lab.lower()}"] = nval.value["text"]
                else:
                    out[f"{tipo_dato}.{lab.lower()}"] = nval.value if nval.value is not None else value_text
            else:
                out[f"{tipo_dato}.{lab.lower()}"] = value_text
    return out

# -----------------------------------------------------------------------------
# MENSAJES DE COMPRENSIÓN
# -----------------------------------------------------------------------------
def missing_message(expected_labels: List[str], result: ExtractionResult) -> Optional[str]:
    if not expected_labels:
        return None
    missing = [lab for lab in expected_labels if lab not in result.by_label]
    if not missing:
        return None

    templates = {
        "DNI": "Creo que no encontré el DNI en tu respuesta. ¿Podrías confirmarlo?",
        "RUC": "No pude detectar el RUC. ¿Me lo indicas por favor?",
        "DIRECCION": "Me falta la dirección completa. ¿Podrías compartirla?",
        "MONTO": "No identifiqué el monto. ¿Cuál es el importe?",
        "PLAZO": "No encontré el plazo. ¿Podrías indicarlo?",
        "FECHA": "Falta la fecha. ¿Cuál sería?",
        "CIUDAD": "No detecté la ciudad de firma. ¿Me la indicas?",
        "CUENTA_BANCARIA": "No pude leer la cuenta bancaria. ¿Puedes escribirla nuevamente?",
        "INTERES": "No encontré el porcentaje de interés. ¿Cuál es?",
        "PENALIDAD": "No detecté la penalidad. ¿Deseas agregarla?"
    }
    for lab in missing:
        if lab in templates:
            return templates[lab]
    return "Creo que me falta un dato en tu respuesta. ¿Podrías detallarlo por favor?"

# -----------------------------------------------------------------------------
# AUDITORÍA / TRAZABILIDAD
# -----------------------------------------------------------------------------
def audit_log(user_id: str, action: str, payload: Dict[str, Any], db=None) -> None:
    record = {
        "user_id": user_id,
        "action": action,
        "payload": payload,
        "timestamp": dt.datetime.utcnow().isoformat() + "Z"
    }
    if db is not None:
        try:
            db.insert_conversation_log(record)
        except Exception as e:
            print(f"[AUDIT][ERROR] {e}")
    else:
        # Fallback: imprime en consola o guarda en archivo local
        print("[AUDIT]", json.dumps(record, ensure_ascii=False))

# -----------------------------------------------------------------------------
# USO BÁSICO
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    sample = "El señor Juan Pérez, DNI 12345678, con domicilio en Jr. Los Pinos 123, Miraflores. El monto total es S/ 1,500.00."
    res = extract(sample, expected_labels=["PERSONA", "DNI", "DIRECCION", "MONTO"])
    print("Entidades:", [asdict(e) for e in res.entities])
    print("Faltantes:", res.missing_expected)
    for ne in res.normalized:
        print(asdict(ne))
    msg = missing_message(["PERSONA", "DNI", "DIRECCION", "MONTO"], res)
    print("Mensaje:", msg)