# db.py
from database import db
from sqlalchemy.exc import SQLAlchemyError
import os
from sqlalchemy.sql import func

DB_URL = os.getenv("DATABASE_URL")

def get_connection():
    return db

# ---------------------------
# Funciones
# ---------------------------

def insert_contrato(usuario_id, tipo_contrato_id, contenido):
    try:
        contrato = Contrato(
            creador_id=usuario_id,
            tipo_contrato_id=tipo_contrato_id,
            contenido=contenido,
            estado="borrador"
        )
        db.session.add(contrato)
        db.session.commit()
        return contrato.id
    except SQLAlchemyError as e:
        db.session.rollback()
        raise e

def insert_evidencia(contrato_id, tipo_id, metadatos=None, url=None):
    try:
        evidencia = Evidencia(
            contrato_id=contrato_id,
            tipo_id=tipo_id,
            metadatos=metadatos or {},
            url=url
        )
        db.session.add(evidencia)
        db.session.commit()
        return evidencia.id
    except SQLAlchemyError as e:
        db.session.rollback()
        raise e

def update_video_attempt(evidence_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE evidencias SET intentos_video = intentos_video + 1
        WHERE id = %s RETURNING intentos_video;
    """, (evidence_id,))
    attempts = cur.fetchone()[0]
    if attempts >= 2:
        cur.execute("UPDATE evidencias SET estado_link = 'bloqueado' WHERE id = %s;", (evidence_id,))
    conn.commit()
    cur.close()
    conn.close()
    return attempts

def log_conversation(user_id, contract_id, mensaje_usuario, respuesta_sistema, ip):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO conversation_logs (user_id, contract_id, mensaje_usuario, respuesta_sistema, ip_origen)
        VALUES (%s, %s, %s, %s, %s);
    """, (user_id, contract_id, mensaje_usuario, respuesta_sistema, ip))
    conn.commit()
    cur.close()
    conn.close()

def audit_action(entidad, accion, usuario, detalle):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO audit_trail (entidad, accion, usuario_responsable, detalle_cambio)
        VALUES (%s, %s, %s, %s);
    """, (entidad, accion, usuario, detalle))
    conn.commit()
    cur.close()
    conn.close()

def get_evidencia_by_token(token):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, contrato_id, codigo_probatorio, link_expiration, estado_link
        FROM evidencias WHERE link_temporal = %s;
    """, (token,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if row:
        return {
            "id": row[0],
            "contrato_id": row[1],
            "codigo_probatorio": row[2],
            "link_expiration": row[3],
            "estado_link": row[4]
        }
    return None

def get_contrato_by_id(contrato_id):
    return Contrato.query.get(contrato_id)

def get_usuario_by_id(usuario_id):
    return Usuario.query.get(usuario_id)

def update_evidencia_video(evidencia_id, video_path):
    try:
        evidencia = Evidencia.query.get(evidencia_id)
        if evidencia:
            evidencia.url = video_path
            db.session.commit()
    except SQLAlchemyError as e:
        db.session.rollback()
        raise e

def update_evidencia_signed(evidencia_id, hash_final, blockchain_hash, tsa_timestamp):
    try:
        evidencia = Evidencia.query.get(evidencia_id)
        if evidencia:
            evidencia.metadatos.update({
                "hash_documento": hash_final,
                "blockchain_hash": blockchain_hash,
                "tsa_timestamp": tsa_timestamp
            })
            evidencia.estado_link = "firmado"
            db.session.commit()
    except SQLAlchemyError as e:
        db.session.rollback()
        raise e


# ---------------------------
# KPIs
# ---------------------------
def get_kpi_tiempo_promedio():
    try:
        result = db.session.query(
            func.avg(func.extract('epoch', Contrato.fecha_actualizacion - Contrato.fecha_creacion)).label("tiempo_promedio_segundos")
        ).filter(Contrato.estado == 'firmado').one()
        return result.tiempo_promedio_segundos
    except SQLAlchemyError as e:
        raise e

# ---------------------------
# CRAWLER
# ---------------------------

def insert_normativa(titulo, texto, url):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO normativa_index (titulo, texto, url)
        VALUES (%s, %s, %s);
    """, (titulo, texto, url))
    conn.commit()
    cur.close()
    conn.close()

def search_normativa_pg(query):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT titulo, texto, url
        FROM normativa_index
        WHERE texto ILIKE %s
        LIMIT 3;
    """, (f"%{query}%",))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [{"titulo": r[0], "texto": r[1], "url": r[2]} for r in rows]