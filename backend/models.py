from datetime import datetime, timedelta
import uuid
from database import db
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.dialects.postgresql import JSONB
from werkzeug.security import generate_password_hash, check_password_hash

# ---------------------------
# ROLES
# ---------------------------
class Rol(db.Model):
    __tablename__ = "roles"

    id = db.Column(db.Integer, primary_key=True)
    nombre = db.Column(db.String(50), nullable=False, unique=True)

    usuarios = db.relationship("Usuario", backref="rol", lazy="dynamic")

    def __repr__(self):
        return f"<Rol {self.nombre}>"

# ---------------------------
# TIPO DOCUMENTO
# ---------------------------
class TipoDocumento(db.Model):
    __tablename__ = "tipo_documento"

    id = db.Column(db.Integer, primary_key=True)
    descripcion = db.Column(db.String(100), nullable=False, unique=True)

    def __repr__(self):
        return f"<TipoDocumento {self.descripcion}>"

# ---------------------------
# USUARIOS
# ---------------------------
class Usuario(db.Model):
    __tablename__ = "usuarios"

    id = db.Column(db.Integer, primary_key=True)
    nombre = db.Column(db.String(120), nullable=False)
    correo = db.Column(db.String(120), unique=True, nullable=False)
    contrasena_hash = db.Column(db.String(255), nullable=False)
    numero_documento = db.Column(db.String(50), nullable=False)
    
    tipo_documento_id = db.Column(db.Integer, db.ForeignKey("tipo_documento.id"), nullable=True)
    rol_id = db.Column(db.Integer, db.ForeignKey("roles.id"), nullable=False)
    
    api_key = db.Column(db.String(255), unique=True, nullable=True)

    # Campos para reseteo de contrasena
    reset_token = db.Column(db.String(120), unique=True, nullable=True)
    reset_token_expiration = db.Column(db.DateTime, nullable=True)

    fecha_creacion = db.Column(db.DateTime, default=datetime.utcnow)
    fecha_actualizacion = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relaciones
    chats = db.relationship("Chat", backref="usuario", lazy="dynamic")
    contratos = db.relationship("Contrato", backref="creador", lazy="dynamic")
    mensajes = db.relationship("Mensaje", backref="usuario", lazy="dynamic")

    # Métodos de seguridad
    def set_password(self, password: str):
        self.contrasena_hash = generate_password_hash(password)

    def check_password(self, password: str) -> bool:
        return check_password_hash(self.contrasena_hash, password)
    
    def generate_api_key(self):
        self.api_key = str(uuid.uuid4())

    def generate_reset_token(self):
        self.reset_token = str(uuid.uuid4())
        self.reset_token_expiration = datetime.utcnow() + timedelta(hours=1)

    def __repr__(self):
        return f"<Usuario {self.correo}>"

# ---------------------------
# CHATS
# ---------------------------
class Chat(db.Model):
    __tablename__ = "chats"

    id = db.Column(db.Integer, primary_key=True)
    nombre = db.Column(db.String(150), nullable=True)
    usuario_id = db.Column(db.Integer, db.ForeignKey("usuarios.id"), nullable=False)
    estado = db.Column(db.String(20), default="activo")  # activo, completado, cancelado, archivado
    metadatos = db.Column(MutableDict.as_mutable(JSONB), default=dict)

    fecha_creacion = db.Column(db.DateTime, default=datetime.utcnow)
    fecha_actualizacion = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relaciones
    mensajes = db.relationship("Mensaje", backref="chat", lazy="dynamic")
    contratos = db.relationship("Contrato", backref="chat", lazy="dynamic")

# ---------------------------
# MENSAJES
# ---------------------------
class Mensaje(db.Model):
    __tablename__ = "mensajes"

    id = db.Column(db.Integer, primary_key=True)
    chat_id = db.Column(db.Integer, db.ForeignKey("chats.id"), nullable=False)
    contrato_id = db.Column(db.Integer, db.ForeignKey("contratos.id"), nullable=True)
    usuario_id = db.Column(db.Integer, db.ForeignKey("usuarios.id"), nullable=True)
    remitente = db.Column(db.String(20), nullable=False)
    contenido = db.Column(db.Text, nullable=False)
    metadatos = db.Column(MutableDict.as_mutable(JSONB), default=dict)

    fecha_creacion = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<Mensaje {self.id} chat={self.chat_id} remitente={self.remitente}>"

# ---------------------------
# TIPO CONTRATO
# ---------------------------
class TipoContrato(db.Model):
    __tablename__ = "tipo_contrato"

    id = db.Column(db.Integer, primary_key=True)
    descripcion = db.Column(db.String(100), nullable=False, unique=True)
    plantilla = db.Column(db.String(150), nullable=False)

    def __repr__(self):
        return f"<TipoContrato {self.descripcion}>"

# ---------------------------
# CONTRATOS
# ---------------------------
class Contrato(db.Model):
    __tablename__ = "contratos"

    id = db.Column(db.Integer, primary_key=True)
    codigo = db.Column(db.String(50), nullable=False, unique=True)  # ej. CONT-2025-0001
    titulo = db.Column(db.String(255), nullable=False)
    descripcion = db.Column(db.Text, nullable=True)

    creador_id = db.Column(db.Integer, db.ForeignKey("usuarios.id"), nullable=False)
    chat_id = db.Column(db.Integer, db.ForeignKey("chats.id"), nullable=False)
    tipo_contrato_id = db.Column(db.Integer, db.ForeignKey("tipo_contrato.id"), nullable=False)

    estado = db.Column(db.String(30), default="borrador")  # borrador, en_proceso, enviado_keynua, firmado, entregado, cancelado

    contenido = db.Column(MutableDict.as_mutable(JSONB), default=dict)  # datos estructurados del contrato
    archivo_original_url = db.Column(db.Text, nullable=True)
    archivo_firmado_url = db.Column(db.Text, nullable=True)  # proporcionado por Keynua cuando esté firmado

    metadatos = db.Column(MutableDict.as_mutable(JSONB), default=dict)

    fecha_creacion = db.Column(db.DateTime, default=datetime.utcnow)
    fecha_firma = db.Column(db.DateTime, nullable=True)
    fecha_actualizacion = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relaciones
    firmantes = db.relationship("Firmante", backref="contrato", lazy="dynamic")
    evidencias = db.relationship("Evidencia", backref="contrato", lazy="dynamic")

    def __repr__(self):
        return f"<Contrato {self.codigo} - {self.titulo}>"

# ---------------------------
# FIRMANTES
# ---------------------------
class Firmante(db.Model):
    __tablename__ = "firmantes"

    id = db.Column(db.Integer, primary_key=True)
    contrato_id = db.Column(db.Integer, db.ForeignKey("contratos.id"), nullable=False)
    nombre = db.Column(db.String(150), nullable=False)
    correo = db.Column(db.String(120), nullable=True)
    telefono = db.Column(db.String(30), nullable=True)
    tipo_documento_id = db.Column(db.Integer, db.ForeignKey("tipo_documento.id"), nullable=True)
    rol_firmante_id = db.Column(db.Integer, db.ForeignKey("roles.id"), nullable=False)

    # Acceso temporal / OTP
    token_acceso = db.Column(db.String(120), unique=True, index=True, nullable=True)  # link temporal para subir video
    token_expira = db.Column(db.DateTime, nullable=True) # fecha de expiración del token
    otp = db.Column(db.String(10), nullable=True) # código OTP enviado por SMS o email
    otp_intentos = db.Column(db.Integer, default=0) # intentos realizados
    otp_max_intentos = db.Column(db.Integer, default=3) # máximo de intentos permitidos

    estado = db.Column(db.String(30), default="INVITADO")  # INVITADO, VALIDADO_KEYNUA, VIDEO_PENDIENTE, VIDEO_SUBIDO, VIDEO_VALIDADO, VIDEO_RECHAZADO
    intentos_video = db.Column(db.Integer, default=0) # número de intentos de subir video
    max_intentos_video = db.Column(db.Integer, default=2) # máximo de intentos permitidos para subir video

    fecha_invitacion = db.Column(db.DateTime, default=datetime.utcnow)
    fecha_ultima_actualizacion = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    fecha_video_subido = db.Column(db.DateTime, nullable=True)
    fecha_video_validado = db.Column(db.DateTime, nullable=True)

    metadatos = db.Column(MutableDict.as_mutable(JSONB), default=dict)  # codigo_aleatorio, reglas, etc.

    # Relaciones
    evidencias = db.relationship("Evidencia", backref="firmante", lazy="dynamic")

    def generar_token_acceso(self, minutos_validos: int = 60):
        self.token_acceso = str(uuid.uuid4())
        self.token_expira = datetime.utcnow() + timedelta(minutes=minutos_validos)

    def generar_otp(self):
        import random
        self.otp = f'{random.randint(100000, 999999)}'
        self.otp_intentos = 0

    def __repr__(self):
        return f"<Firmante {self.nombre} contrato={self.contrato_id}>"

# ---------------------------
# TIPOS DE EVIDENCIAS
# ---------------------------
class TipoEvidencia(db.Model):
    __tablename__ = "tipo_evidencia"

    id = db.Column(db.Integer, primary_key=True)
    descripcion = db.Column(db.String(100), nullable=False, unique=True)

    def __repr__(self):
        return f"<TipoEvidencia {self.descripcion}>"

# ---------------------------
# EVIDENCIAS (videos, hash, tsa, blockchain, logs)
# ---------------------------
class Evidencia(db.Model):
    __tablename__ = "evidencias"

    id = db.Column(db.Integer, primary_key=True)
    contrato_id = db.Column(db.Integer, db.ForeignKey("contratos.id"), nullable=False)
    firmante_id = db.Column(db.Integer, db.ForeignKey("firmantes.id"), nullable=True)
    tipo_id = db.Column(db.Integer, db.ForeignKey("tipo_evidencia.id"), nullable=False)
    
    url = db.Column(db.Text, nullable=True)  # para archivos (S3 o alternativa)
    metadatos = db.Column(MutableDict.as_mutable(JSONB), default=dict)  # duración, codigo_aleatorio, ip, user_agent, etc.

    fecha_creacion = db.Column(db.DateTime, default=datetime.utcnow)
    
    tipo = db.relationship("TipoEvidencia", backref="evidencias")

    def __repr__(self):
        return f"<Evidencia {self.tipo} contrato={self.contrato_id}>"
