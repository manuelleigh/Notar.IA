# generate_synthetic.py
# ============================================================
# Generador de dataset sintético para NER (spaCy v3)
# Contexto: contratos (Perú)
#
# Características:
#  - Variedad de formatos por entidad (montos, fechas, direcciones, etc.).
#  - Inyección de ruido controlado (typos, espacios, puntuación).
#  - Ejemplos negativos.
#  - Balance básico por tipo de ejemplo.
#  - Guardado en .spacy (DocBin) y JSONL.
#  - Estadísticas del dataset.
#
# Uso:
#   python generate_synthetic.py \
#       --train 5000 --dev 1000 --test 1000 \
#       --outdir training_data --seed 42 --noise 0.15 --jsonl
#
# Salidas:
#   training_data/train.spacy
#   training_data/dev.spacy
#   training_data/test.spacy
#   training_data/preview.jsonl
# ============================================================

from __future__ import annotations
import os
import random
import json
import argparse
from pathlib import Path
from typing import List, Tuple, Dict, Any

import spacy
from spacy.tokens import DocBin, Doc
from spacy.util import filter_spans


# -----------------------------
# Configuración y datos base
# -----------------------------

NOMBRES_MASC = [
    "Juan", "José", "Luis", "Carlos", "Jorge", "Manuel", "Miguel", "Víctor",
    "Mario", "Roberto", "David", "Daniel", "Andrés", "Alberto", "Pedro",
    "Ricardo", "Alfredo", "Fernando", "Héctor", "Renato", "Julián", "Ernesto",
    "Santiago", "César", "Hernán", "Ramón", "Raúl", "Omar", "Iván"
]

NOMBRES_FEM = [
    "María", "Ana", "Carmen", "Patricia", "Rosa", "Luisa", "Claudia", "Paola",
    "Sofía", "Valeria", "Gabriela", "Raquel", "Flor", "Daniela", "Tatiana",
    "Mónica", "Teresa", "Pilar", "Susana", "Diana", "Vanessa"
]

NOMBRES_COMPUESTOS = [
    "Juan Carlos", "José Luis", "Luis Alberto", "María José", "Ana María",
    "José Carlos", "María Elena", "José Antonio", "Carlos Alberto",
    "María Fernanda", "Luis Miguel"
]

APELLIDOS = [
    "Pérez", "García", "Ramírez", "Flores", "Martínez", "Rodríguez", "Sánchez",
    "Torres", "Díaz", "Vásquez", "Castro", "Ruiz", "Rojas", "Morales",
    "Fernández", "Jiménez", "Gutiérrez", "Chávez", "Salazar", "Aguilar",
    "Herrera", "Mendoza", "Romero", "Cárdenas", "Reyes", "Paredes", "Campos",
    "Lozano", "Escobar", "Palacios", "Valverde", "Velásquez", "Montoya",
    "Cabrera", "Ortega", "Cortez", "Valencia", "Peña", "Bravo", "Serrano",
    "Acosta", "Navarro", "Ortiz", "Vega", "Huamán", "León", "Meza", "Núñez",
    "Vargas", "López", "Vilca", "Mamani", "Quispe", "Ticona", "Condori",
    "Ancajima", "Alvarado", "Soto", "Tello"
]

DEPARTAMENTOS = [
    'Amazonas', 'Apurímac', 'Arequipa', 'Ayacucho', 'Áncash', 'Cajamarca',
    'Callao', 'Cusco', 'Huánuco', 'Huancavelica', 'Ica', 'Junín', 'La Libertad',
    'Lambayeque', 'Lima', 'Loreto', 'Madre de Dios', 'Moquegua', 'Pasco',
    'Piura', 'Puno', 'San Martín', 'Tacna', 'Tumbes', 'Ucayali'
]

PROVINCIAS = ["Lima", "Arequipa", "Cusco", "Trujillo", "Piura", "Chiclayo", "Huancayo", "Iquitos", "Tacna"]

DISTRITOS = [
    "Miraflores", "San Isidro", "Santiago de Surco", "Barranco", "San Borja",
    "Magdalena del Mar", "Jesús María", "Los Olivos", "Comas",
    "San Juan de Lurigancho", "San Juan de Miraflores", "Lince", "La Molina",
    "Breña", "Chorrillos", "Pueblo Libre", "Rímac", "Ate", "El Agustino",
    "Villa El Salvador", "Villa María del Triunfo", "Cercado de Lima",
    "Independencia"
]

CIUDADES = sorted({*DEPARTAMENTOS, *PROVINCIAS, *DISTRITOS})

TIPOS_VIA = ["Av.", "Avenida", "Jr.", "Jirón", "Calle", "Psje.", "Pasaje", "Mz.", "Manzana", "Malecón", "Alameda"]
CALLES = [
    "Arequipa", "Brasil", "Javier Prado", "Angamos", "La Marina", "Petit Thouars",
    "Los Cedros", "Los Álamos", "Los Sauces", "Las Gardenias", "Los Rosales",
    "Los Pinos", "Las Orquídeas", "Panamericana Sur", "Panamericana Norte"
]

TIPOS_EMPRESA = ["S.A.", "S.A.C.", "S.R.L.", "S.A.A.", "E.I.R.L.", "S.C.R.L.", "S.C."]

NOMBRES_COMERCIALES = [
    "Inversiones", "Servicios", "Consultores", "Corporación", "Grupo", "Constructora",
    "Tecnologías", "Transportes", "Importaciones", "Exportaciones", "Desarrollos",
    "Proyectos", "Logística", "Soluciones", "Representaciones", "Distribuciones"
]

NOMBRES_FANTASIA = [
    "Leigh", "Andes", "Pacífico", "Lima Norte", "Alpha", "Omega", "Perú Global",
    "Norte Industrial", "Sur Export", "Costa Verde", "Pachacamac", "Sierra Azul",
    "Amazonía"
]

MONEDAS_SIMBOLO = ["S/", "S/.", "US$", "USD"]

MONEDAS_TEXTO = ["soles", "dólares"]

MONTOS_BASE = [150, 200, 300, 350, 500, 750, 800, 1000, 1200, 1500, 1800, 2000, 2500, 3000, 3250, 3750, 4200, 4750, 5200, 5600, 6300, 6800, 7400, 7900, 8100, 8500, 9300, 9600, 10400, 10700]

PLAZOS = [
    "30 días", "45 días", "60 días", "90 días", "120 días",
    "3 meses", "4 meses", "5 meses", "6 meses", "12 meses",
    "24 meses", "36 meses", "48 meses", "60 meses",
    "1 año", "2 años", "3 años", "5 años"
]

INTERESES = ["1%", "2%", "3%", "5%", "8%", "10%", "12%", "15%", "20%"]

PENALIDADES = ["S/ 100.00", "S/ 200.00", "S/ 350.00", "S/ 500.00",
               "10% del monto total", "20% del monto total", "1% diario", "0.5% mensual"]
TIPOS_INMUEBLE = ["departamento", "casa", "local comercial", "oficina", "almacén"]

UNIDADES = [
    "", "uno", "dos", "tres", "cuatro", "cinco", "seis", "siete", "ocho", "nueve"
]

DECENAS = [
    "", "diez", "veinte", "treinta", "cuarenta", "cincuenta",
    "sesenta", "setenta", "ochenta", "noventa"
]

ESPECIALES_11_19 = {
    11: "once", 12: "doce", 13: "trece", 14: "catorce", 15: "quince",
    16: "dieciséis", 17: "diecisiete", 18: "dieciocho", 19: "diecinueve"
}

CENTENAS = [
    "", "cien", "doscientos", "trescientos", "cuatrocientos", "quinientos",
    "seiscientos", "setecientos", "ochocientos", "novecientos"
]


# -----------------------------
# Utilidades y ruido controlado
# -----------------------------

def set_seed(seed: int | None):
    if seed is not None:
        random.seed(seed)


def random_name() -> str:
    base = random.choice([NOMBRES_MASC, NOMBRES_FEM])
    nombre = random.choice(base)
    if random.random() < 0.18:
        nombre = random.choice(NOMBRES_COMPUESTOS)
    return f"{nombre} {random.choice(APELLIDOS)} {random.choice(APELLIDOS)}"


def random_dni() -> str:
    return "".join(str(random.randint(0, 9)) for _ in range(8))


def random_ruc() -> str:
    # En Perú: 10/20/17/etc. Para empresas: común "20"
    pref = random.choice(["10", "20", "17"])
    return pref + "".join(str(random.randint(0, 9)) for _ in range(9))


def random_account_number(banco: str) -> str:
    # formatos simples y con separadores
    if banco == "BCP":
        length = random.choice([13, 14])
        base = "".join(str(random.randint(0, 9)) for _ in range(length))
        return f"{base[:3]}-{base[3:11]}-{base[11:12]}-{base[12:]}" if random.random() < 0.5 else base
    if banco == "BBVA":
        length = random.choice([14, 15, 16])
        base = "".join(str(random.randint(0, 9)) for _ in range(length))
        return f"{base[:3]}-{base[3:7]}-{base[7:11]}-{base[11:]}" if random.random() < 0.2 else base
    if banco == "Interbank":
        base = "".join(str(random.randint(0, 9)) for _ in range(13))
        return f"{base[:3]} {base[3:6]} {base[6:9]} {base[9:]}" if random.random() < 0.3 else base
    if banco == "Scotiabank":
        base = "".join(str(random.randint(0, 9)) for _ in range(14))
        return f"{base[:3]}-{base[3:]}" if random.random() < 0.4 else base
    return "".join(str(random.randint(0, 9)) for _ in range(14))


def random_cci() -> str:
    return "".join(str(random.randint(0, 9)) for _ in range(20))


def random_via() -> str:
    return f"{random.choice(TIPOS_VIA)} {random.choice(CALLES)}"


def random_direccion_completa() -> str:
    via = random_via()
    numero = random.randint(100, 9999)
    distrito = random.choice(DISTRITOS)
    provincia = random.choice(PROVINCIAS)
    depa = random.choice(DEPARTAMENTOS)
    variantes = [
        f"{via} {numero}, {distrito}, {provincia}, {depa}",
        f"{via} N° {numero}, distrito de {distrito}, provincia de {provincia}, departamento de {depa}",
        f"{via} {numero}, {distrito}, {depa}",
    ]
    return random.choice(variantes)


def nombre_mes(mes: int) -> str:
    meses = [
        "enero", "febrero", "marzo", "abril", "mayo", "junio",
        "julio", "agosto", random.choice(["setiembre", "septiembre"]),
        "octubre", "noviembre", "diciembre"
    ]
    return meses[mes - 1]


def random_fecha_texto() -> str:
    anio = random.randint(1995, 2026)
    mes = random.randint(1, 12)
    dia = random.randint(1, 28) if mes == 2 else random.randint(1, 30 if mes in [4, 6, 9, 11] else 31)
    mtxt = nombre_mes(mes)
    variantes = [
        f"{dia}/{mes}/{anio}",
        f"{dia}-{mes}-{anio}",
        f"{dia}/{mes}/{str(anio)[2:]}",
        f"{dia}-{mes}-{str(anio)[2:]}",
        f"{dia} de {mtxt} de {anio}",
        f"{dia} {mtxt} {anio}",
        f"a los {dia} días del mes de {mtxt} del año {anio}",
        f"en fecha {dia} de {mtxt} de {anio}",
    ]
    return random.choice(variantes)


def format_monto(valor: int) -> str:
    miles = f"{valor:,}".replace(",", ".")
    decimales = random.choice([".00", "", f".{random.randint(10,99)}"])
    sim = random.choice(MONEDAS_SIMBOLO)
    if random.random() < 0.5:
        return f"{sim} {miles}{decimales}"
    else:
        texto = random.choice(MONEDAS_TEXTO)
        return f"{miles}{decimales} {texto}"


def inject_noise(text: str, noise_prob: float = 0.0) -> str:
    """Aplica ruidos simples: dobles espacios, eliminar/añadir comas, cambios de mayúsculas, typos básicos."""
    if noise_prob <= 0:
        return text
    t = text

    def apply(p: float) -> bool:
        return random.random() < p

    # Espacios dobles
    if apply(noise_prob):
        t = t.replace(" ", "  ")

    # Puntuación
    if apply(noise_prob * 0.7):
        t = t.replace(", ", " ").replace(" .", ".")
    if apply(noise_prob * 0.4):
        t = t.replace(".", " .")

    # Mayúsculas/minúsculas
    if apply(noise_prob * 0.5):
        t = t.capitalize()
    if apply(noise_prob * 0.3):
        t = t.lower()

    # Typos: intercambiar dos letras adyacentes en una palabra larga
    if apply(noise_prob * 0.6):
        parts = t.split()
        for i, w in enumerate(parts):
            if len(w) > 6 and w.isalpha():
                idx = random.randint(1, len(w) - 2)
                chars = list(w)
                chars[idx], chars[idx + 1] = chars[idx + 1], chars[idx]
                parts[i] = "".join(chars)
                t = " ".join(parts)
                break

    return t


# -----------------------------
# Generadores etiquetados
# -----------------------------

def span_indexes(text: str, substr: str) -> Tuple[int, int]:
    start = text.index(substr)
    return start, start + len(substr)


def ejemplo_persona_dni() -> Tuple[str, List[Tuple[int, int, str]]]:
    nombre = random_name()
    dni = random_dni()
    direccion = random_direccion_completa()
    frames = [
        f"{nombre}, identificado con DNI N° {dni}, con domicilio en {direccion}.",
        f"El señor {nombre}, con DNI N° {dni}, domiciliado en {direccion}.",
        f"{nombre}, portador del DNI {dni}, con residencia en {direccion}.",
        f"El compareciente {nombre}, identificado con DNI {dni}, con domicilio en {direccion}.",
    ]
    texto = random.choice(frames)
    ents = []
    s, e = span_indexes(texto, nombre); ents.append((s, e, "PERSONA"))
    s, e = span_indexes(texto, dni); ents.append((s, e, "DNI"))
    s, e = span_indexes(texto, direccion); ents.append((s, e, "DIRECCION"))
    return texto, ents


def ejemplo_empresa() -> Tuple[str, List[Tuple[int, int, str]]]:
    emp = random.choice([
        f"{random.choice(NOMBRES_COMERCIALES)} {random.choice(NOMBRES_FANTASIA)} {random.choice(TIPOS_EMPRESA)}",
        f"{random.choice(NOMBRES_FANTASIA)} {random.choice(NOMBRES_COMERCIALES)} {random.choice(TIPOS_EMPRESA)}",
        f"{random.choice(NOMBRES_COMERCIALES)} {random.choice(NOMBRES_FANTASIA)} del Perú {random.choice(TIPOS_EMPRESA)}",
    ])
    ruc = random_ruc()
    dirx = random_direccion_completa()
    frames = [
        f"La empresa {emp}, identificada con RUC N° {ruc}, con domicilio en {dirx}.",
        f"{emp}, con RUC {ruc}, y domicilio legal en {dirx}.",
        f"{emp}, identificada con número de RUC {ruc}, con sede en {dirx}.",
    ]
    texto = random.choice(frames)
    ents = []
    s, e = span_indexes(texto, emp); ents.append((s, e, "EMPRESA"))
    s, e = span_indexes(texto, ruc); ents.append((s, e, "RUC"))
    s, e = span_indexes(texto, dirx); ents.append((s, e, "DIRECCION"))
    return texto, ents


def ejemplo_persona_empresa() -> Tuple[str, List[Tuple[int, int, str]]]:
    persona = random_name()
    emp, ruc = None, None
    emp_text, ents_emp = ejemplo_empresa()
    # Reutilizamos empresa/ruc del texto generado
    for s, e, lbl in ents_emp:
        if lbl == "EMPRESA":
            emp = emp_text[s:e]
        if lbl == "RUC":
            ruc = emp_text[s:e]
    texto = random.choice([
        f"{persona}, en representación de la empresa {emp}, identificada con RUC {ruc}.",
        f"El señor {persona}, actuando en nombre de la empresa {emp}, con RUC {ruc}.",
        f"{persona}, quien representa a la empresa {emp}, registrada con RUC {ruc}.",
    ])
    ents = []
    s, e = span_indexes(texto, persona); ents.append((s, e, "PERSONA"))
    s, e = span_indexes(texto, emp); ents.append((s, e, "EMPRESA"))
    s, e = span_indexes(texto, ruc); ents.append((s, e, "RUC"))
    return texto, ents


def ejemplo_inmueble() -> Tuple[str, List[Tuple[int, int, str]]]:
    tipo = random.choice(TIPOS_INMUEBLE)
    dirx = random_direccion_completa()
    dist = random.choice(DISTRITOS)
    prov = random.choice(PROVINCIAS)
    depa = random.choice(DEPARTAMENTOS)
    frames = [
        f"El {tipo} ubicado en {dirx}, distrito de {dist}, provincia de {prov}, departamento de {depa}.",
        f"Se trata de un {tipo} situado en {dirx}, en el distrito de {dist}, provincia de {prov}, departamento de {depa}.",
    ]
    texto = random.choice(frames)
    ents = []
    s, e = span_indexes(texto, tipo); ents.append((s, e, "INMUEBLE"))
    s, e = span_indexes(texto, dirx); ents.append((s, e, "DIRECCION"))
    s, e = span_indexes(texto, dist); ents.append((s, e, "DISTRITO"))
    s, e = span_indexes(texto, prov); ents.append((s, e, "PROVINCIA"))
    s, e = texto.rindex(depa), texto.rindex(depa) + len(depa); ents.append((s, e, "DEPARTAMENTO"))
    return texto, ents


def ejemplo_plazo() -> Tuple[str, List[Tuple[int, int, str]]]:
    plazo = random.choice(PLAZOS)
    frames = [
        f"El plazo del contrato será de {plazo} contados a partir de la firma.",
        f"El presente contrato tendrá una duración de {plazo} desde la fecha de suscripción.",
        f"La vigencia del contrato es de {plazo}, iniciando el día de su firma.",
    ]
    texto = random.choice(frames)
    s, e = span_indexes(texto, plazo)
    return texto, [(s, e, "PLAZO")]


def ejemplo_interes() -> Tuple[str, List[Tuple[int, int, str]]]:
    interes = random.choice(INTERESES)
    texto = f"El préstamo devengará un interés anual del {interes}."
    s, e = span_indexes(texto, interes)
    return texto, [(s, e, "INTERES")]


def ejemplo_monto_simple() -> Tuple[str, List[Tuple[int, int, str]]]:
    valor = random.choice(MONTOS_BASE)
    monto = format_monto(valor)
    frames = [
        f"El monto total del préstamo asciende a {monto}.",
        f"La cantidad acordada para el contrato es de {monto}.",
        f"El importe total a pagar es de {monto}.",
    ]
    texto = random.choice(frames)
    s, e = span_indexes(texto, monto)
    return texto, [(s, e, "MONTO")]


def ejemplo_pago() -> Tuple[str, List[Tuple[int, int, str]]]:
    monto = format_monto(random.choice(MONTOS_BASE))
    banco = random.choice(["BCP", "BBVA", "Interbank", "Scotiabank"])
    cuenta = random_account_number(banco)
    frames = [
        f"El pago se realizará por el monto de {monto} a la cuenta N° {cuenta} del banco {banco}.",
        f"Se efectuará un pago de {monto} mediante transferencia a {cuenta} ({banco}).",
        f"Los pagos deberán hacerse en {monto} a la cuenta bancaria {cuenta} del {banco}.",
    ]
    texto = random.choice(frames)
    ents = []
    s, e = span_indexes(texto, monto); ents.append((s, e, "MONTO"))
    s, e = span_indexes(texto, cuenta); ents.append((s, e, "CUENTA_BANCARIA"))
    s, e = span_indexes(texto, banco); ents.append((s, e, "BANCO"))
    return texto, ents


def ejemplo_lugar_fecha() -> Tuple[str, List[Tuple[int, int, str]]]:
    ciudad = random.choice(CIUDADES)
    fecha = random_fecha_texto()
    frames = [
        f"{ciudad}, a los {fecha}.",
        f"Firmado en {ciudad}, el {fecha}.",
        f"En la ciudad de {ciudad}, a los {fecha}.",
        f"{ciudad}, Perú — {fecha}.",
    ]
    texto = random.choice(frames)
    ents = []
    s, e = span_indexes(texto, ciudad); ents.append((s, e, "CIUDAD"))
    s, e = span_indexes(texto, fecha); ents.append((s, e, "FECHA"))
    return texto, ents


def ejemplo_clausula_economica() -> Tuple[str, List[Tuple[int, int, str]]]:
    monto = format_monto(random.choice(MONTOS_BASE))
    plazo = random.choice(PLAZOS)
    interes = random.choice(INTERESES)
    penal = random.choice(PENALIDADES)

    frames = [
        f"El monto total del presente contrato es de {monto}, el cual deberá ser cancelado en un plazo de {plazo}.",
        f"La parte deudora pagará {monto} en un periodo máximo de {plazo}, aplicándose un interés del {interes}.",
        f"El arrendatario abonará {monto} cada mes, pudiendo aplicarse un interés moratorio del {interes}.",
        f"El precio final asciende a {monto}, que será pagado dentro de un plazo de {plazo}.",
        f"Las partes acuerdan un monto de {monto}, sujeto a una penalidad de {penal} en caso de incumplimiento.",
        f"Se establece una penalidad de {penal} si la parte deudora no cumple con el pago de {monto}.",
        f"En caso de retraso, se impondrá una penalidad equivalente a {penal}.",
        f"La penalidad aplicable será de {penal}, acumulativa por cada periodo incumplido.",
        f"Si la obligación por {monto} no es satisfecha, se generará una penalidad de {penal}.",
        f"Ante el incumplimiento del plazo pactado de {plazo}, se impondrá una penalidad de {penal}."
    ]

    texto = random.choice(frames)
    ents = []

    if monto in texto:
        s, e = span_indexes(texto, monto); ents.append((s, e, "MONTO"))
    if plazo in texto:
        s, e = span_indexes(texto, plazo); ents.append((s, e, "PLAZO"))
    if interes in texto and f" {interes}" in texto:
        s, e = span_indexes(texto, interes); ents.append((s, e, "INTERES"))
    if penal in texto:
        s, e = span_indexes(texto, penal); ents.append((s, e, "PENALIDAD"))

    return texto, ents



def ejemplo_negativo() -> Tuple[str, List[Tuple[int, int, str]]]:
    txt = random.choice([
        "Desde el inicio del contrato las partes acuerdan las condiciones.",
        "Hasta la fecha no se ha realizado ninguna modificación.",
        "Entre las partes existe un acuerdo previo.",
        "Durante el plazo establecido, las partes cumplirán sus obligaciones.",
        "Conforme a lo pactado, el contrato entra en vigencia inmediatamente.",
        "Las partes se reúnen virtualmente, sin indicar ciudad.",
    ])
    return txt, []

def numero_a_palabras(n: int) -> str:
    if n == 0:
        return "cero"
    if n < 0 or n > 999_999_999:
        raise ValueError("Fuera de rango (0..999,999,999)")

    def _tres_digitos(x: int) -> str:
        c = x // 100
        d = (x % 100) // 10
        u = x % 10
        partes = []
        # centenas
        if c:
            if c == 1 and (d == 0 and u == 0):
                partes.append("cien")
            else:
                partes.append(CENTENAS[c])
        # decenas y unidades
        if d == 0 and u > 0:
            partes.append(UNIDADES[u])
        elif d == 1:
            if u == 0:
                partes.append("diez")
            else:
                partes.append(ESPECIALES_11_19[10 + u])
        elif d == 2:
            if u == 0:
                partes.append("veinte")
            else:
                partes.append(f"veinti{UNIDADES[u]}")
        else:
            if d > 2:
                if u == 0:
                    partes.append(DECENAS[d])
                else:
                    partes.append(f"{DECENAS[d]} y {UNIDADES[u]}")
        return " ".join([p for p in partes if p])

    millones = n // 1_000_000
    miles = (n % 1_000_000) // 1_000
    cientos = n % 1_000

    out = []
    if millones:
        if millones == 1:
            out.append("un millón")
        else:
            out.append(f"{_tres_digitos(millones)} millones")
    if miles:
        if miles == 1:
            out.append("mil")
        else:
            out.append(f"{_tres_digitos(miles)} mil")
    if cientos:
        out.append(_tres_digitos(cientos))
    return " ".join(out).replace("  ", " ").strip()

def format_monto_numerico(valor: int, moneda: str = "S/") -> str:
    miles = f"{valor:,}".replace(",", ".")
    decimales = ".00"
    if moneda.upper() in {"USD", "US$"}:
        sim = random.choice(["USD", "US$"])
    else:
        sim = random.choice(["S/", "S/."])
    return f"{sim} {miles}{decimales}"

def ejemplo_monto_en_texto() -> tuple[str, list[tuple[int, int, str]]]:
    valor = random.choice([150, 200, 350, 500, 750, 1000, 1200, 1500, 1800, 2000, 2500, 3000, 4200, 5600, 6800, 8100, 9300, 10400])
    moneda_txt = random.choice(["soles", "dólares"])
    monto_palabras = numero_a_palabras(valor)
    monto_num = format_monto_numerico(valor, "USD" if moneda_txt == "dólares" else "S/")
    plantillas = [
        f"El monto total del contrato asciende a {monto_palabras} ({monto_num}) {moneda_txt}.",
        f"La suma acordada es de {monto_palabras} ({monto_num}) {moneda_txt}.",
        f"El importe a pagar será de {monto_palabras} ({monto_num}) {moneda_txt}.",
        f"Las partes fijan como precio {monto_palabras} ({monto_num}) {moneda_txt}.",
    ]
    texto = random.choice(plantillas)
    ents = []
    start = texto.index(monto_palabras)
    end = start + len(monto_palabras)
    ents.append((start, end, "MONTO"))
    return texto, ents


# -----------------------------
# Generación de docs
# -----------------------------

GEN_FUNCS = [
    ejemplo_persona_dni,
    ejemplo_empresa,
    ejemplo_persona_empresa,
    ejemplo_inmueble,
    ejemplo_monto_simple,
    ejemplo_pago,
    ejemplo_plazo,
    ejemplo_interes,
    ejemplo_lugar_fecha,
    ejemplo_clausula_economica,
    ejemplo_negativo,
    ejemplo_monto_en_texto,
]

def realinear_spans(texto_original: str, texto_ruido: str, ents):
    """
    Realinea las entidades después de aplicar ruido.
    Si una entidad no se encuentra → se descarta.
    """
    nuevas = []
    for start, end, label in ents:
        valor = texto_original[start:end]

        try:
            idx = texto_ruido.index(valor)
            nuevas.append((idx, idx + len(valor), label))
            continue
        except ValueError:
            pass

        try:
            idx = texto_ruido.lower().index(valor.lower())
            nuevas.append((idx, idx + len(valor), label))
            continue
        except ValueError:
            pass

        valor_compact = " ".join(valor.split())
        texto_compact = " ".join(texto_ruido.split())
        try:
            idx_c = texto_compact.index(valor_compact)
            first_token = valor_compact.split()[0].lower()
            idx_real = texto_ruido.lower().find(first_token)
            nuevas.append((idx_real, idx_real + len(valor), label))
            continue
        except:
            pass

        return None

    return nuevas


def create_safe_example(fn, noise_prob):
    """
    Genera un ejemplo completo:
    - genera texto base
    - aplica ruido
    - realinea entidades
    - si falla, retorna None
    """
    texto, ents = fn()
    texto_ruido = inject_noise(texto, noise_prob)

    ents_ajustadas = realinear_spans(texto, texto_ruido, ents)
    if ents_ajustadas is None:
        return None

    return texto_ruido, ents_ajustadas


def sample_examples(n_examples: int, noise_prob: float):
    """
    Versión robusta:
    - nunca produce spans inconsistentes
    - descarta ejemplos dañados
    """
    out = []
    while len(out) < n_examples:
        for fn in GEN_FUNCS:
            if len(out) >= n_examples:
                break

            item = create_safe_example(fn, noise_prob)

            if item is None:
                continue  # ejemplo inválido → repetir otro

            out.append(item)

    return out[:n_examples]


def make_doc(nlp, text: str, ents: List[Tuple[int, int, str]]) -> Doc:
    """
    Versión robusta de make_doc:
    - usa alignment_mode="contract"
    - ignora spans None
    - filtra solapamientos
    """
    doc = nlp.make_doc(text)
    spans = []
    for start, end, label in ents:
        span = doc.char_span(start, end, label=label, alignment_mode="contract")
        if span is not None:
            spans.append(span)
    spans = filter_spans(spans)
    doc.ents = spans
    return doc


def save_docbin(docs: List[Doc], path: Path):
    db = DocBin(store_user_data=False)
    for d in docs:
        db.add(d)
    path.parent.mkdir(parents=True, exist_ok=True)
    db.to_disk(path)

def label_stats(docs: List[Doc]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for d in docs:
        for e in d.ents:
            counts[e.label_] = counts.get(e.label_, 0) + 1
    return dict(sorted(counts.items(), key=lambda x: x[0]))

def to_jsonl_preview(docs: List[Doc], path: Path, max_items: int = 300):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf8") as f:
        for d in docs[:max_items]:
            item = {
                "text": d.text,
                "entities": [[e.start_char, e.end_char, e.label_] for e in d.ents]
            }
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

# -----------------------------
# Main (CLI)
# -----------------------------

def main():
    parser = argparse.ArgumentParser(description="Generador sintético NER (Contratos - Perú)")
    parser.add_argument("--train", type=int, default=4000, help="Ejemplos de entrenamiento")
    parser.add_argument("--dev", type=int, default=800, help="Ejemplos de validación")
    parser.add_argument("--test", type=int, default=800, help="Ejemplos de prueba")
    parser.add_argument("--outdir", type=str, default="training_data", help="Carpeta de salida")
    parser.add_argument("--seed", type=int, default=42, help="Semilla aleatoria")
    parser.add_argument("--noise", type=float, default=0.10, help="Probabilidad de ruido en el texto [0-1]")
    parser.add_argument("--jsonl", action="store_true", help="Exportar vista previa JSONL")
    args = parser.parse_args()

    set_seed(args.seed)

    nlp = spacy.blank("es")
    outdir = Path(args.outdir)

    train_raw = sample_examples(args.train, args.noise)
    dev_raw = sample_examples(args.dev, args.noise * 0.8)
    test_raw = sample_examples(args.test, args.noise * 0.0)

    train_docs = [make_doc(nlp, t, e) for t, e in train_raw]
    dev_docs = [make_doc(nlp, t, e) for t, e in dev_raw]
    test_docs = [make_doc(nlp, t, e) for t, e in test_raw]

    save_docbin(train_docs, outdir / "train.spacy")
    save_docbin(dev_docs, outdir / "dev.spacy")
    save_docbin(test_docs, outdir / "test.spacy")

    tr_stats = label_stats(train_docs)
    dv_stats = label_stats(dev_docs)
    ts_stats = label_stats(test_docs)

    print("=== Guardado ===")
    print(outdir / "train.spacy", len(train_docs), "docs")
    print(outdir / "dev.spacy", len(dev_docs), "docs")
    print(outdir / "test.spacy", len(test_docs), "docs")

    print("\n=== Etiquetas (train) ===")
    for k, v in tr_stats.items():
        print(f"{k}: {v}")
    print("\n=== Etiquetas (dev) ===")
    for k, v in dv_stats.items():
        print(f"{k}: {v}")
    print("\n=== Etiquetas (test) ===")
    for k, v in ts_stats.items():
        print(f"{k}: {v}")

    if args.jsonl:
        to_jsonl_preview(train_docs, outdir / "preview.jsonl", max_items=500)
        print(f"\nVista previa JSONL: {outdir / 'preview.jsonl'}")

if __name__ == "__main__":
    main()

# python generate_synthetic.py --train 10000 --dev 1500 --test 1500 --outdir training_data --seed 42 --noise 0.12 --jsonl