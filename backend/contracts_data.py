CONTRACTS = {
    "arrendamiento": {
        "nombre": "Contrato de Arrendamiento de Bien Inmueble",
        "descripcion": "Acuerdo mediante el cual una parte (arrendador) cede temporalmente el uso de un inmueble a otra (arrendatario) a cambio de una renta mensual.",
        "validez": "Documento privado con firma electrónica avanzada. Si el plazo supera 6 años o se inscribe en SUNARP, requiere formalización notarial.",
        "preguntas": [
            {"key": "arrendador", "texto": "Indique el nombre completo, DNI y domicilio del arrendador.", "tipo_dato": "arrendador"},
            {"key": "arrendatario", "texto": "Indique el nombre completo, DNI y domicilio del arrendatario.", "tipo_dato": "arrendatario"},
            {"key": "inmueble", "texto": "Especifique la dirección completa, distrito, provincia y departamento.", "tipo_dato": "inmueble"},
            {"key": "plazo", "texto": "Indique el plazo del arrendamiento (años, fecha de inicio y fecha de fin, días de preaviso y penalidad).", "tipo_dato": "plazo"},
            {"key": "renta", "texto": "Indique los montos y periodos de la renta (por ejemplo: 'S/ 1500 por los primeros 12 meses y S/ 1800 por los siguientes').", "tipo_dato": "renta"},
            {"key": "pago", "texto": "Indique el día límite de pago, los meses de incumplimiento que generan resolución y los datos de la cuenta bancaria del arrendador (tipo, número y banco).", "tipo_dato": "pago"},
            {"key": "inmueble_destino", "texto": "Indique el uso del inmueble (vivienda, local comercial, oficina, etc.).", "tipo_dato": "texto_simple"},
            {"key": "subarriendo", "texto": "¿Se permite el subarriendo? (responda sí o no).", "tipo_dato": "booleano"},
            {"key": "jurisdiccion", "texto": "Indique el distrito judicial para efectos de jurisdicción (ej. Lima, Piura, etc.).", "tipo_dato": "texto_simple"},
            {"key": "contrato", "texto": "Indique la ciudad y fecha de firma del contrato.", "tipo_dato": "lugar_fecha"}
        ],
        "sinonimos": ["arrendar", "alquilar", "rentar", "lease", "rent"],
        "documentos_requeridos": [
            "DNI vigente de ambas partes",
            "Documento que acredite propiedad o posesión",
            "Descripción y datos registrales del bien inmueble"
        ],
        "clausulas_minimas": [
            "Identificación de las partes",
            "Objeto y destino del contrato",
            "Renta, garantías y forma de pago",
            "Plazo del arrendamiento y renovación",
            "Obligaciones del arrendador y arrendatario",
            "Prohibición de subarriendo y mejoras no autorizadas",
            "Cláusula de allanamiento judicial",
            "Solución de controversias",
            "Firma y cierre"
        ],
        "advertencias": [
            "Si el contrato supera 6 años o busca inscripción en SUNARP, requiere escritura pública.",
            "Para facilitar un eventual desalojo, debe incluir la cláusula de allanamiento judicial."
        ],
        "jurisdiccion": "Perú – Código Civil, artículos 1666 al 1703.",
        "plantilla_alias": "arrendamiento_template"
    },


    "prestacion_servicios": {
        "nombre": "Contrato de Prestación de Servicios",
        "descripcion": "Una parte (prestador) se obliga a realizar un servicio para otra (cliente) a cambio de un pago.",
        "validez": "Documento privado firmado electrónicamente.",
        "preguntas": [
            {"key": "prestador", "texto": "Indique el nombre completo o razón social del prestador del servicio.", "tipo_dato": "persona_empresa"},
            {"key": "cliente", "texto": "Indique el nombre completo o razón social del cliente.", "tipo_dato": "persona_empresa"},
            {"key": "objeto", "texto": "Describa el servicio específico que se prestará.", "tipo_dato": "texto_simple"},
            {"key": "pago", "texto": "Defina el monto total y la forma o calendario de pago.", "tipo_dato": "monto_condiciones"},
            {"key": "plazo", "texto": "Señale el plazo estimado para la prestación del servicio.", "tipo_dato": "texto_simple"}
        ],
        "sinonimos": ["prestacion", "servicio", "servicios", "contratacion", "freelance", "outsourcing"],
        "documentos_requeridos": [
            "DNI o RUC del prestador y cliente",
            "Descripción técnica del servicio",
            "Datos de cuenta bancaria para pagos"
        ],
        "clausulas_minimas": [
            "Objeto del servicio",
            "Retribución y forma de pago",
            "Obligaciones del prestador",
            "Obligaciones del cliente",
            "Plazos",
            "Confidencialidad",
            "Solución de controversias",
            "Firma y cierre"
        ],
        "advertencias": [
            "No genera vínculo laboral a menos que se estipulen obligaciones propias del contrato de trabajo."
        ],
        "jurisdiccion": "Perú – Código Civil",
        "plantilla_alias": "servicios_template"
    },

    "compraventa_bienes_muebles": {
        "nombre": "Contrato de Compraventa de Bienes Muebles",
        "descripcion": "Transferencia de propiedad de un bien mueble a cambio de un precio.",
        "validez": "Documento privado, salvo que el bien sea registrable en SUNARP (ej. automóvil).",
        "preguntas": [
            {"key": "vendedor", "texto": "Indique el nombre completo y DNI del vendedor.", "tipo_dato": "persona_dni"},
            {"key": "comprador", "texto": "Indique el nombre completo y DNI del comprador.", "tipo_dato": "persona_dni"},
            {"key": "objeto", "texto": "Describa detalladamente el bien mueble objeto de compraventa.", "tipo_dato": "texto_simple"},
            {"key": "pago", "texto": "Especifique el precio total y la forma de pago.", "tipo_dato": "monto_condiciones"},
            {"key": "entrega", "texto": "Señale el lugar y la fecha de entrega del bien.", "tipo_dato": "lugar_fecha"}
        ],
        "sinonimos": ["compraventa", "venta", "compra", "adquisicion", "transferencia", "traspaso"],
        "documentos_requeridos": [
            "DNI de comprador y vendedor",
            "Documento que acredite propiedad",
            "Ficha técnica o serie del bien"
        ],
        "clausulas_minimas": [
            "Objeto de la compraventa",
            "Precio y forma de pago",
            "Entrega del bien",
            "Garantías",
            "Riesgos y saneamiento",
            "Solución de controversias",
            "Firma y cierre"
        ],
        "advertencias": [
            "Si el bien es registrable, la transferencia debe inscribirse.",
            "Se recomienda verificar cargas o gravámenes."
        ],
        "jurisdiccion": "Perú – Código Civil",
        "plantilla_alias": "compraventa_mueble_template"
    },

    "comodato": {
        "nombre": "Contrato de Comodato (Préstamo de Uso)",
        "descripcion": "Una parte entrega gratuitamente un bien a otra para su uso temporal, con obligación de devolverlo.",
        "validez": "Documento privado, no requiere notario.",
        "preguntas": [
            {"key": "comodante", "texto": "Indique el nombre completo y DNI del comodante (quien presta el bien).", "tipo_dato": "persona_dni"},
            {"key": "comodatario", "texto": "Indique el nombre completo y DNI del comodatario (quien recibe el bien).", "tipo_dato": "persona_dni"},
            {"key": "objeto", "texto": "Describa el bien objeto del comodato.", "tipo_dato": "texto_simple"},
            {"key": "uso_permitido", "texto": "Señale el uso específico para el cual se presta el bien.", "tipo_dato": "texto_simple"},
            {"key": "plazo", "texto": "Indique el plazo del comodato (tiempo de uso).", "tipo_dato": "texto_simple"} # Puede ser fecha o condición
        ],
        "sinonimos": ["comodato", "prestamo_uso", "prestamo bien", "uso", "loan for use"],"sinonimos": ["comodato", "prestamo_uso", "prestamo bien", "uso", "loan for use"],
        "documentos_requeridos": [
            "DNI de ambas partes",
            "Descripción del bien"
        ],
        "clausulas_minimas": [
            "Objeto del comodato",
            "Obligaciones del comodatario",
            "Obligaciones del comodante",
            "Plazo",
            "Responsabilidad por daños",
            "Firma y cierre"
        ],
        "advertencias": [
            "El comodatario no puede ceder el bien a un tercero sin autorización.",
            "Debe devolverse el bien en el mismo estado."
        ],
        "jurisdiccion": "Perú – Código Civil",
        "plantilla_alias": "comodato_template"
    },

    "mutuo": {
        "nombre": "Contrato de Mutuo (Préstamo de Dinero)",
        "descripcion": "Una parte entrega dinero a otra con obligación de devolverlo en un plazo determinado.",
        "validez": "Documento privado, no requiere notario.",
        "preguntas": [
            {"key": "mutuante", "texto": "Indique el nombre completo y DNI del mutuante (quien entrega el dinero).", "tipo_dato": "persona_dni"},
            {"key": "mutuario", "texto": "Indique el nombre completo y DNI del mutuario (quien recibe el dinero).", "tipo_dato": "persona_dni"},
            {"key": "monto", "texto": "Señale el monto total del préstamo.", "tipo_dato": "monto_simple"},
            {"key": "plazo_devolucion", "texto": "Defina el plazo y condiciones de devolución del dinero.", "tipo_dato": "texto_simple"},
            {"key": "intereses", "texto": "¿El préstamo genera intereses? En caso afirmativo, indique el porcentaje.", "tipo_dato": "interes"}
        ],
        "sinonimos": ["mutuo", "prestamo", "dinero", "credito", "loan", "cash"],
        "documentos_requeridos": [
            "DNI de ambas partes",
            "Medio de entrega del dinero (voucher o constancia)"
        ],
        "clausulas_minimas": [
            "Objeto del mutuo",
            "Monto",
            "Intereses",
            "Plazos",
            "Garantías (si aplica)",
            "Firma y cierre"
        ],
        "advertencias": [
            "Si se pactan intereses, deben referirse a la tasa que aplica en el Perú."
        ],
        "jurisdiccion": "Perú – Código Civil",
        "plantilla_alias": "mutuo_template"
    },
    
    "reconocimiento_deuda": {
        "nombre": "Reconocimiento de Deuda",
        "descripcion": "Documento mediante el cual una parte reconoce que debe una cantidad determinada a otra.",
        "validez": "Documento privado con firma electrónica avanzada.",
        "preguntas": [
            {"key": "deudor", "texto": "Indique el nombre completo y DNI del deudor.", "tipo_dato": "persona_dni"},
            {"key": "acreedor", "texto": "Indique el nombre completo y DNI del acreedor.", "tipo_dato": "persona_dni"},
            {"key": "monto", "texto": "Señale la cantidad exacta de la deuda.", "tipo_dato": "monto_simple"},
            {"key": "origen_deuda", "texto": "Describa el origen o motivo de la deuda.", "tipo_dato": "texto_simple"},
            {"key": "plazo_pago", "texto": "Defina el plazo y la forma de pago.", "tipo_dato": "monto_condiciones"} # Se puede reusar monto_condiciones
        ],
        "sinonimos": ["reconocimiento", "deuda", "adeudo", "reconocimiento_deuda", "debt acknowledgment"],
        "documentos_requeridos": [
            "DNI de ambas partes",
            "Documento o mensaje que origine la deuda (si existe)"
        ],
        "clausulas_minimas": [
            "Reconocimiento expreso",
            "Monto y origen",
            "Plazo",
            "Intereses moratorios",
            "Firma y cierre"
        ],
        "advertencias": [
            "Genera obligación exigible judicialmente."
        ],
        "jurisdiccion": "Perú – Código Civil",
        "plantilla_alias": "reconocimiento_deuda_template"
    },

    "trabajo_privado": {
        "nombre": "Contrato de Trabajo Privado",
        "descripcion": "Regula la relación laboral entre empleador y trabajador, fuera de planilla formal.",
        "validez": "Documento privado firmado electrónicamente.",
        "preguntas": [
            {"key": "empleador", "texto": "Indique el nombre completo y DNI del empleador.", "tipo_dato": "persona_dni"},
            {"key": "trabajador", "texto": "Indique el nombre completo y DNI del trabajador.", "tipo_dato": "persona_dni"},
            {"key": "funciones", "texto": "Describa las funciones o actividades que realizará el trabajador.", "tipo_dato": "texto_simple"},
            {"key": "remuneracion", "texto": "Especifique la remuneración y forma de pago.", "tipo_dato": "monto_condiciones"},
            {"key": "jornada_plazo", "texto": "Defina la jornada de trabajo y el plazo del contrato.", "tipo_dato": "texto_simple"}
        ],
        "sinonimos": ["trabajo", "empleo", "laboral", "contrato_trabajo", "job", "hiring"],
        "documentos_requeridos": [
            "DNI de ambas partes",
            "Descripción del puesto o funciones"
        ],
        "clausulas_minimas": [
            "Identificación de las partes",
            "Funciones",
            "Retribución",
            "Horario",
            "Causales de despido",
            "Firma y cierre"
        ],
        "advertencias": [
            "Puede ser considerado relación laboral formal según SUNAFIL."
        ],
        "jurisdiccion": "Perú – Código Civil y normativa laboral",
        "plantilla_alias": "trabajo_privado_template"
    },

    "carta_poder": {
        "nombre": "Carta Poder Simple",
        "descripcion": "Autoriza a otra persona para realizar gestiones simples que no requieren inscripción registral.",
        "validez": "Documento privado con firma biométrica.",
        "preguntas": [
            {"key": "poderdante", "texto": "Indique el nombre completo y DNI del poderdante (quien otorga el poder).", "tipo_dato": "persona_dni"},
            {"key": "apoderado", "texto": "Indique el nombre completo y DNI del apoderado (quien recibe el poder).", "tipo_dato": "persona_dni"},
            {"key": "facultades", "texto": "Describa las facultades específicas que se otorgan.", "tipo_dato": "texto_simple"},
            {"key": "plazo", "texto": "Defina el plazo de vigencia de la carta poder.", "tipo_dato": "texto_simple"} # "indefinido" o fecha
        ],
        "sinonimos": ["carta poder", "poder", "autorizacion", "representacion", "power of attorney"],
        "documentos_requeridos": [
            "DNI del poderdante y apoderado"
        ],
        "clausulas_minimas": [
            "Identificación de las partes",
            "Facultades otorgadas",
            "Vigencia",
            "Limitaciones",
            "Firma y cierre"
        ],
        "advertencias": [
            "No tiene validez para actos registrales."
        ],
        "jurisdiccion": "Perú – Código Civil",
        "plantilla_alias": "carta_poder_template"
    },

    "asociacion_en_participacion": {
        "nombre": "Contrato de Asociación en Participación",
        "descripcion": "Asociación sin personería jurídica para un negocio específico con aporte de recursos.",
        "validez": "Documento privado.",
        "preguntas": [
            {"key": "asociante", "texto": "Indique el nombre completo o razón social del asociante.", "tipo_dato": "persona_empresa"},
            {"key": "asociado", "texto": "Indique el nombre completo o razón social del asociado.", "tipo_dato": "persona_empresa"},
            {"key": "objeto_negocio", "texto": "Describa el negocio o actividad objeto de la asociación.", "tipo_dato": "texto_simple"},
            {"key": "aportes", "texto": "Especifique el aporte de cada parte (dinero, bienes o servicios).", "tipo_dato": "texto_simple"},
            {"key": "distribucion", "texto": "Defina la forma de distribución de utilidades y pérdidas.", "tipo_dato": "texto_simple"}
        ],
        "sinonimos": ["asociacion", "participacion", "negocio conjunto", "joint venture", "partnership"],
        "documentos_requeridos": [
            "DNI o RUC de las partes",
            "Detalle del negocio y aportes"
        ],
        "clausulas_minimas": [
            "Objeto del negocio",
            "Aportes",
            "Participación",
            "Responsabilidad",
            "Plazo",
            "Firma y cierre"
        ],
        "advertencias": [
            "No crea persona jurídica independiente.",
            "No debe confundirse con una sociedad comercial."
        ],
        "jurisdiccion": "Perú – Código Civil",
        "plantilla_alias": "asociacion_template"
    },

    "confidencialidad": {
        "nombre": "Contrato de Confidencialidad (NDA)",
        "descripcion": "Protege información sensible compartida entre partes frente a terceros.",
        "validez": "Documento privado con firma electrónica.",
        "preguntas": [
            {"key": "partes", "texto": "Indique las partes involucradas (nombres completos o razones sociales).", "tipo_dato": "texto_simple"}, # Puede ser complejo
            {"key": "info_confidencial", "texto": "Describa la información que será considerada confidencial.", "tipo_dato": "texto_simple"},
            {"key": "plazo", "texto": "Defina el plazo de vigencia del acuerdo.", "tipo_dato": "texto_simple"},
            {"key": "obligaciones", "texto": "Indique las obligaciones principales de las partes.", "tipo_dato": "texto_simple"},
            {"key": "penalidades", "texto": "Señale las consecuencias en caso de incumplimiento.", "tipo_dato": "texto_simple"}
        ],
        "sinonimos": ["confidencialidad", "nda", "secreto", "reserva", "non disclosure"],
        "documentos_requeridos": [
            "DNI o RUC de las partes"
        ],
        "clausulas_minimas": [
            "Objeto del acuerdo",
            "Definición de información confidencial",
            "Obligaciones",
            "Plazo",
            "Penalidades",
            "Firma y cierre"
        ],
        "advertencias": [
            "No protege información ya pública."
        ],
        "jurisdiccion": "Perú – Código Civil",
        "plantilla_alias": "confidencialidad_template"
    },

    "franquicia": {
        "nombre": "Contrato de Franquicia o Distribución",
        "descripcion": "Regula la relación comercial entre un franquiciante y un franquiciado o distribuidor.",
        "validez": "Documento privado, salvo inscripción registral voluntaria.",
        "preguntas": [
            {"key": "franquiciante", "texto": "Indique el nombre completo o razón social del franquiciante.", "tipo_dato": "persona_empresa"},
            {"key": "franquiciado", "texto": "Indique el nombre completo o razón social del franquiciado.", "tipo_dato": "persona_empresa"},
            {"key": "objeto", "texto": "Describa el objeto de la franquicia o distribución.", "tipo_dato": "texto_simple"},
            {"key": "obligaciones", "texto": "Especifique los derechos y obligaciones de cada parte.", "tipo_dato": "texto_simple"},
            {"key": "pago_plazo", "texto": "Defina la contraprestación (pago, regalías, comisiones) y el plazo del contrato.", "tipo_dato": "monto_condiciones"}
        ],
        "sinonimos": ["franquicia", "distribucion", "licencia", "concesion", "distribution", "franchise"],
        "documentos_requeridos": [
            "RUC de las partes",
            "Marca o producto a comercializar"
        ],
        "clausulas_minimas": [
            "Objeto",
            "Derechos otorgados",
            "Pagos",
            "Obligaciones",
            "Plazo",
            "Firma y cierre"
        ],
        "advertencias": [
            "Se recomienda verificar propiedad intelectual antes de firmar."
        ],
        "jurisdiccion": "Perú – Código Civil",
        "plantilla_alias": "franquicia_template"
    },

    "publicidad": {
        "nombre": "Contrato de Publicidad o Marketing",
        "descripcion": "Regula la difusión de contenido en medios físicos o digitales.",
        "validez": "Documento privado.",
        "preguntas": [
            {"key": "anunciante", "texto": "Indique el nombre completo o razón social del anunciante.", "tipo_dato": "persona_empresa"},
            {"key": "publicista", "texto": "Indique el nombre completo o razón social del medio o publicista.", "tipo_dato": "persona_empresa"},
            {"key": "campana", "texto": "Describa el contenido o campaña publicitaria.", "tipo_dato": "texto_simple"},
            {"key": "pago", "texto": "Defina el monto total y la forma de pago.", "tipo_dato": "monto_condiciones"},
            {"key": "plazo_medios", "texto": "Señale el plazo y los medios de difusión.", "tipo_dato": "texto_simple"}
        ],
        "sinonimos": ["publicidad", "marketing", "anuncio", "campaña", "difusion", "advertising"],
        "documentos_requeridos": [
            "DNI o RUC de las partes",
            "Material o guion publicitario"
        ],
        "clausulas_minimas": [
            "Objeto de la campaña",
            "Medios",
            "Pagos",
            "Obligaciones",
            "Plazo",
            "Firma y cierre"
        ],
        "advertencias": [
            "El contenido publicitario debe cumplir normativa de INDECOPI."
        ],
        "jurisdiccion": "Perú – Código Civil",
        "plantilla_alias": "publicidad_template"
    }
}

# --- Base de Conocimiento de Cláusulas Mapeadas ---
# Aquí mapeas palabras clave (lemas) a cláusulas legales estándar.
CLAUSULAS_MAPEADAS = {
    "prohibicion_subarriendo": {
        "keywords": ["subarrendar", "subarriendo", "alquilar", "tercero"],
        "titulo": "Cláusula de Prohibición de Subarriendo",
        "texto": "EL ARRENDATARIO queda terminantemente prohibido de subarrendar, ceder o transferir total o parcialmente el inmueble a terceros, sin el consentimiento expreso y por escrito de EL ARRENDADOR."
    },
    "prohibicion_mascotas": {
        "keywords": ["mascota", "perro", "gato", "animal"],
        "titulo": "Cláusula de Tenencia de Mascotas",
        "texto": "EL ARRENDATARIO se compromete a no introducir ni mantener mascotas de ninguna especie en el inmueble, salvo autorización expresa y por escrito de EL ARRENDADOR."
    },
    "danos_deterioro": {
        "keywords": ["daño", "romper", "malograr", "deterioro", "reparacion"],
        "titulo": "Cláusula de Deterioro y Daños",
        "texto": "EL ARRENDATARIO se compromete a devolver el inmueble en el mismo estado en que lo recibió, salvo el deterioro normal por el uso. Cualquier daño intencional o negligente causado al inmueble será reparado por cuenta y costo de EL ARRENDATARIO."
    },
    "penalidad_moratoria": {
        "keywords": ["mora", "retraso", "tardanza", "penalidad", "multa", "interes"],
        "titulo": "Cláusula de Penalidad Moratoria",
        "texto": "En caso de retraso en el pago de la renta mensual, EL ARRENDATARIO incurrirá en mora automática sin necesidad de previo aviso, aplicándose una penalidad moratoria equivalente al 0.5% del monto de la renta por cada día de retraso, hasta un máximo acumulable del 15% de la renta."
    }
}

DEF_AFFIRMATIVES = {"si", "sí", "s", "ok", "claro", "confirmo", "confirmar", "dale", "vamos", "start", "comenzar", "yes"}
DEF_NEGATIVES = {"no", "n", "negativo", "cancelar", "otra", "cambiar", "not", "nope"}
CHANGE_KEYWORDS = {"cambiar", "cambio", "otro", "otra", "quiero otro", "quiero cambiar"}