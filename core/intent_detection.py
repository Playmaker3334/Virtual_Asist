import json
import re
import traceback
import logging
from openai import OpenAI as ClientOpenAI
from core.config import (
    OPENAI_API_KEY,
    DEFAULT_OPENAI_MODEL,
)
from core.text_processing import clean_text  # para limpiar el query si lo deseas
from prompts.determine_intent_prompt import DETERMINE_INTENT_SYSTEM_PROMPT

# Configurar el logger para enviar los logs a la terminal
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()  # Salida estándar (terminal)
formatter = logging.Formatter('[%(levelname)s] %(asctime)s - %(name)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

client = ClientOpenAI(api_key=OPENAI_API_KEY)


def determine_intent(query: str) -> dict:
    """
    Determina la intención de la consulta del usuario usando GPT-4.
    Si hay un error, deriva a una consulta exploratoria que utilizará RAG.
    """
    logger.debug("Analizando consulta: '%s'", query)
    
    # Opcionalmente, limpiamos el texto
    cleaned_query = clean_text(query)
    
    # Extraer entidades entre comillas (nombres de usuarios, actividades, etc.)
    quoted_entities = re.findall(r'"([^"]+)"', query)
    
    # Palabras clave para preguntas sobre datos
    data_question_keywords = [
        "qué datos", "que datos", "cuáles datos", "cuales datos",
        "qué información", "que información", "qué registros", "que registros",
        "cómo lo calculaste", "como lo calculaste", "cómo lo obtuviste",
        "como lo obtuviste", "de dónde", "de donde", "qué usaste", "que usaste"
    ]

    # Palabras que indican referencia a consultas previas
    context_reference_words = [
        "mismo", "misma", "mismos", "mismas", 
        "esa", "ese", "esos", "esas", 
        "esta", "este", "estos", "estas",
        "aquella", "aquel", "aquellos", "aquellas",
        "dicha", "dicho", "dichos", "dichas",
        "anterior", "previo", "previa", "mencionado", "mencionada"
    ]

    # Keywords para diferentes entidades
    entity_keywords = {
        "sucursal": ["sucursal", "branch", "sede", "oficina"],
        "usuario": ["usuario", "user", "empleado", "vendedor", "representante"],
        "actividad": ["actividad", "activity", "tarea", "ejercicio", "ronda"],
        "progreso": ["progreso", "evolución", "avance", "trayectoria", "desarrollo", "tendencia", "cambiado"],
        "recomendación": ["recomendación", "recomendaciones", "sugerencia", "sugerencias", "ayúdame", "mejorar"],
        "lista": ["quiénes", "quienes", "cuáles", "cuales", "lista", "nombres", "listado", "dame", "darme", "mostrar", "ver"]
    }

    # Verificar si hay referencia a contexto previo
    has_context_reference = any(word in cleaned_query.lower() for word in context_reference_words)
    
    # Determinar entidades mencionadas
    mentioned_entities = {
        entity: any(keyword in cleaned_query.lower() for keyword in keywords)
        for entity, keywords in entity_keywords.items()
    }

    # Procesar entidades entre comillas
    usuario_value = None
    actividad_value = None
    sucursal_value = None
    
    for entity in quoted_entities:
        if any(kw in entity.lower() for kw in ["representante", "usuario", "user"]):
            usuario_value = entity
        elif any(kw in entity.lower() for kw in ["ronda", "actividad"]):
            actividad_value = entity
        elif "sucursal" in entity.lower():
            sucursal_value = entity
    
    # Si no se encontró una entidad específica pero hay entidades entre comillas
    if not usuario_value and not actividad_value and not sucursal_value and quoted_entities:
        if mentioned_entities["usuario"]:
            usuario_value = quoted_entities[0]
        elif mentioned_entities["actividad"]:
            actividad_value = quoted_entities[0]
        elif mentioned_entities["sucursal"]:
            sucursal_value = quoted_entities[0]

    # NUEVO: Extraer fechas directamente del texto para mantener el formato original
    date_patterns = [
        r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',  # formatos DD/MM/YYYY o DD-MM-YYYY
        r'(\d{4}[/-]\d{1,2}[/-]\d{1,2})',    # formatos YYYY/MM/DD o YYYY-MM-DD
    ]
    
    fecha_value = None
    for pattern in date_patterns:
        date_matches = re.findall(pattern, query)
        if date_matches:
            fecha_value = date_matches[0]
            logger.debug(f"Fecha extraída directamente del texto: {fecha_value}")
            break

    # Intentar con GPT-4 con hasta 3 intentos
    max_attempts = 3
    
    for attempt in range(max_attempts):
        try:
            messages = [
                {
                    "role": "system",
                    "content": DETERMINE_INTENT_SYSTEM_PROMPT
                },
                {
                    "role": "user",
                    "content": cleaned_query
                }
            ]
            
            response = client.chat.completions.create(
                model=DEFAULT_OPENAI_MODEL,
                messages=messages,
                temperature=0.3
            )
            
            try:
                intent = json.loads(response.choices[0].message.content)
                
                # Si se detectó una referencia a contexto, forzar el uso de contexto
                if has_context_reference:
                    intent["use_context"] = True
                
                # Añadir entidades extraídas si no están ya en los parámetros
                if "parameters" in intent:
                    if "usuario" in intent["parameters"] and intent["parameters"]["usuario"] is None:
                        intent["parameters"]["usuario"] = usuario_value
                    if "actividad" in intent["parameters"] and intent["parameters"]["actividad"] is None:
                        intent["parameters"]["actividad"] = actividad_value
                    if "sucursal" in intent["parameters"] and intent["parameters"]["sucursal"] is None:
                        intent["parameters"]["sucursal"] = sucursal_value
                    
                    # NUEVO: Reemplazar el formato de fecha de GPT con el extraído directamente del texto
                    if "fecha" in intent["parameters"] and fecha_value:
                        # Si GPT encontró una fecha pero con formato incorrecto, reemplazarla
                        intent["parameters"]["fecha"] = fecha_value
                        logger.debug(f"Reemplazando fecha de GPT con fecha extraída: {fecha_value}")
                    elif "fecha" in intent["parameters"] and intent["parameters"]["fecha"]:
                        # Si no extrajimos la fecha pero GPT sí, verificar su formato
                        gpt_fecha = intent["parameters"]["fecha"]
                        # Corregir formato si es necesario (por ejemplo, si es solo números sin separadores)
                        if re.match(r'^\d{8}$', gpt_fecha):  # formato DDMMYYYY o YYYYMMDD
                            if int(gpt_fecha[:2]) <= 31 and int(gpt_fecha[2:4]) <= 12:
                                # Probable formato DDMMYYYY
                                intent["parameters"]["fecha"] = f"{gpt_fecha[:2]}/{gpt_fecha[2:4]}/{gpt_fecha[4:]}"
                                logger.debug(f"Reformateando fecha de GPT de {gpt_fecha} a {intent['parameters']['fecha']}")
                            elif int(gpt_fecha[:4]) >= 2000 and int(gpt_fecha[4:6]) <= 12:
                                # Probable formato YYYYMMDD
                                intent["parameters"]["fecha"] = f"{gpt_fecha[6:]}/{gpt_fecha[4:6]}/{gpt_fecha[:4]}"
                                logger.debug(f"Reformateando fecha de GPT de {gpt_fecha} a {intent['parameters']['fecha']}")

                logger.debug("Intención detectada por GPT-4: %s", intent)
                return intent
                
            except json.JSONDecodeError:
                logger.warning(f"Intento {attempt+1}/{max_attempts}: GPT-4 no devolvió un JSON válido. Reintentando...")
                if attempt == max_attempts - 1:
                    raise
                
        except Exception as e:
            error_msg = f"Intento {attempt+1}/{max_attempts}: Error en GPT-4: {str(e)}"
            logger.warning(error_msg)
            if attempt == max_attempts - 1:
                logger.error(f"Todos los intentos con GPT-4 fallaron: {str(e)}. Derivando a RAG.")
                traceback.print_exc()

    # Si todos los intentos fallaron, derivar a RAG (modo exploratorio)
    # Extraer parámetros potenciales de las entidades detectadas
    params = {}
    if usuario_value:
        params["usuario"] = usuario_value
    if actividad_value:
        params["actividad"] = actividad_value
    if sucursal_value:
        params["sucursal"] = sucursal_value
    if fecha_value:
        params["fecha"] = fecha_value
    
    # Determinar si es una consulta general o específica para elegir mejor el tipo
    if mentioned_entities["usuario"] and not mentioned_entities["progreso"] and not mentioned_entities["recomendación"]:
        query_type = "user_performance"
    elif mentioned_entities["sucursal"] and not mentioned_entities["lista"] and not mentioned_entities["usuario"]:
        query_type = "branch_performance"
    elif mentioned_entities["actividad"] and not any(kw in cleaned_query.lower() for kw in ["ranking", "mejor", "peor"]):
        query_type = "activity_analysis"
    elif fecha_value:  # NUEVO: Si encontramos una fecha, probablemente es una consulta de fecha específica
        query_type = "specific_date"
    else:
        query_type = "exploratory_analysis"
    
    logger.debug(f"Fallback a consulta de tipo {query_type} con RAG")
    
    return {
        "requires_data": True,
        "query_type": query_type,
        "parameters": params,
        "use_context": has_context_reference
    }

