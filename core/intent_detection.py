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
    
    # Determinar si es una consulta general o específica para elegir mejor el tipo
    if mentioned_entities["usuario"] and not mentioned_entities["progreso"] and not mentioned_entities["recomendación"]:
        query_type = "user_performance"
    elif mentioned_entities["sucursal"] and not mentioned_entities["lista"] and not mentioned_entities["usuario"]:
        query_type = "branch_performance"
    elif mentioned_entities["actividad"] and not any(kw in cleaned_query.lower() for kw in ["ranking", "mejor", "peor"]):
        query_type = "activity_analysis"
    else:
        query_type = "exploratory_analysis"
    
    logger.debug(f"Fallback a consulta de tipo {query_type} con RAG")
    
    return {
        "requires_data": True,
        "query_type": query_type,
        "parameters": params,
        "use_context": has_context_reference
    }

