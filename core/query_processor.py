# core/query_processor.py
import os
import sys
import traceback
import logging

# 1) Ajustamos el path para que Python encuentre la carpeta principal:
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

# Configurar el logger para enviar los logs a la terminal
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()  # Salida estándar (terminal)
formatter = logging.Formatter('[%(levelname)s] %(asctime)s - %(name)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Importa la lógica de detección de intención
from core.intent_detection import determine_intent

# Importa funciones comunes del querys_Fact_RolPlay_Sim
from querys.querys_Fact_RolPlay_Sim import (
    handle_error,
    update_context,
    get_last_context,
    parse_flexible_date
)

# Importa las funciones de actividades / sucursales / tiempo
from querys.querys_activities import (
    get_activity_stats,
    get_activity_rankings,
    get_branch_performance,
    get_branch_rankings,
    get_time_period_analysis,
    get_trend_analysis,
    get_comparative_analysis,
    get_correlation_analysis,
    get_branch_stats,
    get_top_performances
)


# Importa las funciones de usuarios / rankings / recomendaciones
from querys.querys_users import (
    get_user_activity_history,
    get_user_progression,
    get_user_rankings,
    get_users_by_branch,
    get_personalized_recommendations,
    advanced_search,
    get_general_stats,
    get_exact_activity_result
)

# Importar la clase RolPlayRAG para tipado
from rag_engine import RolPlayRAG


def process_query(rag_engine: RolPlayRAG, query: str, generate_response_func) -> str:
    try:
        # 1. Determinar la intención con determine_intent
        logger.info("Query recibida: %s", query)
        intent = determine_intent(query)
        logger.debug("Intención detectada: %s", intent)

        # 2. Si la intención NO requiere datos, es charla general
        if not intent["requires_data"]:
            logger.debug("Consulta de conversación general. No se requieren datos.")
            return generate_response_func(query)
        
        query_type = intent["query_type"]
        parameters = intent["parameters"]

        # 3. Revisar si se usa contexto previo
        if intent["use_context"]:
            last_ctx = get_last_context()
            logger.debug("Usando contexto previo: %s", last_ctx)
            for key, value in last_ctx.items():
                if key in parameters and (not parameters[key] or parameters[key] is None):
                    parameters[key] = value
                    logger.debug("Aplicando valor de contexto para %s: %s", key, value)

        # NUEVO: 4. Usar RAG para consultas exploratorias específicas
        if query_type == "exploratory_analysis":
            logger.debug("USANDO RAG para consulta exploratoria: %s", query)
            response_data = rag_engine.query(query)
        # 5. Ejecutar la consulta apropiada según query_type
        elif query_type == "specific_date":
            response_data = get_exact_activity_result(
                rag_engine.raw_data,
                parameters.get("fecha"),
                parameters.get("actividad")
            )
        elif query_type == "user_performance":
            # Verificar si el texto de la consulta contiene "Representante" entre comillas
            if "representante" in query.lower() and '"' in query:
                import re
                representante_match = re.search(r'"([^"]*representante[^"]*)"', query, re.IGNORECASE)
                if representante_match:
                    parameters["usuario"] = representante_match.group(1)
                    logger.debug("Usuario extraído de comillas: %s", parameters["usuario"])
                    
            response_data = get_user_activity_history(
                rag_engine.raw_data,
                parameters.get("usuario")
            )
        elif query_type == "branch_performance":
            # Si no se especifica una sucursal, usar branch_rankings en su lugar
            if parameters.get("sucursal") is None:
                logger.debug("No se especificó sucursal, usando rankings de sucursales en su lugar")
                response_data = get_branch_rankings(rag_engine.raw_data)
                # Si la consulta contiene palabras que indican buscar la peor sucursal
                if any(word in query.lower() for word in ["peor", "menor", "más bajo", "mas bajo", "mala"]):
                    parameters["tipo"] = "peores"
                    if "rankings" in response_data.get("data", {}) and "por_calificacion" in response_data["data"]["rankings"]:
                        peores = sorted(response_data["data"]["rankings"]["por_calificacion"], key=lambda x: x["promedio_calificacion"])
                        if peores:
                            peor_sucursal = peores[0]["sucursal"]
                            logger.debug("Guardando peor sucursal en contexto: %s", peor_sucursal)
                            parameters["sucursal"] = peor_sucursal
                else:
                    # Si no se busca la peor, se asume la mejor
                    if "mejor_sucursal" in response_data.get("data", {}):
                        mejor_sucursal = response_data["data"]["mejor_sucursal"]["sucursal"]
                        logger.debug("Guardando mejor sucursal en contexto: %s", mejor_sucursal)
                        parameters["sucursal"] = mejor_sucursal
            else:
                response_data = get_branch_performance(
                    rag_engine.raw_data,
                    parameters.get("sucursal")
                )
        elif query_type == "activity_analysis":
            response_data = get_activity_stats(
                rag_engine.raw_data,
                parameters.get("actividad")
            )
        elif query_type == "top_performance":
            response_data = get_top_performances(
                rag_engine.raw_data,
                parameters.get("n", 5),
                parameters.get("metric", "calificacion"),
                parameters.get("filtros", {})
            )
        elif query_type == "comparative":
            response_data = get_comparative_analysis(
                rag_engine.raw_data,
                parameters.get("usuarios"),
                parameters.get("fechas"),
                parameters.get("actividad")
            )
        elif query_type == "trend":
            response_data = get_trend_analysis(
                rag_engine.raw_data,
                parameters.get("usuario"),
                parameters.get("actividad"),
                parameters.get("sucursal"),
                parameters.get("periodo", "day")
            )
        elif query_type == "branch_ranking":
            response_data = get_branch_rankings(rag_engine.raw_data)
            # Extraer y guardar la sucursal adecuada en el contexto
            if response_data and "data" in response_data:
                if parameters.get("tipo") == "peores":
                    if "rankings" in response_data["data"] and "por_calificacion" in response_data["data"]["rankings"]:
                        peores = sorted(response_data["data"]["rankings"]["por_calificacion"], key=lambda x: x["promedio_calificacion"])
                        if peores:
                            peor_sucursal = peores[0]["sucursal"]
                            logger.debug("Guardando peor sucursal en contexto: %s", peor_sucursal)
                            parameters["sucursal"] = peor_sucursal
                else:
                    if "mejor_sucursal" in response_data["data"]:
                        mejor_sucursal = response_data["data"]["mejor_sucursal"]["sucursal"]
                        logger.debug("Guardando mejor sucursal en contexto: %s", mejor_sucursal)
                        parameters["sucursal"] = mejor_sucursal
        elif query_type == "branch_stats":
            response_data = get_branch_stats(rag_engine.raw_data)
        elif query_type == "activity_ranking":
            response_data = get_activity_rankings(rag_engine.raw_data)
        elif query_type == "time_period":
            response_data = get_time_period_analysis(
                rag_engine.raw_data,
                parameters.get("periodo", "day"),
                parameters.get("metric", "calificacion")
            )
        elif query_type == "user_ranking":
            response_data = get_user_rankings(
                rag_engine.raw_data,
                parameters.get("tipo", "general"),
                parameters.get("sucursal"),
                parameters.get("actividad")
            )
        elif query_type == "correlation":
            response_data = get_correlation_analysis(rag_engine.raw_data)
        elif query_type == "general_stats":
            response_data = get_general_stats(rag_engine.raw_data)
        elif query_type == "users_by_branch":
            response_data = get_users_by_branch(
                rag_engine.raw_data,
                parameters.get("sucursal")
            )
        elif query_type == "user_progression":
            response_data = get_user_progression(
                rag_engine.raw_data,
                parameters.get("usuario"),
                parameters.get("metrica", "calificacion")
            )
        elif query_type == "personalized_recommendations":
            response_data = get_personalized_recommendations(
                rag_engine.raw_data,
                parameters.get("usuario")
            )
        elif query_type == "advanced_search":
            response_data = advanced_search(
                rag_engine.raw_data,
                parameters.get("filtros", {})
            )
        else:
            # Por defecto, delegamos la consulta a rag_engine.query()
            logger.debug("USANDO RAG como último recurso para: %s", query)
            response_data = rag_engine.query(query)

        # 6. Actualizar el contexto
        update_context(
            query_type,
            fecha=parameters.get("fecha"),
            usuario=parameters.get("usuario"),
            actividad=parameters.get("actividad"),
            sucursal=parameters.get("sucursal")
        )
        logger.debug("Contexto actualizado: %s", get_last_context())

        # 7. Generar la respuesta final
        logger.info("Generando respuesta para la consulta.")
        return generate_response_func(query, response_data, query_type)

    except ValueError as e:
        if "metric" in str(e).lower() or "tipo" in str(e).lower():
            return ("Lo siento, parece que hay un problema con el tipo de métrica solicitada. "
                    "Puedo proporcionarte rankings por calificación general, por puntos totales "
                    "o por número de actividades. ¿Cuál te interesa?")
        elif "fecha" in str(e).lower():
            return ("No pude interpretar correctamente la fecha mencionada. Por favor, intenta "
                    "especificar la fecha en un formato como 'DD/MM/YYYY' o 'YYYY-MM-DD'.")
        elif "sucursal" in str(e).lower():
            return ("No pude identificar correctamente la sucursal mencionada. Por favor, "
                    "especifica el número de sucursal claramente.")
        elif "usuario" in str(e).lower():
            return ("No pude identificar correctamente al usuario mencionado. Por favor, "
                    "especifica el nombre o ID del usuario claramente.")
        elif "actividad" in str(e).lower():
            return ("No pude identificar correctamente la actividad mencionada. Por favor, "
                    "especifica el nombre de la actividad claramente.")
        return f"Hubo un problema con los datos proporcionados: {str(e)}. Por favor, intenta reformular tu pregunta."
    except KeyError as e:
        return f"Lo siento, no encuentro información sobre {str(e)}. ¿Podrías verificar si el dato es correcto?"
    except IndexError:
        return ("No encontré suficientes datos para responder a tu consulta. "
                "¿Podrías reformularla o ser más específico?")
    except Exception as e:
        logger.error("Error procesando la consulta: %s", str(e), exc_info=True)
        return ("Lo siento, tuve un problema procesando tu consulta. "
                "Intenta reformularla o hacer una pregunta diferente.")
