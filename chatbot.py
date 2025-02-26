import os
import sys
import re
import json
import traceback
import pandas as pd

from datetime import datetime
from llama_index.core import Settings
from llama_index.llms.openai import OpenAI
from openai import OpenAI as ClientOpenAI

# Config y utils
from core.config import (
    OPENAI_API_KEY,
    DEFAULT_OPENAI_MODEL,
    OPENAI_TIMEOUT,
    STORAGE_PATH,
    FACT_FILE_PATH,
    DEBUG_MODE,
    log_config
)

# Este "generate_response" se mantiene en chatbot.py
# Import del text_processing si lo deseas
from core.text_processing import (
    clean_text,
    split_into_sentences,
    pseudo_summarize_text,
    extract_keywords
)

# (NUEVO) Importamos process_query desde core/query_processor
from core.query_processor import process_query

# Import engine RAG
from rag_engine import RolPlayRAG

# Ajusta el path para los queries (por si se necesita localmente)
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "querys"))

# Import queries (para cosas como update_context, get_last_context)
try:
    from querys_Fact_RolPlay_Sim import (
        update_context,
        get_last_context
        # y el resto si es que lo necesitas en este archivo
    )
except ImportError as e:
    print(f"Error importando módulos de queries: {str(e)}")
    raise

# Import prompts
from prompts.conversation_prompt import CONVERSATION_SYSTEM_PROMPT
from prompts.analyst_prompt import (
    ANALYST_SYSTEM_PROMPT_BASE,
    ANALYST_SYSTEM_PROMPT_DATA_ADDITION
)

client = ClientOpenAI(api_key=OPENAI_API_KEY)

conversation_history = []

def generate_response(query: str, data: dict = None, query_type: str = "conversation") -> str:
    """
    Genera una respuesta natural utilizando GPT-4.
    """
    is_asking_for_data = any(keyword in query.lower() for keyword in [
        "qué datos", "que datos", "cuáles datos", "cuales datos",
        "qué información", "que información", "qué registros", "que registros",
        "cómo lo calculaste", "como lo calculaste", "cómo lo obtuviste",
        "como lo obtuviste", "de dónde", "de donde", "qué usaste", "que usaste"
    ])

    if data and isinstance(data, dict) and "error" in data:
        messages = [
            {
                "role": "system",
                "content": """Eres un asistente conversacional experto en análisis de datos.
Responde SIEMPRE en **Markdown** con formato claro:
- Usa **negritas** para conceptos importantes.
- Usa `código` cuando sea necesario.
- Usa listas y tablas cuando sea útil.
- Separa las ideas con saltos de línea dobles (`\\n\\n`) para mayor legibilidad."""
            },
            {
                "role": "user",
                "content": f"Error al procesar la consulta: {data['error']}\nConsulta original: {query}"
            }
        ]
    elif query_type == "conversation":
        messages = [
            {
                "role": "system",
                "content": CONVERSATION_SYSTEM_PROMPT
            },
            {
                "role": "user",
                "content": query
            }
        ]
    else:
        instruction = ANALYST_SYSTEM_PROMPT_BASE
        if is_asking_for_data and "datos_utilizados" in str(data):
            instruction += ANALYST_SYSTEM_PROMPT_DATA_ADDITION

        messages = [
            {
                "role": "system",
                "content": instruction
            },
            {
                "role": "user",
                "content": f"Consulta: {query}\nDatos disponibles: {json.dumps(data, ensure_ascii=False)}"
            }
        ]

    try:
        response = client.chat.completions.create(
            model=DEFAULT_OPENAI_MODEL,
            messages=messages,
            temperature=0.3
        )
        return response.choices[0].message.content

    except Exception as e:
        print(f"Error en generate_response: {str(e)}")
        traceback.print_exc()
        return f"Lo siento, hubo un error al generar la respuesta. Detalles: {str(e)}"

def create_rolplay_analyzer(excel_path: str):
    """
    Crea y configura el analizador RAG a partir de un archivo Excel.
    """
    try:
        df = pd.read_excel(excel_path)
        print("\nEstructura del DataFrame:")
        print("Columnas:", df.columns.tolist())
        
        rag_engine = RolPlayRAG(persist_dir=STORAGE_PATH)
        rag_engine.build_index(df)
        return df, rag_engine
    except Exception as e:
        print(f"Error creando el analizador: {str(e)}")
        traceback.print_exc()
        return None, None

if __name__ == "__main__":
    if DEBUG_MODE:
        log_config()

    excel_path = FACT_FILE_PATH
    try:
        df, rag_engine = create_rolplay_analyzer(excel_path)
        if df is not None and rag_engine is not None:
            print("\n¡Asistente de análisis iniciado!")
            print("Puedes preguntar sobre usuarios, sucursales, actividades, rankings, tendencias y más.")
            print("Algunas consultas que puedes hacer:")
            while True:
                query = input("\nIngresa tu consulta (o 'q' para salir): ")
                if query.lower() == 'q':
                    break
                # Llamamos a process_query y le pasamos generate_response como 3er arg
                response = process_query(rag_engine, query, generate_response)
                print("\nRespuesta:")
                print(response)
                
            print("\n¡Gracias por usar el asistente de análisis!")
    except Exception as e:
        print(f"Error en la inicialización: {str(e)}")
        traceback.print_exc()




