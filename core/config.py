import os
import logging
from dotenv import load_dotenv

# Carga todas las variables de entorno desde el archivo .env
load_dotenv()

# Configura un logger básico, en caso de que necesites logs
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
)

# Variables de entorno o configuraciones globales
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
DEBUG_MODE = os.getenv("DEBUG_MODE", "False").lower() == "true"

# Puedes agregar más configuraciones generales aquí
# Por ejemplo, si quieres establecer un modelo por defecto de OpenAI:
DEFAULT_OPENAI_MODEL = "gpt-4"

# O algún parámetro de timeout:
OPENAI_TIMEOUT = int(os.getenv("OPENAI_TIMEOUT", "30"))

# Si deseas centralizar rutas
BASE_DATA_PATH = "data/raw/"
FACT_FILE_PATH = os.path.join(BASE_DATA_PATH, "Fact_RolPlay_Sim.xlsx")

# También puedes poner parámetros de persistencia
STORAGE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "storage")

def log_config():
    """
    Muestra la configuración actual por consola. Ideal para depurar.
    """
    logging.info(f"OPENAI_API_KEY: {'***HIDDEN***' if OPENAI_API_KEY else '(not set)'}")
    logging.info(f"DEBUG_MODE: {DEBUG_MODE}")
    logging.info(f"DEFAULT_OPENAI_MODEL: {DEFAULT_OPENAI_MODEL}")
    logging.info(f"OPENAI_TIMEOUT: {OPENAI_TIMEOUT}")
    logging.info(f"FACT_FILE_PATH: {FACT_FILE_PATH}")
    logging.info(f"STORAGE_PATH: {STORAGE_PATH}")
