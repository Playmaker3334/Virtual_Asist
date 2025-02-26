import re
import unicodedata
from typing import List

def clean_text(text: str) -> str:
    """
    Limpia y normaliza el texto dado:
    1. Convierte a 'NFKD' y elimina caracteres diacríticos (tildes, eñes con tildes, etc.).
    2. Pasa a minúsculas.
    3. Quita caracteres no alfanuméricos (excepto espacios y signos de puntuación básicos).
    4. Reemplaza múltiples espacios por uno solo.
    Retorna el texto limpio.
    """
    # Normalizar
    text_nfkd = unicodedata.normalize("NFKD", text)
    # Eliminar diacríticos
    text_without_accents = "".join([c for c in text_nfkd if not unicodedata.combining(c)])
    # Pasar a minúsculas
    text_lower = text_without_accents.lower()
    # Quitar caracteres no deseados (ejemplo simplificado)
    text_alphanumeric = re.sub(r"[^a-z0-9áéíóúüñ .,;:!?¿¡()-]", "", text_lower)
    # Reemplazar múltiples espacios por uno
    text_single_space = re.sub(r"\s+", " ", text_alphanumeric)
    return text_single_space.strip()


def split_into_sentences(text: str) -> List[str]:
    """
    Divide el texto en oraciones utilizando reglas simples de puntuación.
    Retorna una lista con las oraciones separadas.
    """
    # Usar un delimitador de oraciones básico (puntos, signos de exclamación, etc.).
    # Esto se puede mejorar con librerías más avanzadas (por ej. nltk, spacy, etc.).
    sentences = re.split(r'(?<=[.?!])\s+', text)
    
    # Limpiar espacios y filtrar cadenas vacías
    sentences = [s.strip() for s in sentences if s.strip()]
    return sentences


def pseudo_summarize_text(text: str, max_sentences: int = 3) -> str:
    """
    Resume un texto de forma muy básica, tomando la primer parte (oraciones iniciales).
    Esto es un ejemplo 'tonto' de resumen. 
    En la vida real, podrías llamar a GPT-4 o un modelo 'summarize' de tu preferencia.
    """
    # Simplemente cortamos el texto a X oraciones (ejemplo didáctico)
    sentences = split_into_sentences(text)
    summarized = sentences[:max_sentences]
    return " ".join(summarized)


def extract_keywords(text: str, min_length: int = 3) -> List[str]:
    """
    Extrae palabras clave simples basadas en la frecuencia de aparición (bag of words).
    - Limpia el texto
    - Lo separa por espacios
    - Hace un conteo de palabras 
    - Devuelve las más frecuentes que tengan una longitud >= min_length
    """
    cleaned = clean_text(text)
    tokens = cleaned.split()
    
    # Contar frecuencia
    freq = {}
    for token in tokens:
        if len(token) >= min_length:  # sólo palabras "significativas"
            freq[token] = freq.get(token, 0) + 1
    
    # Ordenar por frecuencia descendente
    sorted_words = sorted(freq.items(), key=lambda x: x[1], reverse=True)
    
    # Tomar top 10 palabras más frecuentes (por ejemplo)
    top_10 = sorted_words[:10]
    
    # Devolver solo las palabras
    return [word for word, _ in top_10]
