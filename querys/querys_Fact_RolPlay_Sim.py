import pandas as pd
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import traceback
from core.config import FACT_FILE_PATH, DEBUG_MODE, log_config
import re

last_context = {
    "fecha": None,
    "usuario": None,
    "actividad": None,
    "sucursal": None,
    "tipo_consulta": None
}

def handle_error(e: Exception, context: str) -> Dict[str, Any]:
    if isinstance(e, ValueError):
        return {"error": f"Error de valor en {context}: {str(e)}", "data": None}
    elif isinstance(e, KeyError):
        return {"error": f"Error de columna no encontrada en {context}: {str(e)}", "data": None}
    else:
        return {"error": f"Error inesperado en {context}: {str(e)}", "data": None}

def update_context(tipo: str, **kwargs):
    global last_context
    last_context["tipo_consulta"] = tipo
    for key, value in kwargs.items():
        if value is not None:
            last_context[key] = value

def get_last_context() -> Dict[str, Any]:
    return last_context

def parse_flexible_date(fecha_str: str) -> pd.Timestamp:
    formatos = [
        '%d/%m/%y %H:%M','%d/%m/%Y %H:%M','%Y-%m-%d %H:%M:%S','%d/%m/%y','%d/%m/%Y',
        '%Y-%m-%d','%d-%m-%Y %H:%M','%Y/%m/%d %H:%M','%d.%m.%Y %H:%M'
    ]
    for formato in formatos:
        try:
            return pd.to_datetime(fecha_str, format=formato)
        except:
            continue
    try:
        fecha_str = fecha_str.lower()
        fecha_str = re.sub(r'del?', '', fecha_str)
        fecha_str = re.sub(r'año', '', fecha_str)
        fecha_str = re.sub(r'a las', '', fecha_str)
        fecha_str = fecha_str.strip()
        if 'pm' in fecha_str or 'p.m' in fecha_str:
            fecha_str = re.sub(r'pm|p.m', '', fecha_str).strip()
            hora_obj = pd.to_datetime(fecha_str)
            if hora_obj.hour < 12:
                hora_obj += timedelta(hours=12)
            return hora_obj
        else:
            fecha_str = re.sub(r'am|a.m', '', fecha_str).strip()
            return pd.to_datetime(fecha_str)
    except Exception as e:
        raise ValueError(f"No se pudo interpretar la fecha: {fecha_str}")









def get_time_analysis(raw_data: pd.DataFrame, periodo: str = 'day') -> Dict[str, Any]:
    try:
        raw_data['fecha'] = pd.to_datetime(raw_data['Fecha_y_Hora'])
        if periodo == 'day':
            grouper = raw_data['fecha'].dt.date
        elif periodo == 'week':
            grouper = raw_data['fecha'].dt.isocalendar().week
        else:
            grouper = raw_data['fecha'].dt.to_period('M')
        analisis = raw_data.groupby(grouper).agg({
            'Usuario': 'nunique',
            'Actividad_Nombre': 'count',
            'Calificacion': 'mean',
            'Puntos_Totales': 'sum'
        }).reset_index()
        ultimos_periodos = analisis.tail(5)
        return {
            "message": f"Análisis por {periodo} completado",
            "data": [{
                "periodo": str(row['fecha']),
                "usuarios": int(row['Usuario']),
                "actividades": int(row['Actividad_Nombre']),
                "promedio_calificacion": round(float(row['Calificacion']), 2)
            } for _, row in ultimos_periodos.iterrows()]
        }
    except Exception as e:
        return handle_error(e, "get_time_analysis")

def search_activities(raw_data: pd.DataFrame, texto: str) -> Dict[str, Any]:
    try:
        mask = (
            raw_data['Actividad_Nombre'].str.contains(texto, case=False, na=False) |
            raw_data['Usuario'].astype(str).str.contains(texto, case=False, na=False) |
            raw_data['Usuario Nombre'].str.contains(texto, case=False, na=False) |
            raw_data['Sucursal'].astype(str).str.contains(texto, case=False, na=False)
        )
        results = raw_data[mask].sort_values('Fecha_y_Hora', ascending=False).head(10)
        if len(results) == 0:
            return {"message": f"No se encontraron resultados para '{texto}'", "data": None}
        return {
            "message": f"Se encontraron {len(results)} resultados",
            "data": [{
                "fecha": pd.to_datetime(row['Fecha_y_Hora']).strftime('%d/%m/%y %H:%M'),
                "actividad": row['Actividad_Nombre'],
                "usuario": row['Usuario'],
                "sucursal": str(row['Sucursal']),
                "calificacion": float(row['Calificacion'])
            } for _, row in results.iterrows()]
        }
    except Exception as e:
        return handle_error(e, "search_activities")

def get_top_performances(raw_data: pd.DataFrame, n: int = 5, metric: str = "calificacion", filtros: Dict[str, Any] = None) -> Dict[str, Any]:
    try:
        data = raw_data.copy()
        
        # Aplicar filtros si existen
        if filtros:
            for columna, valor in filtros.items():
                if valor:
                    if columna == 'fecha':
                        fecha_dt = parse_flexible_date(valor)
                        data = data[pd.to_datetime(data['Fecha_y_Hora']).dt.date == fecha_dt.date()]
                    else:
                        data = data[data[columna].astype(str).str.contains(str(valor), case=False, na=False)]
        
        # Determinar métrica de ordenamiento
        if metric == "calificacion":
            ordenar_por = "Calificacion"
        elif metric == "puntos":
            ordenar_por = "Puntos_Totales"
        elif metric == "mejora":
            data = data.sort_values('Fecha_y_Hora')
            data['mejora'] = data.groupby('Usuario')['Calificacion'].diff()
            ordenar_por = "mejora"
        else:
            raise ValueError(f"Métrica no válida: {metric}")
        
        # Obtener top N
        top_n = data.nlargest(n, ordenar_por)
        
        # Preparar datos utilizados completos
        datos_utilizados = [{
            "fecha": pd.to_datetime(row['Fecha_y_Hora']).strftime('%d/%m/%y %H:%M'),
            "usuario": row['Usuario'],
            "actividad": row['Actividad_Nombre'],
            "calificacion": float(row['Calificacion']),
            "puntos": float(row['Puntos_Totales']),
            "sucursal": str(row['Sucursal']),
            "valor_metrica": float(row[ordenar_por]) if ordenar_por in row else None
        } for _, row in data.iterrows()]
        
        # Resultados principales
        resultados = [{
            "fecha": pd.to_datetime(row['Fecha_y_Hora']).strftime('%d/%m/%y %H:%M'),
            "usuario": row['Usuario'],
            "actividad": row['Actividad_Nombre'],
            "calificacion": float(row['Calificacion']),
            "puntos": float(row['Puntos_Totales']),
            "sucursal": str(row['Sucursal']),
            "valor_metrica": float(row[ordenar_por]) if ordenar_por in row else None
        } for _, row in top_n.iterrows()]
        
        # Estadísticas adicionales
        stats = {
            "promedio_general": float(data[ordenar_por].mean()),
            "mediana": float(data[ordenar_por].median()),
            "desviacion_estandar": float(data[ordenar_por].std())
        }
        
        return {
            "message": f"Top {n} mejores desempeños por {metric}",
            "data": {
                "resultados": resultados,
                "metrica": metric,
                "total_registros": len(data),
                "estadisticas": stats,
                "datos_utilizados": datos_utilizados,
                "filtros_aplicados": filtros,
                "rango_fechas": {
                    "inicio": pd.to_datetime(data['Fecha_y_Hora'].min()).strftime('%d/%m/%y %H:%M'),
                    "fin": pd.to_datetime(data['Fecha_y_Hora'].max()).strftime('%d/%m/%y %H:%M')
                }
            }
        }
    except Exception as e:
        return handle_error(e, "get_top_performances")





def get_activity_success_factors(raw_data: pd.DataFrame) -> Dict[str, Any]:
    """Analiza factores correlacionados con el éxito en las actividades"""
    try:
        data = raw_data.copy()
        # Añadir nuevas variables calculadas
        data['fecha'] = pd.to_datetime(data['Fecha_y_Hora'])
        data['hora_dia'] = data['fecha'].dt.hour
        data['dia_semana'] = data['fecha'].dt.dayofweek
        data['mes'] = data['fecha'].dt.month
        data['experiencia_usuario'] = data.groupby('Usuario').cumcount()
        
        # Correlaciones numéricas
        numeric_vars = ['Calificacion', 'Puntos_Totales', 'hora_dia', 'dia_semana', 'mes', 'experiencia_usuario']
        correlations = data[numeric_vars].corr()['Calificacion'].drop('Calificacion').to_dict()
        
        # Análisis de variables categóricas
        categorical_vars = ['Sucursal', 'Actividad_Nombre']
        categorical_impact = {}
        
        for var in categorical_vars:
            # Obtener promedio global
            global_avg = data['Calificacion'].mean()
            
            # Calcular promedios por categoría
            category_avgs = data.groupby(var)['Calificacion'].agg(['mean', 'count'])
            
            # Filtrar categorías con suficientes datos (mínimo 5 observaciones)
            category_avgs = category_avgs[category_avgs['count'] >= 5]
            
            # Calcular desviación respecto al promedio global
            category_avgs['impact'] = (category_avgs['mean'] - global_avg) / global_avg * 100
            
            # Ordenar por impacto
            top_positive = category_avgs.nlargest(3, 'impact')
            top_negative = category_avgs.nsmallest(3, 'impact')
            
            categorical_impact[var] = {
                "positive": [{
                    "valor": str(idx),
                    "impacto": float(row['impact'].round(2)),
                    "muestras": int(row['count'])
                } for idx, row in top_positive.iterrows()],
                "negative": [{
                    "valor": str(idx),
                    "impacto": float(row['impact'].round(2)),
                    "muestras": int(row['count'])
                } for idx, row in top_negative.iterrows()]
            }
        
        # Obtener mejores horarios del día
        hourly_performance = data.groupby('hora_dia')['Calificacion'].mean().sort_values(ascending=False)
        best_hours = [{
            "hora": int(hour),
            "calificacion": float(score.round(2))
        } for hour, score in hourly_performance.head(3).items()]
        
        # Obtener mejores días de la semana
        weekday_map = {
            0: 'Lunes', 1: 'Martes', 2: 'Miércoles',
            3: 'Jueves', 4: 'Viernes', 5: 'Sábado', 6: 'Domingo'
        }
        weekday_performance = data.groupby('dia_semana')['Calificacion'].mean().sort_values(ascending=False)
        best_days = [{
            "dia": weekday_map[day],
            "calificacion": float(score.round(2))
        } for day, score in weekday_performance.head(3).items()]
        
        return {
            "message": "Análisis de factores de éxito en actividades",
            "data": {
                "correlaciones": correlations,
                "impacto_categorico": categorical_impact,
                "mejor_momento": {
                    "horas": best_hours,
                    "dias": best_days
                },
                "observaciones": len(data)
            }
        }
    except Exception as e:
        return handle_error(e, "get_activity_success_factors")
    





