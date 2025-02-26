# querys_activities.py

import pandas as pd
import traceback
import re
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

# Importa (o define) las mismas utilidades que usabas antes
from core.config import FACT_FILE_PATH, DEBUG_MODE, log_config
from querys.querys_Fact_RolPlay_Sim import last_context, update_context, get_last_context, handle_error, parse_flexible_date

def get_activity_stats(raw_data: pd.DataFrame, actividad: str) -> Dict[str, Any]:
    try:
        # Validar que 'actividad' sea una cadena no vacía
        if not actividad or not isinstance(actividad, str) or actividad.strip() == "":
            return {
                "message": "No se proporcionó una actividad válida. Por favor, especifica una actividad.",
                "data": None
            }
        
        mask = raw_data['Actividad_Nombre'].str.contains(actividad, case=False, na=False)
        activity_data = raw_data[mask]
        
        if len(activity_data) == 0:
            return {"message": f"No se encontró la actividad {actividad}", "data": None}
        
        recent_attempts = activity_data.sort_values('Fecha_y_Hora').tail(5)
        
        # Preparar los datos utilizados para el cálculo
        datos_utilizados = [{
            "fecha": pd.to_datetime(row['Fecha_y_Hora']).strftime('%d/%m/%y %H:%M'),
            "usuario": row['Usuario'],
            "actividad": row['Actividad_Nombre'],
            "calificacion": float(row['Calificacion']),
            "puntos": float(row['Puntos_Totales']),
            "sucursal": str(row['Sucursal'])
        } for _, row in activity_data.iterrows()]
        
        return {
            "message": "Actividad encontrada",
            "data": {
                "actividad": activity_data.iloc[0]['Actividad_Nombre'],
                "estadisticas": {
                    "total_intentos": len(activity_data),
                    "promedio_calificacion": round(float(activity_data['Calificacion'].mean()), 2),
                    "mejor_calificacion": float(activity_data['Calificacion'].max()),
                    "peor_calificacion": float(activity_data['Calificacion'].min())
                },
                "ultimos_intentos": [{
                    "fecha": pd.to_datetime(row['Fecha_y_Hora']).strftime('%d/%m/%y %H:%M'),
                    "usuario": row['Usuario'],
                    "calificacion": float(row['Calificacion'])
                } for _, row in recent_attempts.iterrows()],
                "datos_utilizados": datos_utilizados,
                "rango_fechas": {
                    "inicio": pd.to_datetime(activity_data['Fecha_y_Hora'].min()).strftime('%d/%m/%y %H:%M'),
                    "fin": pd.to_datetime(activity_data['Fecha_y_Hora'].max()).strftime('%d/%m/%y %H:%M')
                }
            }
        }
    except Exception as e:
        return handle_error(e, "get_activity_stats")


    
def get_activity_rankings(raw_data: pd.DataFrame) -> Dict[str, Any]:
    try:
        stats_por_actividad = []
        for act in raw_data['Actividad_Nombre'].unique():
            data_actividad = raw_data[raw_data['Actividad_Nombre'] == act]
            stats_por_actividad.append({
                "actividad": act,
                "promedio_calificacion": float(data_actividad['Calificacion'].mean().round(2)),
                "mejor_calificacion": float(data_actividad['Calificacion'].max()),
                "peor_calificacion": float(data_actividad['Calificacion'].min()),
                "total_intentos": len(data_actividad),
                "usuarios_unicos": len(data_actividad['Usuario'].unique()),
                "sucursales": len(data_actividad['Sucursal'].unique())
            })
        mejores = sorted(stats_por_actividad, key=lambda x: x["promedio_calificacion"], reverse=True)[:5]
        peores = sorted(stats_por_actividad, key=lambda x: x["promedio_calificacion"])[:5]
        return {
            "message": "Rankings de actividades generados",
            "data": {
                "mas_exitosas": mejores,
                "mas_desafiantes": peores,
                "total_actividades": len(stats_por_actividad)
            }
        }
    except Exception as e:
        return handle_error(e, "get_activity_rankings")
    
    
def get_branch_performance(raw_data: pd.DataFrame, sucursal: str) -> Dict[str, Any]:
    try:
        mask = raw_data['Sucursal'].astype(str).str.contains(str(sucursal), case=False, na=False)
        branch_data = raw_data[mask]
        if len(branch_data) == 0:
            return {"message": f"No se encontró la sucursal {sucursal}", "data": None}
        recent_data = branch_data.sort_values('Fecha_y_Hora').tail(10)
        datos_utilizados = [{
            "fecha": pd.to_datetime(row['Fecha_y_Hora']).strftime('%d/%m/%y %H:%M'),
            "usuario": row['Usuario'],
            "actividad": row['Actividad_Nombre'],
            "calificacion": float(row['Calificacion']),
            "puntos": float(row['Puntos_Totales'])
        } for _, row in branch_data.iterrows()]
        return {
            "message": "Sucursal encontrada",
            "data": {
                "sucursal": sucursal,
                "metricas": {
                    "total_actividades": len(branch_data),
                    "usuarios_activos": len(branch_data['Usuario'].unique()),
                    "promedio_calificacion": round(float(branch_data['Calificacion'].mean()), 2),
                    "mejor_calificacion": float(branch_data['Calificacion'].max())
                },
                "actividades_recientes": [{
                    "fecha": pd.to_datetime(row['Fecha_y_Hora']).strftime('%d/%m/%y %H:%M'),
                    "usuario": row['Usuario'],
                    "actividad": row['Actividad_Nombre'],
                    "calificacion": float(row['Calificacion'])
                } for _, row in recent_data.iterrows()],
                "datos_utilizados": datos_utilizados
            }
        }
    except Exception as e:
        return handle_error(e, "get_branch_performance")
    
    
def get_branch_rankings(raw_data: pd.DataFrame) -> Dict[str, Any]:
    try:
        stats_por_sucursal = []
        for suc in raw_data['Sucursal'].dropna().unique():
            data_sucursal = raw_data[raw_data['Sucursal'] == suc]
            calif_mean = float(data_sucursal['Calificacion'].mean().round(2))
            stats_por_sucursal.append({
                "sucursal": str(suc),
                "promedio_calificacion": calif_mean,
                "total_actividades": len(data_sucursal),
                "usuarios_unicos": len(data_sucursal['Usuario'].unique()),
                "puntos_totales": float(data_sucursal['Puntos_Totales'].sum()),
                "mejor_calificacion": float(data_sucursal['Calificacion'].max()),
                "peor_calificacion": float(data_sucursal['Calificacion'].min())
            })
        
        # Crear lista ordenada para mejores y peores por calificación
        sucursales_ordenadas = [s for s in stats_por_sucursal if not pd.isna(s["promedio_calificacion"])]
        mejores = sorted(sucursales_ordenadas, key=lambda x: x["promedio_calificacion"], reverse=True)[:5]
        peores = sorted(sucursales_ordenadas, key=lambda x: x["promedio_calificacion"])[:5]
        
        rankings = {
            "por_calificacion": mejores,
            "por_calificacion_peores": peores,
            "por_actividad": sorted(
                stats_por_sucursal,
                key=lambda x: x["total_actividades"],
                reverse=True
            )[:5],
            "por_puntos": sorted(
                stats_por_sucursal,
                key=lambda x: x["puntos_totales"],
                reverse=True
            )[:5]
        }
        
        mejor_sucursal = max(
            [s for s in stats_por_sucursal if not pd.isna(s["promedio_calificacion"])],
            key=lambda x: x["promedio_calificacion"]
        )
        
        peor_sucursal = min(
            [s for s in stats_por_sucursal if not pd.isna(s["promedio_calificacion"])],
            key=lambda x: x["promedio_calificacion"]
        )
        
        return {
            "message": "Rankings de sucursales generados",
            "data": {
                "rankings": rankings,
                "mejor_sucursal": mejor_sucursal,
                "peor_sucursal": peor_sucursal,
                "total_sucursales": len(stats_por_sucursal)
            }
        }
    except Exception as e:
        return handle_error(e, "get_branch_rankings")


def get_time_period_analysis(raw_data: pd.DataFrame, periodo: str = 'day', metrica: str = 'calificacion') -> Dict[str, Any]:
    try:
        data = raw_data.copy()
        data['fecha'] = pd.to_datetime(data['Fecha_y_Hora'])
        if periodo == 'hour':
            grouper = data['fecha'].dt.hour
        elif periodo == 'day':
            grouper = data['fecha'].dt.date
        elif periodo == 'week':
            grouper = data['fecha'].dt.isocalendar().week
        else:
            grouper = data['fecha'].dt.to_period('M')
        if metrica == 'calificacion':
            metric_col = 'Calificacion'
        elif metrica == 'puntos':
            metric_col = 'Puntos_Totales'
        else:
            raise ValueError(f"Métrica no válida: {metrica}")
        df = data.groupby(grouper).agg({
            metric_col: ['mean','max','min','std','count'],
            'Usuario': 'nunique',
            'Sucursal': 'nunique',
            'Actividad_Nombre': 'nunique'
        }).round(2)
        df.columns = [
            f"{metric_col}_mean",f"{metric_col}_max",f"{metric_col}_min",f"{metric_col}_std",f"{metric_col}_count",
            "usuarios_unicos","sucursales","actividades_diferentes"
        ]
        mejores_periodos = df.nlargest(3, f"{metric_col}_mean")
        peores_periodos = df.nsmallest(3, f"{metric_col}_mean")
        return {
            "message": f"Análisis por {periodo} completado",
            "data": {
                "mejores_periodos": [{
                    "periodo": str(k),
                    "promedio": float(v[f"{metric_col}_mean"]),
                    "max": float(v[f"{metric_col}_max"]),
                    "actividades": int(v[f"{metric_col}_count"]),
                    "usuarios": int(v["usuarios_unicos"]),
                    "sucursales": int(v["sucursales"])
                } for k, v in mejores_periodos.iterrows()],
                "peores_periodos": [{
                    "periodo": str(k),
                    "promedio": float(v[f"{metric_col}_mean"]),
                    "min": float(v[f"{metric_col}_min"]),
                    "actividades": int(v[f"{metric_col}_count"]),
                    "usuarios": int(v["usuarios_unicos"]),
                    "sucursales": int(v["sucursales"])
                } for k, v in peores_periodos.iterrows()]
            }
        }
    except Exception as e:
        return handle_error(e, "get_time_period_analysis")
    
def get_trend_analysis(raw_data: pd.DataFrame, usuario: str = None, actividad: str = None, sucursal: str = None, periodo: str = 'day') -> Dict[str, Any]:
    try:
        data = raw_data.copy()
        if usuario:
            mask = (data['Usuario'].astype(str).str.contains(usuario, case=False, na=False)) | \
                   (data['Usuario Nombre'].str.contains(usuario, case=False, na=False))
            data = data[mask]
        if actividad:
            data = data[data['Actividad_Nombre'].str.contains(actividad, case=False, na=False)]
        if sucursal:
            data = data[data['Sucursal'].astype(str).str.contains(str(sucursal), case=False, na=False)]
        if len(data) == 0:
            return {"message": "No se encontraron datos para analizar", "data": None}
        data['fecha'] = pd.to_datetime(data['Fecha_y_Hora'])
        if periodo == 'hour':
            grouper = data['fecha'].dt.floor('H')
        elif periodo == 'day':
            grouper = data['fecha'].dt.date
        elif periodo == 'week':
            grouper = data['fecha'].dt.isocalendar().week
        else:
            grouper = data['fecha'].dt.to_period('M')
        g = data.groupby(grouper).agg({
            'Usuario': 'nunique',
            'Actividad_Nombre': 'count',
            'Calificacion': ['mean','max','min','std'],
            'Puntos_Totales': 'sum'
        }).round(2)
        g.columns = ['usuarios','actividades','calif_mean','calif_max','calif_min','calif_std','puntos_totales']
        tendencia_valor = data['Calificacion'].corr(pd.to_numeric(data.index))
        direccion_tendencia = "positiva" if tendencia_valor > 0 else "negativa"
        return {
            "message": f"Análisis de tendencias por {periodo} completado",
            "data": {
                "tendencias": g.to_dict(),
                "metricas_generales": {
                    "total_registros": len(data),
                    "promedio_general": float(data['Calificacion'].mean()),
                    "tendencia": direccion_tendencia
                },
                "periodo_analizado": {
                    "inicio": data['Fecha_y_Hora'].min(),
                    "fin": data['Fecha_y_Hora'].max()
                }
            }
        }
    except Exception as e:
        return handle_error(e, "get_trend_analysis")


def get_comparative_analysis(raw_data: pd.DataFrame, usuarios: List[str] = None, fechas: List[str] = None, actividad: str = None) -> Dict[str, Any]:
    try:
        data = raw_data.copy()
        if actividad:
            data = data[data['Actividad_Nombre'].str.contains(actividad, case=False, na=False)]
        if usuarios:
            mask = data['Usuario'].astype(str).isin(usuarios) | \
                   data['Usuario Nombre'].str.contains('|'.join(usuarios), case=False, na=False)
            data = data[mask]
        if fechas:
            fechas_dt = [parse_flexible_date(f) for f in fechas]
            mask = pd.to_datetime(data['Fecha_y_Hora']).dt.date.isin([f.date() for f in fechas_dt])
            data = data[mask]
        if len(data) == 0:
            return {"message": "No se encontraron datos para comparar", "data": None}
        g = data.groupby('Usuario').agg({
            'Calificacion': ['count','mean','max','min'],
            'Puntos_Totales':'sum'
        }).round(2)
        g.columns = ['calif_count','calif_mean','calif_max','calif_min','puntos_totales']
        if fechas:
            t = data.groupby(pd.to_datetime(data['Fecha_y_Hora']).dt.date).agg({
                'Usuario':'nunique',
                'Calificacion':'mean',
                'Puntos_Totales':'sum'
            }).round(2)
            analisis_temporal = t.to_dict()
        else:
            analisis_temporal = None
        fecha_inicio = data['Fecha_y_Hora'].min()
        fecha_fin = data['Fecha_y_Hora'].max()
        return {
            "message": "Análisis comparativo completado",
            "data": {
                "usuarios": g.to_dict(),
                "temporal": analisis_temporal,
                "total_registros": len(data),
                "periodo_analizado": {
                    "inicio": fecha_inicio.strftime('%Y-%m-%d %H:%M:%S') if fecha_inicio is not None else None,
                    "fin": fecha_fin.strftime('%Y-%m-%d %H:%M:%S') if fecha_fin is not None else None
                }
            }
        }
    except Exception as e:
        return handle_error(e, "get_comparative_analysis")
    
def get_correlation_analysis(raw_data: pd.DataFrame) -> Dict[str, Any]:
    try:
        data = raw_data.copy()
        data['fecha'] = pd.to_datetime(data['Fecha_y_Hora'])
        data['hora'] = data['fecha'].dt.hour
        numeric_cols = ['Calificacion','Puntos_Totales','hora']
        correlations = data[numeric_cols].corr().round(3)
        categorical_analysis = {
            "por_sucursal": data.groupby('Sucursal')['Calificacion'].mean().round(2).to_dict(),
            "por_actividad": data.groupby('Actividad_Nombre')['Calificacion'].mean().round(2).to_dict(),
            "por_hora": data.groupby(data['fecha'].dt.hour)['Calificacion'].mean().round(2).to_dict()
        }
        return {
            "message": "Análisis de correlaciones completado",
            "data": {
                "correlaciones": correlations.to_dict(),
                "analisis_categorico": categorical_analysis,
                "insights": {
                    "mejor_hora": int(max(categorical_analysis["por_hora"].items(), key=lambda x: x[1])[0]),
                    "mejor_sucursal": str(max(categorical_analysis["por_sucursal"].items(), key=lambda x: x[1])[0]),
                    "mejor_actividad": str(max(categorical_analysis["por_actividad"].items(), key=lambda x: x[1])[0])
                }
            }
        }
    except Exception as e:
        return handle_error(e, "get_correlation_analysis")


def get_branch_stats(raw_data: pd.DataFrame) -> Dict[str, Any]:
    try:
        sucursales = raw_data['Sucursal'].dropna().unique()
        stats_por_sucursal = []
        for suc in sucursales:
            data_sucursal = raw_data[raw_data['Sucursal'] == suc]
            calif_mean = float(data_sucursal['Calificacion'].mean())
            stats_por_sucursal.append({
                "sucursal": str(suc),
                "total_actividades": len(data_sucursal),
                "usuarios_unicos": len(data_sucursal['Usuario'].unique()),
                "promedio_calificacion": calif_mean,
                "puntos_totales": float(data_sucursal['Puntos_Totales'].sum())
            })
        total_actividades = sum(s["total_actividades"] for s in stats_por_sucursal)
        total_usuarios = len(raw_data['Usuario'].unique())
        promedio_general = float(raw_data['Calificacion'].mean())
        mejor_por_calificacion = max(stats_por_sucursal, key=lambda x: x["promedio_calificacion"])
        mejor_por_actividad = max(stats_por_sucursal, key=lambda x: x["total_actividades"])
        mejor_por_puntos = max(stats_por_sucursal, key=lambda x: x["puntos_totales"])
        return {
            "message": f"Análisis de {len(sucursales)} sucursales",
            "data": {
                "estadisticas_globales": {
                    "total_sucursales": len(sucursales),
                    "total_actividades": total_actividades,
                    "total_usuarios": total_usuarios,
                    "promedio_calificacion": promedio_general
                },
                "mejores_sucursales": {
                    "por_calificacion": mejor_por_calificacion,
                    "por_actividad": mejor_por_actividad,
                    "por_puntos": mejor_por_puntos
                },
                "stats_por_sucursal": stats_por_sucursal
            }
        }
    except Exception as e:
        return handle_error(e, "get_branch_stats")

def get_top_performances(raw_data: pd.DataFrame, n: int = 5, metric: str = "calificacion", filtros: Dict[str, Any] = None) -> Dict[str, Any]:
    try:
        data = raw_data.copy()
        
        # Aplicar filtros si se han especificado
        if filtros:
            for columna, valor in filtros.items():
                if valor:
                    if columna == 'fecha':
                        fecha_dt = parse_flexible_date(valor)
                        data = data[pd.to_datetime(data['Fecha_y_Hora']).dt.date == fecha_dt.date()]
                    else:
                        data = data[data[columna].astype(str).str.contains(str(valor), case=False, na=False)]
        
        # Determinar la métrica para ordenar
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
        
        # Obtener el top N
        top_n = data.nlargest(n, ordenar_por)
        
        # Preparar la lista completa de datos utilizados
        datos_utilizados = [{
            "fecha": pd.to_datetime(row['Fecha_y_Hora']).strftime('%d/%m/%y %H:%M'),
            "usuario": row['Usuario'],
            "actividad": row['Actividad_Nombre'],
            "calificacion": float(row['Calificacion']),
            "puntos": float(row['Puntos_Totales']),
            "sucursal": str(row['Sucursal']),
            "valor_metrica": float(row[ordenar_por]) if ordenar_por in row else None
        } for _, row in data.iterrows()]
        
        # Preparar los resultados principales
        resultados = [{
            "fecha": pd.to_datetime(row['Fecha_y_Hora']).strftime('%d/%m/%y %H:%M'),
            "usuario": row['Usuario'],
            "actividad": row['Actividad_Nombre'],
            "calificacion": float(row['Calificacion']),
            "puntos": float(row['Puntos_Totales']),
            "sucursal": str(row['Sucursal']),
            "valor_metrica": float(row[ordenar_por]) if ordenar_por in row else None
        } for _, row in top_n.iterrows()]
        
        # Calcular estadísticas adicionales
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

