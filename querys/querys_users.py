# querys_users.py

import pandas as pd
import traceback
import re
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from core.config import FACT_FILE_PATH, DEBUG_MODE, log_config
from querys.querys_Fact_RolPlay_Sim import last_context, update_context, get_last_context, handle_error, parse_flexible_date

def get_user_activity_history(raw_data: pd.DataFrame, usuario: str) -> Dict[str, Any]:
    try:
        # Nueva lógica mejorada para coincidencia exacta cuando se trata de números
        if usuario and usuario.isdigit():
            # Si es un número, buscar coincidencia exacta con "userXX" o "Representante XX"
            mask = (raw_data['Usuario'] == f"user{usuario}") | \
                   (raw_data['Usuario Nombre'] == f"Representante {usuario}")
        else:
            # Mantener la búsqueda con contains para casos no numéricos
            mask = (raw_data['Usuario'].astype(str).str.contains(usuario, case=False, na=False)) | \
                   (raw_data['Usuario Nombre'].str.contains(usuario, case=False, na=False))
                   
        user_data = raw_data[mask].sort_values('Fecha_y_Hora')
        
        if len(user_data) == 0:
            return {"message": f"No se encontró al usuario {usuario}", "data": None}
            
        recent_activities = user_data.tail(5)
        
        # Preparar todos los datos utilizados para el cálculo
        datos_utilizados = [{
            "fecha": pd.to_datetime(row['Fecha_y_Hora']).strftime('%d/%m/%y %H:%M'),
            "actividad": row['Actividad_Nombre'],
            "calificacion": float(row['Calificacion']),
            "puntos": float(row['Puntos_Totales']),
            "sucursal": "Sin asignar" if pd.isna(row['Sucursal']) else str(row['Sucursal']),
            "caso_uso": row['Caso_de_Uso_Nombre'] if 'Caso_de_Uso_Nombre' in row else None
        } for _, row in user_data.iterrows()]
        
        # Calcular estadísticas por sucursal (filtrando valores nulos)
        sucursal_valida = user_data['Sucursal'].notna()
        stats_por_sucursal = user_data[sucursal_valida].groupby('Sucursal').agg({
            'Calificacion': ['mean', 'count'],
            'Puntos_Totales': 'sum'
        }).round(2)
        
        # Crear una entrada adicional para actividades sin sucursal asignada
        actividades_sin_sucursal = len(user_data) - len(user_data[sucursal_valida])
        
        stats_sucursales = [{
            "sucursal": str(suc),
            "promedio": float(stats['Calificacion']['mean']),
            "actividades": int(stats['Calificacion']['count']),
            "puntos": float(stats['Puntos_Totales']['sum'])
        } for suc, stats in stats_por_sucursal.iterrows()]
        
        # Añadir info sobre actividades sin sucursal si hay alguna
        if actividades_sin_sucursal > 0:
            # Calculamos métricas para las actividades sin sucursal
            sin_sucursal_data = user_data[~sucursal_valida]
            stats_sucursales.append({
                "sucursal": "Sin asignar",
                "promedio": float(sin_sucursal_data['Calificacion'].mean().round(2)) if len(sin_sucursal_data) > 0 else 0.0,
                "actividades": actividades_sin_sucursal,
                "puntos": float(sin_sucursal_data['Puntos_Totales'].sum()) if len(sin_sucursal_data) > 0 else 0.0
            })

        # Lista de sucursales para mostrar en el resumen (incluyendo "Sin asignar" si hay actividades sin sucursal)
        sucursales_lista = list(user_data['Sucursal'].dropna().unique())
        if actividades_sin_sucursal > 0:
            sucursales_lista.append("Sin asignar")

        return {
            "message": f"Usuario encontrado con {len(user_data)} actividades",
            "data": {
                "usuario": user_data.iloc[0]['Usuario'],
                "nombre": user_data.iloc[0]['Usuario Nombre'],
                "resumen": {
                    "total_actividades": len(user_data),
                    "promedio_calificacion": round(float(user_data['Calificacion'].mean()), 2),
                    "mejor_calificacion": float(user_data['Calificacion'].max()),
                    "peor_calificacion": float(user_data['Calificacion'].min()),
                    "total_puntos": float(user_data['Puntos_Totales'].sum()),
                    "sucursales_distintas": len(sucursales_lista),
                    "sucursales": [str(s) for s in sucursales_lista]  # Lista explícita de sucursales
                },
                "ultimas_actividades": [{
                    "fecha": pd.to_datetime(row['Fecha_y_Hora']).strftime('%d/%m/%y %H:%M'),
                    "actividad": row['Actividad_Nombre'],
                    "calificacion": float(row['Calificacion']),
                    "puntos": float(row['Puntos_Totales']),
                    "sucursal": "Sin asignar" if pd.isna(row['Sucursal']) else str(row['Sucursal'])
                } for _, row in recent_activities.iterrows()],
                "estadisticas_sucursales": stats_sucursales,
                "datos_utilizados": datos_utilizados,
                "rango_fechas": {
                    "primera_actividad": pd.to_datetime(user_data['Fecha_y_Hora'].min()).strftime('%d/%m/%y %H:%M'),
                    "ultima_actividad": pd.to_datetime(user_data['Fecha_y_Hora'].max()).strftime('%d/%m/%y %H:%M')
                }
            }
        }
    except Exception as e:
        return handle_error(e, "get_user_activity_history")
    
def get_user_progression(raw_data: pd.DataFrame, usuario: str, metrica: str = 'calificacion') -> Dict[str, Any]:
    """Analiza la progresión de un usuario a lo largo del tiempo"""
    try:
        # Agregar prints para depuración
        print(f"DEBUG: get_user_progression iniciando con usuario={usuario}, metrica={metrica}")
        
        # Validar parámetros de entrada
        if usuario is None:
            print("DEBUG: Error - usuario es None")
            return {"message": "Se requiere especificar un usuario para analizar su progresión", "data": None}
            
        # Asegurar que metrica siempre tenga un valor válido
        if metrica is None:
            print("DEBUG: metrica es None, usando valor predeterminado 'calificacion'")
            metrica = 'calificacion'
        
        # Mejora en la búsqueda de usuario por número
        if usuario.isdigit():
            # Si es solo un número, buscar coincidencia exacta con el nombre o ID
            usuario_pattern = f"Representante {usuario}$"  # $ asegura que coincida con el final
            mask = raw_data['Usuario Nombre'].str.match(usuario_pattern, case=False, na=False) | \
                   (raw_data['Usuario'] == f"user{usuario}")
            print(f"DEBUG: Búsqueda por número: usuario={usuario}, patrón={usuario_pattern}")
        else:
            # Para otros patrones, usar contains
            mask = (raw_data['Usuario'].astype(str).str.contains(usuario, case=False, na=False)) | \
                   (raw_data['Usuario Nombre'].str.contains(usuario, case=False, na=False))
            print(f"DEBUG: Búsqueda por texto: usuario={usuario}")
        
        user_data = raw_data[mask].sort_values('Fecha_y_Hora')
        print(f"DEBUG: Encontradas {len(user_data)} actividades para el usuario")
        
        # Imprimir para depuración
        if len(user_data) > 0:
            print(f"DEBUG: Usuario encontrado: {user_data['Usuario Nombre'].iloc[0]}")
        
        if len(user_data) == 0:
            return {"message": f"No se encontró al usuario {usuario}", "data": None}
        
        if len(user_data) < 3:
            return {"message": f"Datos insuficientes para analizar progresión. Se requieren al menos 3 actividades, pero el usuario {usuario} solo tiene {len(user_data)}.", "data": None}
            
        # Preparar series temporal
        user_data['fecha'] = pd.to_datetime(user_data['Fecha_y_Hora'])
        
        # Agrupar por semana
        weekly_data = user_data.groupby(pd.Grouper(key='fecha', freq='W')).agg({
            'Calificacion': ['mean', 'max', 'count'],
            'Puntos_Totales': 'sum'
        }).round(2)
        
        weekly_data.columns = ['calif_mean', 'calif_max', 'actividades', 'puntos']
        weekly_data = weekly_data.reset_index()
        print(f"DEBUG: Datos semanales agrupados: {len(weekly_data)} semanas")
        
        # Calcular métrica de progresión
        if metrica == 'calificacion':
            metric_col = 'calif_mean'
        elif metrica == 'puntos':
            metric_col = 'puntos'
        else:
            print(f"DEBUG: Métrica '{metrica}' no reconocida, usando 'calificacion' como predeterminada")
            metric_col = 'calif_mean'
            
        print(f"DEBUG: Usando métrica: {metrica} (columna: {metric_col})")
            
        # Calcular tendencia (regresión lineal simple)
        x_vals = list(range(len(weekly_data)))  # Convertir a lista en lugar de range
        y_vals = weekly_data[metric_col].values.tolist()  # Convertir a lista también
        
        print(f"DEBUG: Valores para regresión: x={x_vals}, y={y_vals}")
        
        if len(x_vals) < 2:
            slope = 0
            print("DEBUG: No hay suficientes puntos para calcular la pendiente")
        else:
            # Calcular pendiente (m en y = mx + b) con valores explícitos para evitar problemas de tipo
            n = len(x_vals)
            # Calcular los productos y sumas manualmente
            x_times_y = sum(x_vals[i] * y_vals[i] for i in range(n))
            sum_x = sum(x_vals)
            sum_y = sum(y_vals)
            sum_x_squared = sum(x**2 for x in x_vals)
            
            print(f"DEBUG: Cálculos para pendiente: n={n}, x_times_y={x_times_y}, sum_x={sum_x}, sum_y={sum_y}, sum_x_squared={sum_x_squared}")
            
            # Aplicar la fórmula de la pendiente
            if (n * sum_x_squared - sum_x**2) != 0:  # Evitar división por cero
                slope = (n * x_times_y - sum_x * sum_y) / (n * sum_x_squared - sum_x**2)
                print(f"DEBUG: Pendiente calculada: {slope}")
            else:
                slope = 0
                print("DEBUG: Denominador es cero, usando pendiente=0")
        
        # Evitar división por cero en mejora porcentual
        mejora_porcentual = 0.0
        if weekly_data[metric_col].iloc[0] != 0:
            mejora_porcentual = float(((weekly_data[metric_col].iloc[-1] / weekly_data[metric_col].iloc[0]) - 1) * 100)
            print(f"DEBUG: Mejora porcentual calculada: {mejora_porcentual}%")
        else:
            print("DEBUG: Primera semana en 0, no se puede calcular mejora porcentual")
            
        progreso = {
            "tendencia": "positiva" if slope > 0 else "negativa",
            "velocidad": "rápida" if abs(slope) > 2 else "moderada" if abs(slope) > 0.5 else "lenta",
            "valor_pendiente": float(slope),
            "primera_semana": float(weekly_data[metric_col].iloc[0]),
            "ultima_semana": float(weekly_data[metric_col].iloc[-1]),
            "mejora_porcentual": mejora_porcentual
        }
        
        # Filtrar por último mes si es posible
        current_date = pd.to_datetime(user_data['fecha'].max())
        one_month_ago = current_date - pd.DateOffset(months=1)
        
        last_month_data = user_data[user_data['fecha'] >= one_month_ago]
        
        has_last_month_data = len(last_month_data) > 0
        print(f"DEBUG: Datos del último mes: {len(last_month_data)} actividades")
        
        # Agrega información específica del último mes
        if has_last_month_data:
            progreso["ultimo_mes"] = {
                "actividades": len(last_month_data),
                "promedio": float(last_month_data['Calificacion'].mean().round(2)),
                "mejor": float(last_month_data['Calificacion'].max()),
                "peor": float(last_month_data['Calificacion'].min()),
                "fecha_inicio": one_month_ago.strftime('%d/%m/%y'),
                "fecha_fin": current_date.strftime('%d/%m/%y')
            }
        
        print("DEBUG: Análisis completado exitosamente")
        return {
            "message": f"Análisis de progresión para {user_data.iloc[0]['Usuario Nombre']}",
            "data": {
                "usuario": user_data.iloc[0]['Usuario'],
                "nombre": user_data.iloc[0]['Usuario Nombre'],
                "progresion": progreso,
                "metrica_analizada": metrica,  # Añadido para claridad
                "datos_semanales": [{
                    "semana": row['fecha'].strftime('%d/%m/%y'),
                    "promedio": float(row['calif_mean']),
                    "maximo": float(row['calif_max']),
                    "actividades": int(row['actividades']),
                    "puntos": float(row['puntos'])
                } for _, row in weekly_data.iterrows()]
            }
        }
    except Exception as e:
        print(f"ERROR en get_user_progression: {str(e)}")
        traceback.print_exc()
        return handle_error(e, "get_user_progression")
    

def get_user_rankings(raw_data: pd.DataFrame, tipo: str = 'general', sucursal: str = None, 
                     actividad: str = None, order: str = "desc", min_activities: int = 1) -> Dict[str, Any]:
    try:
        # MARCA DISTINTIVA PARA CONFIRMAR QUE SE ESTÁ EJECUTANDO LA VERSIÓN MEJORADA
        print("******** FUNCIÓN GET_USER_RANKINGS VERSIÓN MEJORADA EJECUTÁNDOSE ********")
        
        # NUEVOS LOGS: Mostrar información detallada sobre las calificaciones mínimas individuales
        print(f"DATOS RAW: {len(raw_data)} filas")
        
        # Mostrar estadísticas generales
        print(f"Estadísticas generales de calificación:")
        print(f"- Mínima: {raw_data['Calificacion'].min()}")
        print(f"- Máxima: {raw_data['Calificacion'].max()}")
        print(f"- Promedio: {raw_data['Calificacion'].mean().round(2)}")
        
        # NUEVO: Mostrar top 5 usuarios con peores calificaciones individuales
        peores_individuales = raw_data.nsmallest(5, 'Calificacion')
        print("\nTop 5 usuarios con PEORES CALIFICACIONES INDIVIDUALES (antes de agrupar):")
        for idx, row in peores_individuales.iterrows():
            print(f"  - Usuario: {row['Usuario']}, Nombre: {row['Usuario Nombre']}, "
                  f"Calificación: {row['Calificacion']}, Actividad: {row['Actividad_Nombre']}")
                  
        # NUEVO: Mostrar top 5 usuarios con mejores calificaciones individuales
        mejores_individuales = raw_data.nlargest(5, 'Calificacion')
        print("\nTop 5 usuarios con MEJORES CALIFICACIONES INDIVIDUALES (antes de agrupar):")
        for idx, row in mejores_individuales.iterrows():
            print(f"  - Usuario: {row['Usuario']}, Nombre: {row['Usuario Nombre']}, "
                  f"Calificación: {row['Calificacion']}, Actividad: {row['Actividad_Nombre']}")
        
        # Aplicar filtros iniciales
        data = raw_data.copy()
        if sucursal:
            print(f"\nFiltrando por sucursal: {sucursal}")
            data = data[data['Sucursal'].astype(str) == str(sucursal)]
            print(f"  - Quedan {len(data)} filas después del filtro")
            
        if actividad:
            print(f"\nFiltrando por actividad: {actividad}")
            data = data[data['Actividad_Nombre'] == actividad]
            print(f"  - Quedan {len(data)} filas después del filtro")
        
        print(f"\nTrabajando con {len(data)} registros después de filtros iniciales")
        
        # PROCESO DE AGRUPACIÓN Y CÁLCULO DE MÉTRICAS POR USUARIO
        print("\nAgrupando datos por usuario para calcular promedios...")
        g = data.groupby(['Usuario','Usuario Nombre']).agg({
            'Calificacion': ['mean','max','min','std','count'],
            'Puntos_Totales': 'sum',
            'Sucursal': 'nunique',
            'Actividad_Nombre': 'nunique'
        }).round(2)
        
        g.columns = [
            'calif_mean','calif_max','calif_min','calif_std','calif_count',
            'puntos_totales','sucursales','actividades_diferentes'
        ]
        
        print(f"Después de agrupar: {len(g)} usuarios únicos")
        
        # NUEVO: Mostrar usuarios con el PEOR PROMEDIO de calificación
        if len(g) > 0:
            min_calif_mean = g['calif_mean'].min()
            print(f"\nUsuarios con PEOR PROMEDIO DE CALIFICACIÓN ({min_calif_mean}):")
            # Convertir a DataFrame normal para facilitar manipulación
            g_reset = g.reset_index()
            usuarios_peor_promedio = g_reset[g_reset['calif_mean'] == min_calif_mean]
            for idx, row in usuarios_peor_promedio.iterrows():
                print(f"  - Usuario: {row['Usuario']}, Nombre: {row['Usuario Nombre']}, "
                      f"Promedio: {row['calif_mean']}, Calificación Mínima: {row['calif_min']}, "
                      f"Calificación Máxima: {row['calif_max']}, Actividades: {row['calif_count']}")
        
        # Aplicar filtro de actividades mínimas después de agrupar
        if min_activities > 1:
            print(f"\nFiltrando usuarios con al menos {min_activities} actividades")
            usuarios_antes = len(g)
            g = g[g['calif_count'] >= min_activities]
            print(f"  - {usuarios_antes - len(g)} usuarios excluidos por tener menos de {min_activities} actividades")
            print(f"  - Quedan {len(g)} usuarios")
            
        # Verificar que hay datos después del filtrado
        if len(g) == 0:
            return {"message": "No se encontraron usuarios que cumplan con los criterios", "data": []}
            
        # Determinar qué métrica usar para el ranking
        if tipo == 'general':
            metric = 'calif_mean'
            print(f"\nUsando PROMEDIO DE CALIFICACIÓN como métrica para el ranking")
        elif tipo == 'puntos':
            metric = 'puntos_totales'
            print(f"\nUsando PUNTOS TOTALES como métrica para el ranking")
        elif tipo == 'actividades':
            metric = 'calif_count'
            print(f"\nUsando CANTIDAD DE ACTIVIDADES como métrica para el ranking")
        else:
            raise ValueError(f"Tipo de ranking no válido: {tipo}")

        # NUEVO: Mostrar información sobre la métrica seleccionada
        print(f"Información sobre la métrica '{metric}':")
        print(f"  - Valor mínimo: {g[metric].min()}")
        print(f"  - Valor máximo: {g[metric].max()}")
        print(f"  - Promedio: {g[metric].mean().round(2)}")
        
        # NUEVO: Mostrar usuarios con el valor mínimo de la métrica seleccionada
        min_metric_value = g[metric].min()
        g_reset = g.reset_index()
        min_metric_users = g_reset[g_reset[metric] == min_metric_value]
        print(f"\nUsuarios con el VALOR MÍNIMO de la métrica '{metric}' ({min_metric_value}):")
        for idx, row in min_metric_users.iterrows():
            print(f"  - Usuario: {row['Usuario']}, Nombre: {row['Usuario Nombre']}, "
                  f"Valor: {row[metric]}, Actividades: {row['calif_count']}")
                  
        # Ordenar y obtener los top N usuarios según la métrica
        print(f"\nOrdenando usuarios por '{metric}' en orden {'ASCENDENTE' if order.lower() == 'asc' else 'DESCENDENTE'}")
        if order.lower() == "asc":
            # PEORES primero (valores más bajos)
            top_users = g.sort_values(by=metric, ascending=True).head(10)
            print("  - Mostrando primero los PEORES usuarios (valores más bajos)")
        else:
            # MEJORES primero (valores más altos)
            top_users = g.sort_values(by=metric, ascending=False).head(10)
            print("  - Mostrando primero los MEJORES usuarios (valores más altos)")

        # NUEVO: Mostrar detalles de los usuarios seleccionados por la ordenación
        print(f"\nDetalles de los {len(top_users)} usuarios seleccionados por orden:")
        top_reset = top_users.reset_index()
        for i, (_, row) in enumerate(top_reset.iterrows(), 1):
            print(f"  {i}. Usuario: {row['Usuario']}, Nombre: {row['Usuario Nombre']}, "
                  f"{metric}: {row[metric]}, Actividades: {row['calif_count']}")
        
        # Crear un ranking adecuado
        top_users = top_users.reset_index()
        top_users['rank'] = top_users[metric].rank(method='min', ascending=(order.lower() == "asc"))
        
        # Contar cuántos usuarios comparten cada posición
        rank_counts = top_users.groupby('rank').size().to_dict()
        
        # Construir el resultado final
        result = []
        for _, row in top_users.iterrows():
            result.append({
                "posicion": int(row['rank']),
                "usuarios_misma_posicion": rank_counts[row['rank']],
                "usuario": row['Usuario'],
                "nombre": row['Usuario Nombre'],
                "promedio": float(row['calif_mean']),
                "mejor_calif": float(row['calif_max']),
                "peor_calif": float(row['calif_min']),
                "total_actividades": int(row['calif_count']),
                "puntos_totales": float(row['puntos_totales']),
                "sucursales": int(row['sucursales']),
                "actividades_diferentes": int(row['actividades_diferentes']),
                "valor_metrica": float(row[metric])
            })
            
        # Asegurarse de que el resultado esté en el orden correcto
        result = sorted(result, key=lambda x: x["posicion"])
        
        # NUEVO: Añadir información sobre el usuario con la peor calificación individual
        # para asegurar que esta información llegue al modelo de lenguaje
        peor_individual = {
            "usuario": peores_individuales.iloc[0]['Usuario'],
            "nombre": peores_individuales.iloc[0]['Usuario Nombre'],
            "calificacion": float(peores_individuales.iloc[0]['Calificacion']),
            "actividad": peores_individuales.iloc[0]['Actividad_Nombre']
        }
        
        return {
            "message": f"Ranking de usuarios por {tipo} ({'ascendente' if order == 'asc' else 'descendente'})",
            "data": result,
            "metrica_utilizada": metric,
            "usuarios_excluidos": len(g[g['calif_count'] < min_activities]) if min_activities > 1 else 0,
            "min_actividades": min_activities,
            "peor_calificacion_individual": peor_individual  # Añadido para asegurar que se vea
        }
    except Exception as e:
        print(f"Error en get_user_rankings: {str(e)}")
        traceback.print_exc()
        return handle_error(e, "get_user_rankings")
    

def get_users_by_branch(raw_data: pd.DataFrame, sucursal: str) -> Dict[str, Any]:
    """Obtiene la lista de usuarios de una sucursal específica con sus métricas principales"""
    try:
        # Para depuración: ver los formatos de sucursales
        print(f"Buscando sucursal: '{sucursal}'")
        print(f"Valores únicos de sucursales que contienen '{sucursal}': {raw_data['Sucursal'].astype(str).str.contains(str(sucursal), case=False, na=False).sum()}")
        
        # Comprobar si el valor ya incluye el prefijo "Sucursal"
        if "sucursal" in str(sucursal).lower():
            search_term = str(sucursal)
        else:
            search_term = f"Sucursal {sucursal}"
            
        print(f"Término de búsqueda final: '{search_term}'")
        
        # Usar comparación exacta con el término correcto
        mask = raw_data['Sucursal'].astype(str) == search_term
        branch_data = raw_data[mask]
        
        print(f"Registros encontrados para '{search_term}': {len(branch_data)}")
        
        if len(branch_data) == 0:
            return {"message": f"No se encontró la sucursal {sucursal}", "data": None}
            
        user_stats = branch_data.groupby(['Usuario', 'Usuario Nombre']).agg({
            'Calificacion': ['mean', 'max', 'count'],
            'Puntos_Totales': 'sum',
            'Actividad_Nombre': 'nunique'
        }).reset_index()
        
        # Reorganizar columnas multi-índice
        user_stats.columns = [
            'usuario', 'nombre', 'promedio_calificacion', 'mejor_calificacion', 
            'total_actividades', 'puntos_totales', 'actividades_diferentes'
        ]
        
        # Convertir a tipo de datos apropiados
        user_stats['promedio_calificacion'] = user_stats['promedio_calificacion'].round(2).astype(float)
        user_stats['mejor_calificacion'] = user_stats['mejor_calificacion'].astype(float)
        user_stats['total_actividades'] = user_stats['total_actividades'].astype(int)
        user_stats['puntos_totales'] = user_stats['puntos_totales'].astype(float)
        user_stats['actividades_diferentes'] = user_stats['actividades_diferentes'].astype(int)
        
        # Para depuración
        print(f"Usuarios únicos encontrados: {len(user_stats)}")
        print(f"Lista de usuarios: {user_stats['usuario'].tolist()}")
        
        return {
            "message": f"Usuarios encontrados en sucursal {sucursal}",
            "data": {
                "sucursal": sucursal,
                "total_usuarios": len(user_stats),
                "usuarios": user_stats.to_dict(orient='records'),
                "resumen": {
                    "promedio_general": float(branch_data['Calificacion'].mean().round(2)),
                    "total_actividades": len(branch_data),
                    "periodo": {
                        "inicio": pd.to_datetime(branch_data['Fecha_y_Hora'].min()).strftime('%d/%m/%y'),
                        "fin": pd.to_datetime(branch_data['Fecha_y_Hora'].max()).strftime('%d/%m/%y')
                    }
                }
            }
        }
    except Exception as e:
        return handle_error(e, "get_users_by_branch")

def get_personalized_recommendations(raw_data: pd.DataFrame, usuario: str) -> Dict[str, Any]:
    """Genera recomendaciones personalizadas para mejorar el desempeño de un usuario"""
    try:
        # Filtrar datos del usuario con lógica mejorada
        if usuario and usuario.isdigit():
            # Si es un número, buscar coincidencia exacta con "userXX" o "Representante XX"
            mask = (raw_data['Usuario'] == f"user{usuario}") | \
                   (raw_data['Usuario Nombre'] == f"Representante {usuario}")
        else:
            # Mantener la búsqueda con contains para casos no numéricos
            mask = (raw_data['Usuario'].astype(str).str.contains(usuario, case=False, na=False)) | \
                   (raw_data['Usuario Nombre'].str.contains(usuario, case=False, na=False))
        
        user_data = raw_data[mask]
        
        if len(user_data) == 0:
            return {"message": f"No se encontró al usuario {usuario}", "data": None}
            
        # Obtener perfil básico del usuario
        user_profile = {
            "usuario": user_data.iloc[0]['Usuario'],
            "nombre": user_data.iloc[0]['Usuario Nombre'],
            "promedio_general": float(user_data['Calificacion'].mean().round(2)),
            "actividades_completadas": len(user_data),
            "fortalezas": [],
            "areas_mejora": [],
            "recomendaciones": []
        }
        
        # Identificar fortalezas (actividades con mejor desempeño)
        if len(user_data) >= 3:
            # Actividades con al menos 2 intentos
            activity_stats = user_data.groupby('Actividad_Nombre').agg({
                'Calificacion': ['mean', 'count']
            }).reset_index()
            activity_stats.columns = ['actividad', 'promedio', 'intentos']
            activity_stats = activity_stats[activity_stats['intentos'] >= 2]
            
            if len(activity_stats) > 0:
                # Top 3 actividades con mejor promedio
                fortalezas = activity_stats.nlargest(3, 'promedio')
                user_profile["fortalezas"] = [{
                    "actividad": row['actividad'],
                    "calificacion": float(row['promedio'].round(2)),
                    "intentos": int(row['intentos'])
                } for _, row in fortalezas.iterrows()]
                
                # Actividades con peor desempeño
                areas_mejora = activity_stats.nsmallest(3, 'promedio')
                user_profile["areas_mejora"] = [{
                    "actividad": row['actividad'],
                    "calificacion": float(row['promedio'].round(2)),
                    "intentos": int(row['intentos'])
                } for _, row in areas_mejora.iterrows()]
        
        # Analizar patrones de tiempo
        user_data['fecha'] = pd.to_datetime(user_data['Fecha_y_Hora'])
        user_data['hora'] = user_data['fecha'].dt.hour
        
        # Mejores horas para el usuario
        if len(user_data) >= 5:
            user_hours = user_data.groupby('hora')['Calificacion'].agg(['mean', 'count'])
            user_hours = user_hours[user_hours['count'] >= 2]
            if len(user_hours) > 0:
                best_hour = user_hours['mean'].idxmax()
                user_profile["recomendaciones"].append({
                    "tipo": "horario",
                    "recomendacion": f"Realizar actividades alrededor de las {best_hour}:00 horas",
                    "razon": "Históricamente muestras mejor rendimiento en ese horario",
                    "impacto_estimado": "Medio"
                })
        
        # Recomendar actividades populares que el usuario no ha intentado
        global_best_activities = raw_data.groupby('Actividad_Nombre')['Calificacion'].mean().nlargest(10)
        user_activities = set(user_data['Actividad_Nombre'].unique())
        recommended_activities = [act for act in global_best_activities.index if act not in user_activities][:3]
        
        if recommended_activities:
            user_profile["recomendaciones"].append({
                "tipo": "nuevas_actividades",
                "actividades": recommended_activities,
                "razon": "Actividades populares con altas calificaciones promedio",
                "impacto_estimado": "Alto"
            })
        
        # Analizar sucursales de mejor rendimiento para el usuario
        if len(user_data['Sucursal'].unique()) > 1:
            branch_performance = user_data.groupby('Sucursal')['Calificacion'].mean()
            best_branch = str(branch_performance.idxmax())
            user_profile["recomendaciones"].append({
                "tipo": "sucursal",
                "recomendacion": f"Priorizar actividades en Sucursal {best_branch}",
                "razon": f"Tu rendimiento es superior en esta sucursal (Promedio: {branch_performance.max().round(2)})",
                "impacto_estimado": "Medio-Alto"
            })
        
        return {
            "message": f"Recomendaciones personalizadas para {user_profile['nombre']}",
            "data": user_profile
        }
    except Exception as e:
        return handle_error(e, "get_personalized_recommendations")



def advanced_search(raw_data: pd.DataFrame, filtros: Dict[str, Any]) -> Dict[str, Any]:
    """Realiza una búsqueda avanzada con múltiples filtros"""
    try:
        data = raw_data.copy()
        filtros_aplicados = {}
        
        # Procesar sucursal
        if 'sucursal' in filtros and filtros['sucursal']:
            data = data[data['Sucursal'].astype(str).str.contains(str(filtros['sucursal']), case=False, na=False)]
            filtros_aplicados['sucursal'] = str(filtros['sucursal'])
            
        # Procesar usuario
        if 'usuario' in filtros and filtros['usuario']:
            usuario = filtros['usuario']
            mask = (data['Usuario'].astype(str).str.contains(usuario, case=False, na=False)) | \
                   (data['Usuario Nombre'].str.contains(usuario, case=False, na=False))
            data = data[mask]
            filtros_aplicados['usuario'] = usuario
            
        # Procesar actividad
        if 'actividad' in filtros and filtros['actividad']:
            data = data[data['Actividad_Nombre'].str.contains(filtros['actividad'], case=False, na=False)]
            filtros_aplicados['actividad'] = filtros['actividad']
            
        # Procesar fecha
        if 'fecha_inicio' in filtros and filtros['fecha_inicio']:
            fecha_inicio = parse_flexible_date(filtros['fecha_inicio'])
            data = data[pd.to_datetime(data['Fecha_y_Hora']) >= fecha_inicio]
            filtros_aplicados['fecha_inicio'] = fecha_inicio.strftime('%d/%m/%Y')
            
        if 'fecha_fin' in filtros and filtros['fecha_fin']:
            fecha_fin = parse_flexible_date(filtros['fecha_fin'])
            data = data[pd.to_datetime(data['Fecha_y_Hora']) <= fecha_fin]
            filtros_aplicados['fecha_fin'] = fecha_fin.strftime('%d/%m/%Y')
            
        # Procesar calificación
        if 'calif_min' in filtros and filtros['calif_min'] is not None:
            data = data[data['Calificacion'] >= filtros['calif_min']]
            filtros_aplicados['calif_min'] = float(filtros['calif_min'])
            
        if 'calif_max' in filtros and filtros['calif_max'] is not None:
            data = data[data['Calificacion'] <= filtros['calif_max']]
            filtros_aplicados['calif_max'] = float(filtros['calif_max'])
            
        # Procesar puntos
        if 'puntos_min' in filtros and filtros['puntos_min'] is not None:
            data = data[data['Puntos_Totales'] >= filtros['puntos_min']]
            filtros_aplicados['puntos_min'] = float(filtros['puntos_min'])
            
        if 'puntos_max' in filtros and filtros['puntos_max'] is not None:
            data = data[data['Puntos_Totales'] <= filtros['puntos_max']]
            filtros_aplicados['puntos_max'] = float(filtros['puntos_max'])
        
        if len(data) == 0:
            return {
                "message": "No se encontraron resultados para los filtros especificados",
                "data": {
                    "filtros_aplicados": filtros_aplicados,
                    "resultados": []
                }
            }
            
        # Limitar resultados    
        limit = filtros.get('limit', 20)
        resultados = [{
            "fecha": pd.to_datetime(row['Fecha_y_Hora']).strftime('%d/%m/%Y %H:%M'),
            "usuario": row['Usuario'],
            "nombre": row['Usuario Nombre'],
            "actividad": row['Actividad_Nombre'],
            "calificacion": float(row['Calificacion']),
            "puntos": float(row['Puntos_Totales']),
            "sucursal": str(row['Sucursal'])
        } for idx, row in data.head(limit).iterrows()]
        
        # Estadísticas de resultados
        stats = {
            "total_resultados": len(data),
            "promedio_calificacion": float(data['Calificacion'].mean().round(2)),
            "calificacion_max": float(data['Calificacion'].max()),
            "calificacion_min": float(data['Calificacion'].min()),
            "usuarios_unicos": int(data['Usuario'].nunique()),
            "actividades_unicas": int(data['Actividad_Nombre'].nunique()),
            "sucursales": int(data['Sucursal'].nunique())
        }
        
        return {
            "message": f"Se encontraron {len(data)} resultados (mostrando {len(resultados)})",
            "data": {
                "filtros_aplicados": filtros_aplicados,
                "estadisticas": stats,
                "resultados": resultados
            }
        }
    except Exception as e:
        return handle_error(e, "advanced_search")



def get_general_stats(raw_data: pd.DataFrame) -> Dict[str, Any]:
    try:
        total_sucursales = len(raw_data['Sucursal'].dropna().unique())
        total_usuarios = len(raw_data['Usuario'].dropna().unique())
        total_usuarios_activos = total_usuarios  # Todos los usuarios son activos
        total_actividades = len(raw_data[raw_data['Sucursal'].notna()])
        promedio_general = float(raw_data['Calificacion'].mean().round(2))
        mejor_calificacion = float(raw_data['Calificacion'].max())
        peor_calificacion = float(raw_data['Calificacion'].min())
        total_puntos = float(raw_data['Puntos_Totales'].sum())
        return {
            "message": "Estadísticas generales",
            "data": {
                "total_sucursales": total_sucursales,
                "total_usuarios": total_usuarios,
                "total_usuarios_activos": total_usuarios_activos,  # Añadido campo explícito
                "total_actividades": total_actividades,
                "promedio_general": promedio_general,
                "mejor_calificacion_global": mejor_calificacion,
                "peor_calificacion_global": peor_calificacion,
                "total_puntos": total_puntos
            }
        }
    except Exception as e:
        return handle_error(e, "get_general_stats")
    

    
def get_exact_activity_result(raw_data: pd.DataFrame, fecha: str, actividad: str = None) -> Dict[str, Any]:
    try:
        # Manejar casos especiales de fechas relativas
        if fecha and isinstance(fecha, str):
            fecha_lower = fecha.lower()
            
            # Manejar caso de "primera" fecha
            if "primera" in fecha_lower or "primer" in fecha_lower or "inicial" in fecha_lower:
                # Convertir a datetime para ordenar
                raw_data['fecha_dt'] = pd.to_datetime(raw_data['Fecha_y_Hora'])
                
                # Filtrar por actividad si se especifica
                filtered_data = raw_data
                if actividad:
                    filtered_data = raw_data[raw_data['Actividad_Nombre'].str.contains(actividad, case=False, na=False)]
                
                if len(filtered_data) == 0:
                    return {"message": f"No se encontraron actividades para {actividad if actividad else 'ninguna actividad'}", "data": None}
                
                # Obtener el registro más antiguo
                result = filtered_data.loc[filtered_data['fecha_dt'].idxmin()].to_frame().T
                
                actividades = [{
                    "fecha": pd.to_datetime(result['Fecha_y_Hora'].iloc[0]).strftime('%d/%m/%y %H:%M'),
                    "hora": pd.to_datetime(result['Fecha_y_Hora'].iloc[0]).strftime('%H:%M'),
                    "actividad": result['Actividad_Nombre'].iloc[0],
                    "usuario": result['Usuario'].iloc[0],
                    "calificacion": float(result['Calificacion'].iloc[0]),
                    "puntos": float(result['Puntos_Totales'].iloc[0]),
                    "sucursal": str(result['Sucursal'].iloc[0])
                }]
                
                update_context("specific_date", fecha=fecha, actividad=actividad)
                return {
                    "message": f"Primera actividad encontrada ({actividades[0]['fecha']})",
                    "data": actividades[0]
                }
            
            # Manejar caso de "última" fecha
            elif "ultima" in fecha_lower or "última" in fecha_lower or "reciente" in fecha_lower:
                # Convertir a datetime para ordenar
                raw_data['fecha_dt'] = pd.to_datetime(raw_data['Fecha_y_Hora'])
                
                # Filtrar por actividad si se especifica
                filtered_data = raw_data
                if actividad:
                    filtered_data = raw_data[raw_data['Actividad_Nombre'].str.contains(actividad, case=False, na=False)]
                
                if len(filtered_data) == 0:
                    return {"message": f"No se encontraron actividades para {actividad if actividad else 'ninguna actividad'}", "data": None}
                
                # Obtener el registro más reciente
                result = filtered_data.loc[filtered_data['fecha_dt'].idxmax()].to_frame().T
                
                actividades = [{
                    "fecha": pd.to_datetime(result['Fecha_y_Hora'].iloc[0]).strftime('%d/%m/%y %H:%M'),
                    "hora": pd.to_datetime(result['Fecha_y_Hora'].iloc[0]).strftime('%H:%M'),
                    "actividad": result['Actividad_Nombre'].iloc[0],
                    "usuario": result['Usuario'].iloc[0],
                    "calificacion": float(result['Calificacion'].iloc[0]),
                    "puntos": float(result['Puntos_Totales'].iloc[0]),
                    "sucursal": str(result['Sucursal'].iloc[0])
                }]
                
                update_context("specific_date", fecha=fecha, actividad=actividad)
                return {
                    "message": f"Última actividad encontrada ({actividades[0]['fecha']})",
                    "data": actividades[0]
                }
        
        # Comportamiento original para fechas específicas
        fecha_dt = parse_flexible_date(fecha)
        if fecha_dt.hour == 0 and fecha_dt.minute == 0:
            mask = pd.to_datetime(raw_data['Fecha_y_Hora']).dt.date == fecha_dt.date()
        else:
            fecha_inicio = fecha_dt - timedelta(minutes=5)
            fecha_fin = fecha_dt + timedelta(minutes=5)
            mask = (pd.to_datetime(raw_data['Fecha_y_Hora']) >= fecha_inicio) & \
                   (pd.to_datetime(raw_data['Fecha_y_Hora']) <= fecha_fin)
        if actividad:
            mask &= raw_data['Actividad_Nombre'].str.contains(actividad, case=False, na=False)
        result = raw_data[mask]
        if len(result) == 0:
            update_context("specific_date", fecha=fecha, actividad=actividad)
            return {"message": f"No se encontraron actividades para {fecha_dt.strftime('%d/%m/%y %H:%M')}", "data": None}
        actividades = [{
            "hora": pd.to_datetime(row['Fecha_y_Hora']).strftime('%H:%M'),
            "actividad": row['Actividad_Nombre'],
            "usuario": row['Usuario'],
            "calificacion": float(row['Calificacion']),
            "puntos": float(row['Puntos_Totales']),
            "sucursal": str(row['Sucursal'])
        } for _, row in result.iterrows()]
        if len(actividades) > 1:
            resumen = {
                "total_actividades": len(actividades),
                "promedio_calificacion": round(result['Calificacion'].mean(), 2),
                "mejor_calificacion": float(result['Calificacion'].max()),
                "usuarios_unicos": len(result['Usuario'].unique())
            }
            response_data = {
                "message": f"Encontradas {len(actividades)} actividades",
                "data": {
                    "actividades": actividades,
                    "resumen": resumen
                }
            }
        else:
            response_data = {
                "message": "Actividad encontrada",
                "data": actividades[0]
            }
        update_context("specific_date", fecha=fecha, actividad=actividad)
        return response_data
    except Exception as e:
        return handle_error(e, "get_exact_activity_result")