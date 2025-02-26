from typing import List, Dict, Any
import pandas as pd
import numpy as np
from datetime import datetime
import hashlib
import json
import os
import traceback
from llama_index.core import (
    VectorStoreIndex,
    Document,
    Settings,
    StorageContext,
    load_index_from_storage
)
from llama_index.llms.openai import OpenAI
from llama_index.embeddings.openai import OpenAIEmbedding

class RolPlayRAG:
    def __init__(self, persist_dir: str = "./storage"):
        self.persist_dir = persist_dir
        self.raw_data = None
        self.metadata_path = os.path.join(persist_dir, "index_metadata.json")
        os.makedirs(self.persist_dir, exist_ok=True)
        
        self.llm = OpenAI(model="gpt-4", temperature=0.7)
        Settings.llm = self.llm
        Settings.embed_model = OpenAIEmbedding()
        self.index = None

    def _convert_to_serializable(self, obj):
        """Convierte objetos a formatos serializables"""
        if isinstance(obj, (np.int8, np.int16, np.int32, np.int64)):
            return int(obj)
        elif isinstance(obj, (np.float16, np.float32, np.float64)):
            return float(obj)
        elif isinstance(obj, (np.ndarray,)):
            return obj.tolist()
        elif isinstance(obj, (pd.Timestamp)):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        return obj

    def _calculate_data_hash(self, df: pd.DataFrame) -> str:
        """Calcula un hash del DataFrame para detectar cambios"""
        # Convertimos el DataFrame a una representación consistente
        df_string = df.to_json(orient='records', date_format='iso')
        return hashlib.sha256(df_string.encode()).hexdigest()

    def _save_metadata(self, data_hash: str):
        """Guarda metadata del índice"""
        metadata = {
            "data_hash": data_hash,
            "creation_date": datetime.now().isoformat(),
            "version": "1.0"
        }
        with open(self.metadata_path, 'w') as f:
            json.dump(metadata, f)

    def _load_metadata(self) -> Dict:
        """Carga metadata del índice"""
        if os.path.exists(self.metadata_path):
            with open(self.metadata_path, 'r') as f:
                return json.load(f)
        return {}

    def _should_rebuild_index(self, current_hash: str) -> bool:
        """Determina si el índice debe ser reconstruido"""
        if not os.path.exists(os.path.join(self.persist_dir, "docstore.json")) or \
           not os.path.exists(os.path.join(self.persist_dir, "index_store.json")):
            return True

        metadata = self._load_metadata()
        return metadata.get("data_hash", "") != current_hash

    def _create_detailed_activity_document(self, activity_data: pd.DataFrame) -> Document:
        """Crea un documento detallado para una actividad específica"""
        first_row = activity_data.iloc[0]
        
        content = f"""
        Actividad Específica:
        Nombre: {first_row['Actividad_Nombre']}
        Fecha y Hora: {first_row['Fecha_y_Hora']}
        Caso de Uso: {first_row['Caso_de_Uso_Nombre']}
        Calificación: {first_row['Calificacion']}
        Puntos Totales: {first_row['Puntos_Totales']}
        Usuario: {first_row['Usuario']} ({first_row['Usuario Nombre']})
        Sucursal: {str(first_row['Sucursal'])}
        
        Detalles de Puntuación:
        """
        
        for i in range(1, 11):
            info_correcta = first_row.get(f'Info_Correcta{i}', 'No disponible')
            puntos = first_row.get(f'Puntos{i}', 'No disponible')
            if pd.notna(info_correcta) and pd.notna(puntos) and info_correcta != 'No aplica' and puntos != 'No aplica':
                content += f"- Punto {i}: {info_correcta} (Puntos: {puntos})\n"
        
        metadata = {
            "actividad": str(first_row['Actividad_Nombre']),
            "fecha": str(first_row['Fecha_y_Hora']),
            "usuario": str(first_row['Usuario']),
            "sucursal": str(first_row['Sucursal']),
            "calificacion": self._convert_to_serializable(first_row['Calificacion']),
            "document_type": "activity_detail"
        }
        
        return Document(text=content, metadata=metadata)

    def _create_documents(self, df: pd.DataFrame) -> List[Document]:
        """Crea documentos para indexación"""
        documents = []
        self.raw_data = df.copy()
        
        # Documentos de actividades
        for _, activity in df.iterrows():
            documents.append(self._create_detailed_activity_document(pd.DataFrame([activity])))
        
        # Documentos de usuarios
        for usuario, user_data in df.groupby('Usuario'):
            sucursales = [str(suc) for suc in user_data['Sucursal'].dropna().unique()]
            
            user_metrics = {
                "total_actividades": self._convert_to_serializable(len(user_data)),
                "calificacion_promedio": self._convert_to_serializable(user_data['Calificacion'].mean()),
                "puntos_totales": self._convert_to_serializable(user_data['Puntos_Totales'].sum()),
                "sucursales": sucursales
            }
            
            content = f"""
            Perfil de Usuario: {usuario}
            Nombre Completo: {user_data['Usuario Nombre'].iloc[0]}
            
            Resumen:
            - Total Actividades: {user_metrics['total_actividades']}
            - Calificación Promedio: {user_metrics['calificacion_promedio']:.2f}
            - Puntos Totales: {user_metrics['puntos_totales']}
            - Sucursales: {', '.join(sucursales) if sucursales else 'No especificadas'}
            """
            
            documents.append(Document(
                text=content,
                metadata={
                    "usuario": str(usuario),
                    "metrics": user_metrics,
                    "document_type": "user_summary"
                }
            ))
        
        # Documentos de sucursales
        for sucursal in df['Sucursal'].dropna().unique():
            branch_data = df[df['Sucursal'] == sucursal]
            branch_metrics = {
                "total_usuarios": len(branch_data['Usuario'].unique()),
                "total_actividades": len(branch_data),
                "calificacion_promedio": self._convert_to_serializable(branch_data['Calificacion'].mean()),
                "puntos_totales": self._convert_to_serializable(branch_data['Puntos_Totales'].sum())
            }
            
            content = f"""
            Análisis de Sucursal: {sucursal}
            
            Métricas:
            - Total Usuarios: {branch_metrics['total_usuarios']}
            - Total Actividades: {branch_metrics['total_actividades']}
            - Calificación Promedio: {branch_metrics['calificacion_promedio']:.2f}
            - Puntos Totales: {branch_metrics['puntos_totales']}
            """
            
            documents.append(Document(
                text=content,
                metadata={
                    "sucursal": str(sucursal),
                    "metrics": branch_metrics,
                    "document_type": "branch_summary"
                }
            ))
        
        # NUEVO: Documento de estadísticas generales
        summary_stats = {
            "total_users": len(df['Usuario'].unique()),
            "total_branches": len(df['Sucursal'].dropna().unique()),
            "total_activities": len(df),
            "avg_score": float(df['Calificacion'].mean().round(2)),
            "date_range": [
                pd.to_datetime(df['Fecha_y_Hora'].min()).strftime('%Y-%m-%d'),
                pd.to_datetime(df['Fecha_y_Hora'].max()).strftime('%Y-%m-%d')
            ]
        }
        
        content = f"""
        Resumen General del Dataset Educativo:
        
        Este dataset contiene información sobre actividades educativas con {summary_stats['total_users']} usuarios
        distribuidos en {summary_stats['total_branches']} sucursales, con un total de {summary_stats['total_activities']} 
        actividades registradas entre {summary_stats['date_range'][0]} y {summary_stats['date_range'][1]}.
        
        La calificación promedio general es {summary_stats['avg_score']}, con actividades
        que abarcan diferentes tipos y niveles de dificultad.
        
        Los datos incluyen información sobre usuarios, sus actividades, calificaciones,
        puntos obtenidos y las sucursales donde se realizaron.
        """
        
        documents.append(Document(
            text=content,
            metadata={
                "document_type": "general_summary",
                "metrics": summary_stats
            }
        ))
        
        # NUEVO: Documento de correlaciones principales
        try:
            # Crear dataframe con hora del día
            temp_df = df.copy()
            temp_df['fecha'] = pd.to_datetime(temp_df['Fecha_y_Hora'])
            temp_df['hora'] = temp_df['fecha'].dt.hour
            
            # Calcular correlaciones
            numeric_cols = ['Calificacion', 'Puntos_Totales', 'hora']
            correlations = temp_df[numeric_cols].corr().round(3)
            
            content = f"""
            Principales Correlaciones en el Dataset:
            
            Correlación entre Calificación y Puntos: {correlations.loc['Calificacion', 'Puntos_Totales']}
            Correlación entre Calificación y Hora del día: {correlations.loc['Calificacion', 'hora']}
            
            La hora del día con mejor rendimiento promedio: {temp_df.groupby('hora')['Calificacion'].mean().idxmax()}
            """
            
            documents.append(Document(
                text=content,
                metadata={
                    "document_type": "correlation_insights",
                    "correlations": correlations.to_dict()
                }
            ))
        except Exception as e:
            print(f"Error al crear documento de correlaciones: {str(e)}")
        
        return documents

    def build_index(self, df: pd.DataFrame, rebuild: bool = False):
        """Construye o carga el índice vectorial"""
        try:
            current_hash = self._calculate_data_hash(df)
            
            if rebuild or self._should_rebuild_index(current_hash):
                print("\n⚠️ AVISO DE COSTOS ⚠️")
                print("Se va a crear un nuevo índice de embeddings (esto generará costos de API).")
                print("Motivo: Primera ejecución o cambios detectados en los datos.")
                
                documents = self._create_documents(df)
                storage_context = StorageContext.from_defaults()
                self.index = VectorStoreIndex.from_documents(
                    documents,
                    storage_context=storage_context
                )
                storage_context.persist(persist_dir=self.persist_dir)
                self._save_metadata(current_hash)
                
                print("✅ Embeddings generados y guardados exitosamente.")
            else:
                print("\n💰 AHORRO DE COSTOS 💰")
                print("Usando índice de embeddings existente (sin costo adicional).")
                
                storage_context = StorageContext.from_defaults(persist_dir=self.persist_dir)
                self.index = load_index_from_storage(storage_context)
                self.raw_data = df.copy()
                
        except Exception as e:
            print(f"Error en build_index: {str(e)}")
            traceback.print_exc()
            raise

    def query(self, query_str: str) -> Dict[str, Any]:
        """Realiza una consulta general al índice"""
        if not self.index:
            raise ValueError("El índice no ha sido construido")
        
        try:
            query_engine = self.index.as_query_engine(
                response_mode="tree_summarize",
                streaming=True
            )
            
            response = query_engine.query(query_str)
            
            return {
                "response": str(response),
                "source_nodes": [
                    {
                        "text": node.text,
                        "metadata": node.metadata
                    } for node in response.source_nodes
                ] if hasattr(response, 'source_nodes') else []
            }
        except Exception as e:
            print(f"Error en query: {str(e)}")
            traceback.print_exc()
            raise