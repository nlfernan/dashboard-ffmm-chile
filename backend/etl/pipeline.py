# -*- coding: utf-8 -*-
import pandas as pd
from sqlalchemy import create_engine, text

# -------------------------------
# Configuración de la DB
# -------------------------------
DB_URL = "postgresql+psycopg2://usuario:password@host:puerto/basedatos"
engine = create_engine(DB_URL)

# -------------------------------
# Función para insertar un batch
# -------------------------------
def insertar_batch(df_chunk, tabla_destino):
    if df_chunk.empty:
        return

    # Convierte DataFrame a lista de diccionarios para insert masivo
    registros = df_chunk.to_dict(orient="records")

    columnas = df_chunk.columns.tolist()
    cols_str = ",".join(columnas)
    valores_str = ",".join([f":{col}" for col in columnas])

    insert_stmt = text(f"""
        INSERT INTO {tabla_destino} ({cols_str})
        VALUES ({valores_str})
    """)

    # ✅ Usar engine.begin() para manejar transacción correctamente
    with engine.begin() as conn:
        conn.execute(insert_stmt, registros)

# -------------------------------
# Pipeline de carga de Parquet
# -------------------------------
def cargar_parquet_en_db(ruta_parquet, tabla_destino, chunk_size=100000):
    print(f"🚀 Iniciando carga batch por chunks desde parquet...")
    parquet_file = pd.read_parquet(ruta_parquet)
    total_filas = len(parquet_file)
    print(f"📂 Archivo: {ruta_parquet} | {total_filas} filas")

    for i in range(0, total_filas, chunk_size):
        df_chunk = parquet_file.iloc[i:i+chunk_size]
        print(f"🔹 Chunk {i//chunk_size+1}: {df_chunk.shape[0]} filas")
        insertar_batch(df_chunk, tabla_destino)

    print("✅ Carga completa")

# -------------------------------
# Ejecución principal
# -------------------------------
if __name__ == "__main__":
    RUTA_PARQUET = "/app/data_fuentes/ffmm_merged.parquet"
    TABLA_DESTINO = "fondos_mutuos"

    cargar_parquet_en_db(RUTA_PARQUET, TABLA_DESTINO)
