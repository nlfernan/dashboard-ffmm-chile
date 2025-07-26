# -*- coding: utf-8 -*-
import os
import pandas as pd
from sqlalchemy import create_engine, text

# -------------------------------
# Configuración de DB
# -------------------------------
DB_URL = os.getenv("DB_URL")

if not DB_URL:
    raise RuntimeError("❌ DB_URL no está configurada. Verificá las variables de entorno en Railway.")

# ✅ Pool pre_ping para evitar conexiones muertas en Railway
engine = create_engine(DB_URL, pool_pre_ping=True, future=True)

# -------------------------------
# Función de inserción por chunk
# -------------------------------
def insertar_batch(df_chunk, tabla_destino):
    if df_chunk.empty:
        return

    registros = df_chunk.to_dict(orient="records")
    columnas = df_chunk.columns.tolist()
    cols_str = ",".join(columnas)
    valores_str = ",".join([f":{col}" for col in columnas])

    insert_stmt = text(f"""
        INSERT INTO {tabla_destino} ({cols_str})
        VALUES ({valores_str})
    """)

    # ✅ Usar engine.begin() en lugar de with conn:
    with engine.begin() as conn:
        conn.execute(insert_stmt, registros)

# -------------------------------
# Pipeline principal
# -------------------------------
def procesar_parquet_por_chunks(ruta_parquet, tabla_destino, chunk_size=100000):
    print(f"🚀 Iniciando carga batch por chunks desde parquet...")
    print(f"📂 Leyendo parquet en chunks: {ruta_parquet}")

    df = pd.read_parquet(ruta_parquet)
    total_filas = len(df)
    print(f"✅ Preview parquet: {min(1000, total_filas)} filas")
    print(f"📝 Columnas: {list(df.columns)}")

    for i in range(0, total_filas, chunk_size):
        df_chunk = df.iloc[i:i+chunk_size]
        print(f"🔹 Chunk {i//chunk_size+1}: {df_chunk.shape[0]} filas")
        insertar_batch(df_chunk, tabla_destino)

    print("✅ Carga completa en la tabla:", tabla_destino)

# -------------------------------
# Ejecución directa
# -------------------------------
if __name__ == "__main__":
    RUTA_PARQUET = "/app/data_fuentes/ffmm_merged.parquet"
    TABLA_DESTINO = "fondos_mutuos"

    procesar_parquet_por_chunks(RUTA_PARQUET, TABLA_DESTINO)
