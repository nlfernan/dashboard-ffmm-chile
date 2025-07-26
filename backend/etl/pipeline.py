# -*- coding: utf-8 -*-
import os
import pandas as pd
from sqlalchemy import create_engine, text

# -------------------------------
# Configuración de DB
# -------------------------------
raw_url = os.getenv("DATABASE_URL")

if not raw_url:
    raise RuntimeError("❌ DATABASE_URL no está configurada. Revisá las variables de entorno en Railway.")

# ✅ Ajuste para SQLAlchemy + psycopg2 (Railway usa 'postgresql://')
if raw_url.startswith("postgres://"):
    DB_URL = raw_url.replace("postgres://", "postgresql+psycopg2://", 1)
elif raw_url.startswith("postgresql://"):
    DB_URL = raw_url.replace("postgresql://", "postgresql+psycopg2://", 1)
else:
    DB_URL = raw_url

print(f"🔗 Conectando a DB: {DB_URL.split('@')[-1]}")  # Debug: muestra solo host:puerto/bd

# ✅ Crear engine con pool pre_ping para Railway
engine = create_engine(DB_URL, pool_pre_ping=True, future=True)

# -------------------------------
# Función de inserción por chunk
# -------------------------------
def insertar_batch(df_chunk, tabla_destino):
    if df_chunk.empty:
        return

    # ✅ Limpiar nombres de columnas dentro del chunk (puntos, espacios, acentos)
    df_chunk.columns = [col.replace(".", "_").replace(" ", "_") for col in df_chunk.columns]

    registros = df_chunk.to_dict(orient="records")
    columnas = df_chunk.columns.tolist()
    cols_str = ",".join(columnas)
    valores_str = ",".join([f":{col}" for col in columnas])

    insert_stmt = text(f"""
        INSERT INTO {tabla_destino} ({cols_str})
        VALUES ({valores_str})
    """)

    # ✅ Usar engine.begin() para manejar commits y evitar _ConnectionFairy
    with engine.begin() as conn:
        conn.execute(insert_stmt, registros)

# -------------------------------
# Pipeline principal (con defaults)
# -------------------------------
def procesar_parquet_por_chunks(
    ruta_parquet="/app/data_fuentes/ffmm_merged.parquet",
    tabla_destino="fondos_mutuos",
    chunk_size=100000
):
    print(f"🚀 Iniciando carga batch por chunks desde parquet...")
    print(f"📂 Leyendo parquet en chunks: {ruta_parquet}")

    df = pd.read_parquet(ruta_parquet)

    # ✅ Limpiar nombres de columnas para que sean SQL-safe
    df.columns = [col.replace(".", "_").replace(" ", "_") for col in df.columns]

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
    procesar_parquet_por_chunks()  # Usa defaults automáticamente
