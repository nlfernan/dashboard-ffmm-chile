import os
import pandas as pd
from sqlalchemy import create_engine

DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("❌ DB_URL no está configurada. Verificá las variables de entorno en Railway.")

engine = create_engine(DB_URL)

def procesar_parquet_por_chunks(ruta_parquet, tabla_destino, chunk_size=100000):
    print(f"📂 Leyendo parquet: {ruta_parquet}")
    df = pd.read_parquet(ruta_parquet)

    total_filas = len(df)
    total_chunks = (total_filas // chunk_size) + (1 if total_filas % chunk_size != 0 else 0)

    print(f"✅ Dataframe cargado: {total_filas} filas")
    print(f"📝 Columnas: {list(df.columns)}")

    for i in range(total_chunks):
        start = i * chunk_size
        end = min(start + chunk_size, total_filas)
        chunk = df.iloc[start:end]
        print(f"🔹 Insertando chunk {i+1}/{total_chunks}: {len(chunk)} filas")
        chunk.to_sql(tabla_destino, engine, if_exists="append", index=False)

    print("✅ Inserción completa. Todos los chunks fueron cargados en la tabla.")

