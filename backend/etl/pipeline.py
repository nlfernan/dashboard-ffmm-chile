import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# 🔗 Usar DATABASE_URL (Railway) o DB_URL (manual)
DB_URL = os.getenv("DB_URL") or os.getenv("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("❌ No se encontró DB_URL ni DATABASE_URL. Verificá las variables de entorno en Railway.")

engine = create_engine(DB_URL)

def procesar_parquet_por_chunks(ruta_parquet, tabla_destino, chunk_size=100000):
    print(f"🚀 Iniciando carga batch por chunks desde parquet...")
    print(f"📂 Leyendo parquet en chunks: {ruta_parquet}")

    try:
        # Preview inicial
        preview = pd.read_parquet(ruta_parquet, engine="pyarrow").head(1000)
        print(f"✅ Preview parquet: {len(preview)} filas")
        print(f"📝 Columnas: {list(preview.columns)}")
    except Exception as e:
        print(f"❌ Error al leer preview: {e}")
        return

    try:
        for i, chunk in enumerate(pd.read_parquet(ruta_parquet, engine="pyarrow", chunksize=chunk_size)):
            print(f"🔹 Chunk {i+1}: {len(chunk)} filas")
            try:
                chunk.to_sql(tabla_destino, engine, if_exists="append", index=False)
                print(f"🛠️ Insertado batch de {len(chunk)} filas")
            except SQLAlchemyError as e:
                print(f"⚠️ Error al ejecutar batch en startup: {e}")
                break
    except Exception as e:
        print(f"❌ Error general en procesamiento: {e}")
