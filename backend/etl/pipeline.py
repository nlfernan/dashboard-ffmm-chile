import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Obtener URL de conexión desde variables de entorno
DB_URL = os.getenv("DB_URL") or os.getenv("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("❌ No se encontró DB_URL ni DATABASE_URL. Verificá las variables de entorno en Railway.")

engine = create_engine(DB_URL)

def procesar_parquet_por_chunks(ruta_parquet, tabla_destino, chunk_size=50000):
    """
    Carga un parquet en PostgreSQL en chunks para evitar saturar pg_wal.
    Ejecuta VACUUM FULL al final para liberar espacio.
    """
    print(f"🚀 Iniciando carga batch por chunks desde parquet...")
    print(f"📂 Leyendo parquet: {ruta_parquet}")

    try:
        df = pd.read_parquet(ruta_parquet, engine="pyarrow")
        print(f"✅ Dataframe cargado: {len(df)} filas")
        print(f"📝 Columnas: {list(df.columns)}")
    except Exception as e:
        print(f"❌ Error al leer parquet: {e}")
        return

    try:
        total = len(df)
        for i in range(0, total, chunk_size):
            chunk = df.iloc[i:i+chunk_size]
            print(f"🔹 Insertando chunk {i//chunk_size + 1}: {len(chunk)} filas")
            try:
                chunk.to_sql(tabla_destino, engine, if_exists="append", index=False, method='multi')
            except SQLAlchemyError as e:
                print(f"⚠️ Error al insertar chunk: {e}")
                break

        # Ejecutar VACUUM FULL ANALYZE al final
        with engine.connect() as conn:
            print("🧹 Ejecutando VACUUM FULL ANALYZE...")
            conn.execute(text(f"VACUUM FULL ANALYZE {tabla_destino};"))
            print("✅ VACUUM completado")
    except Exception as e:
        print(f"❌ Error general en procesamiento: {e}")
