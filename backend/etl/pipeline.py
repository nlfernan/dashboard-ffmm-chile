import os
import pandas as pd
import unicodedata
import traceback
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

DB_URL = os.getenv("DATABASE_PUBLIC_URL") or os.getenv("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("❌ No se encontró DATABASE_PUBLIC_URL ni DATABASE_URL. Verificá las variables de entorno en Railway.")

engine = create_engine(DB_URL)
print(f"🔗 Usando URL: {DB_URL}")

def limpiar_nombre(col):
    # Normalizar a ASCII, quitar acentos
    col = unicodedata.normalize('NFKD', col).encode('ascii', 'ignore').decode('ascii')
    # Reemplazar caracteres no alfanuméricos por _
    col = ''.join(c if c.isalnum() else '_' for c in col)
    # Pasar a minúsculas
    return col.lower()

def procesar_parquet_por_chunks(ruta_parquet="/app/data_fuentes/ffmm_merged.parquet",
                                tabla_destino="fondos_mutuos",
                                chunk_size=50000):
    print("🚀 Iniciando carga batch por chunks desde parquet...")
    print(f"📂 Leyendo parquet: {ruta_parquet}")

    try:
        df = pd.read_parquet(ruta_parquet, engine="pyarrow")
        print(f"✅ Dataframe cargado: {len(df)} filas")
        print(f"📝 Columnas originales: {list(df.columns)}")

        # 🔄 Normalizar columnas (quitar puntos, acentos, espacios)
        df.columns = [limpiar_nombre(c) for c in df.columns]
        print(f"📝 Columnas limpias: {list(df.columns)}")

    except Exception as e:
        print(f"❌ Error al leer parquet: {e}")
        return

    try:
        with engine.begin() as conn:
            print("⚠️ Eliminando tabla fondos_mutuos si existe...")
            conn.execute(text(f'DROP TABLE IF EXISTS "{tabla_destino}";'))

        total = len(df)
        for i in range(0, total, chunk_size):
            chunk = df.iloc[i:i+chunk_size]
            print(f"🔹 Insertando chunk {i//chunk_size + 1}: {len(chunk)} filas")

            if i == 0:
                try:
                    with engine.begin() as conn:
                        chunk.to_sql(tabla_destino, conn, if_exists="replace", index=False, method='multi')
                        print("✅ Tabla creada e insertado primer chunk")
                except Exception:
                    print("❌ Error en primer chunk:")
                    traceback.print_exc()
                    break
            else:
                try:
                    with engine.begin() as conn:
                        chunk.to_sql(tabla_destino, conn, if_exists="append", index=False, method='multi')
                except SQLAlchemyError as e:
                    print(f"⚠️ Error al insertar chunk: {e}")
                    break

        with engine.connect() as conn:
            print("🧹 Ejecutando VACUUM FULL ANALYZE...")
            conn.execute(text(f'VACUUM FULL ANALYZE "{tabla_destino}";'))
            print("✅ VACUUM completado")

    except Exception as e:
        print(f"❌ Error general en procesamiento: {e}")

if __name__ == "__main__":
    procesar_parquet_por_chunks()
