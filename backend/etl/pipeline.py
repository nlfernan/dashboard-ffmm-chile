import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Usar DATABASE_URL como fuente principal
DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("❌ No se encontró DATABASE_URL. Verificá las variables de entorno en Railway.")

engine = create_engine(DB_URL)
print(f"🔗 Usando DATABASE_URL: {DB_URL}")

def procesar_parquet_por_chunks(ruta_parquet="/app/data_fuentes/ffmm_merged.parquet",
                                tabla_destino="fondos_mutuos",
                                chunk_size=50000):
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
                # Usar transacción explícita para asegurar commit
                if i == 0:
                    with engine.begin() as conn:
                        chunk.to_sql(tabla_destino, conn, if_exists="replace", index=False, method='multi')
                else:
                    with engine.begin() as conn:
                        chunk.to_sql(tabla_destino, conn, if_exists="append", index=False, method='multi')
            except SQLAlchemyError as e:
                print(f"⚠️ Error al insertar chunk: {e}")
                break

        # VACUUM para liberar espacio y optimizar índices
        with engine.connect() as conn:
            print("🧹 Ejecutando VACUUM FULL ANALYZE...")
            conn.execute(text(f"VACUUM FULL ANALYZE {tabla_destino};"))
            print("✅ VACUUM completado")

    except Exception as e:
        print(f"❌ Error general en procesamiento: {e}")

if __name__ == "__main__":
    procesar_parquet_por_chunks()
