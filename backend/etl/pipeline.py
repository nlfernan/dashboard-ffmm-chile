import os
import pandas as pd
import unicodedata
import traceback
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# 🔗 Forzar URL explícita para evitar confusión entre interna y pública
DB_URL = "postgresql://postgres:DZJlkeBFSrYelRzBhtFOJKlhVkkTrUID@mainline.proxy.rlwy.net:23801/railway"
engine = create_engine(DB_URL, pool_pre_ping=True)
print(f"🔗 Usando URL: {DB_URL}")

# 📂 Ruta parquet
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARQUET_PATH = os.path.join(BASE_DIR, "../data_fuentes/ffmm_merged.parquet")

def limpiar_nombre(col):
    col = unicodedata.normalize('NFKD', col).encode('ascii', 'ignore').decode('ascii')
    return ''.join(c if c.isalnum() else '_' for c in col).lower()

def hacer_unicas(cols):
    seen = {}
    nuevas = []
    for c in cols:
        if c not in seen:
            seen[c] = 0
            nuevas.append(c)
        else:
            seen[c] += 1
            nuevas.append(f"{c}_{seen[c]}")
    return nuevas

def procesar_parquet_por_chunks(ruta_parquet=PARQUET_PATH,
                                tabla_destino="fondos_mutuos",
                                chunk_size=200000):
    print("🚀 Iniciando carga batch por chunks desde parquet...")
    print(f"📂 Leyendo parquet: {ruta_parquet}")

    try:
        df = pd.read_parquet(ruta_parquet, engine="pyarrow")
        print(f"✅ Dataframe cargado: {len(df)} filas")
        print(f"📝 Columnas originales: {list(df.columns)}")
        df.columns = [limpiar_nombre(c) for c in df.columns]
        df.columns = hacer_unicas(df.columns)
        print(f"📝 Columnas finales: {list(df.columns)}")
    except Exception as e:
        print(f"❌ Error al leer parquet: {e}")
        return

    try:
        with engine.connect() as conn:
            db, schema = conn.execute(text("SELECT current_database(), current_schema();")).fetchone()
            print(f"📌 Conectado a Base: {db}, Schema: {schema}")

        # 🧹 Limpiar tabla antes de cargar
        with engine.begin() as conn:
            print(f"🧹 Limpiando tabla {tabla_destino}...")
            conn.execute(text(f'TRUNCATE TABLE "{tabla_destino}"'))

        total = len(df)
        for i in range(0, total, chunk_size):
            chunk = df.iloc[i:i+chunk_size]
            print(f"🔹 Insertando filas {i+1:,} a {i+len(chunk):,} de {total:,}")
            with engine.begin() as conn:
                chunk.to_sql(tabla_destino, conn, if_exists="append", index=False, method='multi')

            # ✅ Contar después de cada chunk
            with engine.connect() as conn:
                count = conn.execute(text(f'SELECT COUNT(*) FROM "{tabla_destino}"')).scalar()
                print(f"📊 Total filas actuales en {tabla_destino}: {count}")

        print("🧹 Ejecutando ANALYZE...")
        with engine.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT").execute(text(f'ANALYZE "{tabla_destino}";'))

        with engine.connect() as conn:
            total_final = conn.execute(text(f'SELECT COUNT(*) FROM "{tabla_destino}"')).scalar()
            print(f"✅ Carga completada. Total final en {tabla_destino}: {total_final}")

    except SQLAlchemyError as e:
        print(f"❌ Error general en procesamiento: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    procesar_parquet_por_chunks()
