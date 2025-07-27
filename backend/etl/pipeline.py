import os
import pandas as pd
import unicodedata
import traceback
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

DB_URL = os.getenv("DATABASE_URL") or os.getenv("DATABASE_PUBLIC_URL")
if not DB_URL:
    raise RuntimeError("❌ No se encontró DATABASE_URL ni DATABASE_PUBLIC_URL.")

engine = create_engine(DB_URL)
print(f"🔗 Usando URL: {DB_URL}")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARQUET_PATH = os.path.join(BASE_DIR, "../data_fuentes/ffmm_merged.parquet")

def limpiar_nombre(col):
    col = unicodedata.normalize('NFKD', col).encode('ascii', 'ignore').decode('ascii')
    col = ''.join(c if c.isalnum() else '_' for c in col)
    return col.lower()

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

def procesar_parquet_debug(ruta_parquet=PARQUET_PATH,
                           tabla_destino="fondos_mutuos_debug",
                           chunk_size=20000,
                           max_filas=50000):
    print("🚀 Debug: Cargando sólo primeras 50 000 filas del parquet")
    print(f"📂 Leyendo parquet: {ruta_parquet}")

    try:
        df = pd.read_parquet(ruta_parquet, engine="pyarrow")
        df = df.head(max_filas)
        print(f"✅ Dataframe cargado: {len(df)} filas para debug")
        df.columns = [limpiar_nombre(c) for c in df.columns]
        df.columns = hacer_unicas(df.columns)
    except Exception as e:
        print(f"❌ Error al leer parquet: {e}")
        return

    tmp_table = f"{tabla_destino}_tmp"

    try:
        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{tmp_table}"'))

        print("📌 Creando tabla temporal...")
        with engine.begin() as conn:
            df.iloc[0:0].to_sql(tmp_table, conn, if_exists="replace", index=False)
        print("✅ Tabla temporal creada")

        total = len(df)
        for i in range(0, total, chunk_size):
            chunk = df.iloc[i:i+chunk_size]
            print(f"➡️ Insertando filas {i+1:,} a {i+len(chunk):,} de {total:,}")
            with engine.begin() as conn:
                chunk.to_sql(tmp_table, conn, if_exists="append", index=False)

            with engine.connect() as conn:
                count = conn.execute(text(f'SELECT COUNT(*) FROM "{tmp_table}"')).scalar()
                print(f"📊 Total acumulado en {tmp_table}: {count}")

        with engine.connect() as conn:
            final_count = conn.execute(text(f'SELECT COUNT(*) FROM "{tmp_table}"')).scalar()
            print(f"✅ Debug completado. Total final insertado: {final_count}")

    except SQLAlchemyError as e:
        print(f"❌ Error SQLAlchemy: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    procesar_parquet_debug()
