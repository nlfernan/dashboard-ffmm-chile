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

    tmp_table = f"{tabla_destino}_tmp"

    try:
        with engine.begin() as conn:
            print(f"🧹 Eliminando tabla temporal previa {tmp_table} si existe...")
            conn.execute(text(f'DROP TABLE IF EXISTS "{tmp_table}"'))

        total = len(df)
        for i in range(0, total, chunk_size):
            chunk = df.iloc[i:i+chunk_size]
            print(f"🔹 Insertando filas {i+1:,} a {i+len(chunk):,} de {total:,} en {tmp_table}")
            with engine.begin() as conn:
                chunk.to_sql(tmp_table, conn, if_exists="append", index=False, method='multi')

        with engine.begin() as conn:
            print(f"🔄 Reemplazando {tabla_destino} con {tmp_table}")
            conn.execute(text(f'DROP TABLE IF EXISTS "{tabla_destino}"'))
            conn.execute(text(f'ALTER TABLE "{tmp_table}" RENAME TO "{tabla_destino}"'))

        with engine.connect() as conn:
            total_final = conn.execute(text(f'SELECT COUNT(*) FROM "{tabla_destino}"')).scalar()
            print(f"✅ Carga completada. Total en {tabla_destino}: {total_final}")

    except SQLAlchemyError as e:
        print(f"❌ Error en procesamiento: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    procesar_parquet_por_chunks()
