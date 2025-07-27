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

def procesar_parquet_por_chunks(ruta_parquet=PARQUET_PATH,
                                tabla_destino="fondos_mutuos",
                                chunk_size=50000):
    print("🚀 Iniciando carga batch desde parquet...")
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

    total = len(df)
    tmp_table = f"{tabla_destino}_tmp"

    try:
        # Borrar tabla temporal previa
        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{tmp_table}"'))

        # Crear tabla temporal vacía
        with engine.begin() as conn:
            df.iloc[0:0].to_sql(tmp_table, conn, if_exists="replace", index=False)

        # Insertar datos en chunks
        for i in range(0, total, chunk_size):
            chunk = df.iloc[i:i+chunk_size]
            print(f"🔹 Insertando filas {i+1:,} a {i+len(chunk):,} de {total:,}")
            with engine.begin() as conn:
                chunk.to_sql(tmp_table, conn, if_exists="append", index=False)

        # Verificar total en la temporal
        with engine.connect() as conn:
            total_tmp = conn.execute(text(f'SELECT COUNT(*) FROM "{tmp_table}"')).scalar()
            print(f"📌 Total en {tmp_table}: {total_tmp}")

        if total_tmp != total:
            print("❌ El total en la tabla temporal no coincide con el parquet. Abortando swap.")
            return

        # Swap con backup: mantiene las dos
        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{tabla_destino}_backup"'))
            conn.execute(text(f'ALTER TABLE "{tabla_destino}" RENAME TO "{tabla_destino}_backup"'))
            conn.execute(text(f'ALTER TABLE "{tmp_table}" RENAME TO "{tabla_destino}"'))

        with engine.connect() as conn:
            final_count = conn.execute(text(f'SELECT COUNT(*) FROM "{tabla_destino}"')).scalar()
            backup_count = conn.execute(text(f'SELECT COUNT(*) FROM "{tabla_destino}_backup"')).scalar()
            print(f"✅ Carga completada.")
            print(f"📊 Total nuevo en {tabla_destino}: {final_count}")
            print(f"📊 Total anterior en {tabla_destino}_backup: {backup_count}")

    except SQLAlchemyError as e:
        print(f"❌ Error general en procesamiento: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    procesar_parquet_por_chunks()
