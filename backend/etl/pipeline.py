import os
import pandas as pd
import unicodedata
import traceback
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# -------------------------------
# Configuración DB
# -------------------------------
DB_URL = os.getenv("DATABASE_URL") or os.getenv("DATABASE_PUBLIC_URL")
if not DB_URL:
    raise RuntimeError("❌ No se encontró DATABASE_URL ni DATABASE_PUBLIC_URL.")

engine = create_engine(DB_URL)
print(f"🔗 Usando URL: {DB_URL}")

# -------------------------------
# Paths de Parquet
# -------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARQUET_PATH = os.path.join(BASE_DIR, "../data_fuentes/ffmm_merged.parquet")

# 📌 Carpeta persistente para el Parquet optimizado (Volume en Railway)
FINAL_PARQUET_PATH = "/data/ffmm_merged.parquet"

# -------------------------------
# Funciones de limpieza de columnas
# -------------------------------
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

# -------------------------------
# Pipeline: cargar parquet → PostgreSQL
# -------------------------------
def procesar_parquet_por_chunks(ruta_parquet=PARQUET_PATH,
                                tabla_destino="fondos_mutuos",
                                chunk_size=20000):
    print("🚀 Iniciando carga batch desde parquet...")
    print(f"📂 Leyendo parquet: {ruta_parquet}")

    try:
        df = pd.read_parquet(ruta_parquet, engine="pyarrow")
        total = len(df)
        print(f"✅ Dataframe cargado: {total} filas")
        print(f"📝 Columnas originales: {list(df.columns)}")

        df.columns = [limpiar_nombre(c) for c in df.columns]
        df.columns = hacer_unicas(df.columns)
        print(f"📝 Columnas finales: {list(df.columns)}")

    except Exception as e:
        print(f"❌ Error al leer parquet: {e}")
        return

    tmp_table = f"{tabla_destino}_tmp"

    try:
        # Borrar tabla temporal previa
        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{tmp_table}"'))

        # Crear tabla temporal vacía
        print("📌 Creando tabla temporal...")
        with engine.begin() as conn:
            df.iloc[0:0].to_sql(tmp_table, conn, if_exists="replace", index=False)
        print("✅ Tabla temporal creada")

        # Insertar en chunks con logs
        for i in range(0, total, chunk_size):
            chunk = df.iloc[i:i+chunk_size]
            print(f"➡️ Insertando filas {i+1:,} a {i+len(chunk):,} de {total:,}")
            with engine.begin() as conn:
                chunk.to_sql(tmp_table, conn, if_exists="append", index=False)

            with engine.connect() as conn:
                count = conn.execute(text(f'SELECT COUNT(*) FROM "{tmp_table}"')).scalar()
                print(f"📊 Total acumulado en {tmp_table}: {count}")

        # Verificar total
        with engine.connect() as conn:
            total_tmp = conn.execute(text(f'SELECT COUNT(*) FROM "{tmp_table}"')).scalar()
            print(f"📌 Total en {tmp_table}: {total_tmp}")

        if total_tmp != total:
            print("❌ El total en la tabla temporal no coincide con el parquet. Abortando swap.")
            return

        # Swap seguro con backup
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

        # -------------------------------
        # ✅ Generar Parquet optimizado para el Dashboard
        # -------------------------------
        print("📦 Generando Parquet optimizado para el Dashboard...")
        df_full = pd.read_sql(f'SELECT * FROM "{tabla_destino}";', engine)
        os.makedirs(os.path.dirname(FINAL_PARQUET_PATH), exist_ok=True)
        df_full.to_parquet(FINAL_PARQUET_PATH, index=False)
        print(f"✅ Parquet generado en {FINAL_PARQUET_PATH} con {df_full.shape[0]:,} filas.")

    except SQLAlchemyError as e:
        print(f"❌ Error en el pipeline: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    procesar_parquet_por_chunks()
