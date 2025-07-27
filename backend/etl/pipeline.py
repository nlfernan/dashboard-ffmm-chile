import os
import pandas as pd
import unicodedata
import traceback
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

DB_URL = os.getenv("DATABASE_PUBLIC_URL") or os.getenv("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("❌ No se encontró DATABASE_PUBLIC_URL ni DATABASE_URL.")

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

    try:
        # ✅ Crear índice único si no existe
        with engine.begin() as conn:
            conn.execute(text("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_indexes WHERE tablename='fondos_mutuos' AND indexname='unique_fm_fecha'
                    ) THEN
                        ALTER TABLE fondos_mutuos
                        ADD CONSTRAINT unique_fm_fecha UNIQUE (run_fm, fecha_inf, serie);
                    END IF;
                END$$;
            """))

        total = len(df)
        for i in range(0, total, chunk_size):
            chunk = df.iloc[i:i+chunk_size]
            print(f"🔹 Insertando filas {i+1:,} a {i+len(chunk):,} de {total:,}")

            # Cargar a tabla staging temporal
            with engine.begin() as conn:
                chunk.to_sql("fondos_temp", conn, if_exists="replace" if i == 0 else "append", index=False)

        # ✅ Merge desde tabla temporal con ON CONFLICT DO NOTHING
        with engine.begin() as conn:
            columnas = ','.join(df.columns)
            conn.execute(text(f"""
                INSERT INTO fondos_mutuos ({columnas})
                SELECT {columnas} FROM fondos_temp
                ON CONFLICT (run_fm, fecha_inf, serie) DO NOTHING;
            """))
            conn.execute(text("DROP TABLE fondos_temp;"))

        # ✅ Analyze final
        with engine.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT").execute(text(f'ANALYZE "{tabla_destino}";'))
            result = conn.execute(text(f'SELECT COUNT(*) FROM "{tabla_destino}";')).scalar()
            print(f"✅ Carga completada. Total de filas en {tabla_destino}: {result}")

    except SQLAlchemyError as e:
        print(f"❌ Error general en procesamiento: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    procesar_parquet_por_chunks()
