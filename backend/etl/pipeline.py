import os
import pandas as pd
from sqlalchemy import create_engine, text

# Usar la URL interna si está disponible
DB_URL = os.getenv("DATABASE_URL") or os.getenv("DATABASE_PUBLIC_URL")
if not DB_URL:
    raise RuntimeError("❌ No se encontró DATABASE_URL ni DATABASE_PUBLIC_URL.")

engine = create_engine(DB_URL)
print(f"🔗 Usando URL: {DB_URL}")

# Ruta al parquet
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARQUET_PATH = os.path.join(BASE_DIR, "../data_fuentes/ffmm_merged.parquet")

# Leer parquet y tomar 3 registros
df = pd.read_parquet(PARQUET_PATH, engine="pyarrow")
df_sample = df.head(3)
print("🔍 Data de prueba:\n", df_sample)

# Nombre de tabla de test
tabla_test = "fondos_mutuos_test"

# Crear tabla de prueba desde cero
with engine.begin() as conn:
    conn.execute(text(f'DROP TABLE IF EXISTS "{tabla_test}"'))
    df_sample.to_sql(tabla_test, conn, if_exists="replace", index=False)

# Verificar conteo
with engine.connect() as conn:
    count = conn.execute(text(f'SELECT COUNT(*) FROM "{tabla_test}"')).scalar()
    print(f"✅ Registros insertados en {tabla_test}: {count}")
