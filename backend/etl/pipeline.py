import pandas as pd
from sqlalchemy import create_engine, text, inspect
import os

# 🔗 URL de la DB desde variables de entorno
DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise RuntimeError("❌ DB_URL no está configurada. Verificá las variables de entorno en Railway.")

# 🚀 Conexión al motor
engine = create_engine(DB_URL)

# 📌 Función para procesar parquet en chunks
def procesar_parquet_por_chunks(ruta_parquet, tabla_destino, chunk_size=100000):
    print(f"🚀 Iniciando carga batch por chunks desde parquet...")
    df_preview = pd.read_parquet(ruta_parquet)
    print(f"✅ Preview parquet: {len(df_preview)} filas")
    print(f"📝 Columnas: {list(df_preview.columns)}")

    # 🛠️ Crear tabla si no existe
    with engine.connect() as conn:
        inspector = inspect(engine)
        if tabla_destino not in inspector.get_table_names():
            print(f"📌 Tabla '{tabla_destino}' no existe, creando automáticamente...")
            conn.execute(text(f"""
            CREATE TABLE {tabla_destino} (
                RUN_ADM BIGINT,
                NOM_ADM TEXT,
                RUN_FM TEXT,
                FECHA_INF BIGINT,
                ACTIVO_TOT NUMERIC,
                MONEDA TEXT,
                PARTICIPES_INST TEXT,
                INVERSION_EN_FONDOS NUMERIC,
                SERIE TEXT,
                CUOTAS_APORTADAS NUMERIC,
                CUOTAS_RESCATADAS NUMERIC,
                CUOTAS_EN_CIRCULACION NUMERIC,
                VALOR_CUOTA NUMERIC,
                PATRIMONIO_NETO NUMERIC,
                NUM_PARTICIPES INTEGER,
                NUM_PARTICIPES_INST INTEGER,
                FONDO_PEN TEXT,
                REM_FIJA NUMERIC,
                REM_VARIABLE NUMERIC,
                GASTOS_AFECTOS NUMERIC,
                GASTOS_NO_AFECTOS NUMERIC,
                COMISION_INVERSION NUMERIC,
                COMISION_RESCATE NUMERIC,
                FACTOR_DE_REPARTO NUMERIC,
                RUT_Administradora BIGINT,
                Raz__Social_Administradora TEXT,
                RUN_Fondo BIGINT,
                Nombre_Fondo TEXT,
                Nombre_Corto TEXT,
                Fecha_Res__Aprobación_del_RI TEXT,
                Nro__Res__Aprobación_del_RI NUMERIC,
                Tipo_de_Fondo_Mutuo INTEGER,
                Fecha_Inicio_Operaciones TEXT,
                Fecha_Término_Operaciones TEXT,
                Moneda TEXT,
                Rut TEXT,
                Categoría TEXT,
                FECHA_INF_DATE TIMESTAMP,
                PATRIMONIO_NETO_MM NUMERIC,
                RUN_FM_NOMBRECORTO TEXT,
                VENTA_NETA_MM NUMERIC,
                Nombre_Tipo TEXT,
                TIPO_FM TEXT
            );
            """))
            conn.commit()
            print(f"✅ Tabla '{tabla_destino}' creada.")

    # 📂 Leer en chunks
    for chunk in pd.read_parquet(ruta_parquet, chunksize=chunk_size):
        print(f"🔹 Chunk {chunk.shape[0]} filas")
        try:
            chunk.to_sql(tabla_destino, engine, if_exists='append', index=False)
        except Exception as e:
            print(f"⚠️ Error al ejecutar batch en startup: {e}")
            break
