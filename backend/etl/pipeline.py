import pandas as pd
import os
from io import StringIO
from sqlalchemy import text
from app.database import engine

# Ruta absoluta al parquet
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARQUET_PATH = os.path.join(BASE_DIR, "..", "data_fuentes", "ffmm_merged.parquet")
PARQUET_PATH = os.path.normpath(PARQUET_PATH)

def procesar_parquet():
    print(f"📂 Leyendo parquet: {PARQUET_PATH}")
    if not os.path.exists(PARQUET_PATH):
        raise FileNotFoundError(f"❌ No se encontró el parquet en {PARQUET_PATH}")
    try:
        df = pd.read_parquet(PARQUET_PATH)
    except Exception as e:
        print(f"❌ Error leyendo parquet: {e}")
        raise
    print(f"✅ Total registros leídos: {len(df)}")
    print(f"📝 Columnas en parquet: {list(df.columns)}")
    return df

def cargar_a_postgres_batch(df):
    print("🛠️ Renombrando columnas para Postgres")
    df_sql = df.rename(columns={
        "RUN_ADM": "run_adm",
        "NOM_ADM": "nom_adm",
        "RUN_FM": "run_fm",
        "Nombre_Fondo": "nombre_fondo",
        "Nombre_Corto": "nombre_corto",
        "Categoría": "categoria",
        "FECHA_INF_DATE": "fecha_inf",
        "PATRIMONIO_NETO": "patrimonio_neto",
        "PATRIMONIO_NETO_MM": "patrimonio_neto_mm",
        "VALOR_CUOTA": "valor_cuota",
        "CUOTAS_EN_CIRCULACION": "cuotas_en_circulacion",
        "VENTA_NETA_MM": "venta_neta_mm",
        "Tipo_de_Fondo_Mutuo": "tipo_fondo",
        "Nombre_Tipo": "nombre_tipo",
        "Moneda": "moneda"
    })

    columnas = [
        "run_adm","nom_adm","run_fm","nombre_fondo","nombre_corto",
        "categoria","fecha_inf","patrimonio_neto","patrimonio_neto_mm",
        "valor_cuota","cuotas_en_circulacion","venta_neta_mm",
        "tipo_fondo","nombre_tipo","moneda"
    ]

    print(f"🔍 Columnas después de rename: {list(df_sql.columns)}")

    try:
        df_sql = df_sql[columnas]
    except KeyError as e:
        print(f"❌ Error: faltan columnas necesarias en el DataFrame: {e}")
        raise

    print(f"🔍 Preparando batch con {len(df_sql)} filas para insertar en Postgres")

    buffer = StringIO()
    df_sql.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    with engine.raw_connection() as conn:
        cursor = conn.cursor()
        cursor.copy_expert(f"""
            COPY fondos_mutuos ({','.join(columnas)})
            FROM STDIN WITH CSV
        """, buffer)
        conn.commit()
        cursor.close()

    print(f"✅ Cargados {len(df_sql)} registros vía batch COPY")

if __name__ == "__main__":
    df = procesar_parquet()
    cargar_a_postgres_batch(df)
