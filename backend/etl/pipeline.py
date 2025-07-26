import os
import pandas as pd
from io import StringIO
from sqlalchemy import text
from app.database import engine
import pyarrow.dataset as ds  # Necesario para chunks

# Ruta absoluta al parquet
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARQUET_PATH = os.path.join(BASE_DIR, "..", "data_fuentes", "ffmm_merged.parquet")
PARQUET_PATH = os.path.normpath(PARQUET_PATH)

CHUNK_SIZE = 100_000  # Cantidad de filas por batch

def procesar_parquet_por_chunks():
    print(f"📂 Leyendo parquet en chunks: {PARQUET_PATH}")
    if not os.path.exists(PARQUET_PATH):
        raise FileNotFoundError(f"❌ No se encontró el parquet en {PARQUET_PATH}")

    dataset = ds.dataset(PARQUET_PATH, format="parquet")
    total_rows = 0

    for i, table in enumerate(dataset.to_batches(batch_size=CHUNK_SIZE)):
        df = table.to_pandas()
        total_rows += len(df)
        print(f"🔹 Chunk {i+1}: {len(df)} filas")
        cargar_a_postgres_batch(df)

    print(f"✅ Total filas cargadas: {total_rows}")

def cargar_a_postgres_batch(df):
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

    df_sql = df_sql[columnas]

    print(f"🛠️ Insertando batch de {len(df_sql)} filas")

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

if __name__ == "__main__":
    procesar_parquet_por_chunks()
