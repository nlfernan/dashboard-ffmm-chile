import pandas as pd
from io import StringIO
from sqlalchemy import text
from app.database import engine

PARQUET_PATH = "ffmm_merged.parquet"

def procesar_parquet():
    print(f"📂 Leyendo parquet: {PARQUET_PATH}")
    df = pd.read_parquet(PARQUET_PATH)
    print(f"✅ Total registros leídos: {len(df)}")
    return df

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
