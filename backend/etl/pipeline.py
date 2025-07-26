import pandas as pd
import os
from io import StringIO
from sqlalchemy import text
from app.database import engine

def procesar_fuentes():
    carpeta = "data_fuentes"
    dfs = []
    for archivo in os.listdir(carpeta):
        if archivo.endswith(".xlsx") or archivo.endswith(".csv"):
            ruta = os.path.join(carpeta, archivo)
            df = pd.read_excel(ruta) if archivo.endswith(".xlsx") else pd.read_csv(ruta, sep=";")
            dfs.append(df)

    df_final = pd.concat(dfs, ignore_index=True)
    df_final["FECHA_INF_DATE"] = pd.to_datetime(df_final["FECHA_INF"], format="%Y%m%d", errors="coerce")
    df_final["PATRIMONIO_NETO_MM"] = df_final["PATRIMONIO_NETO"] / 1e6
    return df_final

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

    columnas = ["run_adm","nom_adm","run_fm","nombre_fondo","nombre_corto",
                "categoria","fecha_inf","patrimonio_neto","patrimonio_neto_mm",
                "valor_cuota","cuotas_en_circulacion","venta_neta_mm",
                "tipo_fondo","nombre_tipo","moneda"]

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
    df = procesar_fuentes()
    cargar_a_postgres_batch(df)