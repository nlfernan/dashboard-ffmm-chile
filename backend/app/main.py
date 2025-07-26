import pandas as pd
from fastapi import FastAPI
from sqlalchemy.orm import Session
from app.database import engine, SessionLocal
from app.models import Base, FondoMutuo

# Crear las tablas en Postgres si no existen
Base.metadata.create_all(bind=engine)

app = FastAPI()

# Cargar el parquet y poblar la base (solo una vez)
@app.on_event("startup")
def cargar_datos():
    db = SessionLocal()
    try:
        # Verificamos si ya hay datos para no duplicar
        count = db.query(FondoMutuo).count()
        if count == 0:
            df = pd.read_parquet("ffmm_merged.parquet")

            # Renombrar columnas para mapear con el modelo
            df = df.rename(columns={
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

            # Convertir fecha a datetime
            df["fecha_inf"] = pd.to_datetime(df["fecha_inf"])

            # Insertar fila por fila
            registros = [
                FondoMutuo(**row.dropna().to_dict()) for _, row in df.iterrows()
            ]
            db.bulk_save_objects(registros)
            db.commit()
            print(f"✅ Insertados {len(registros)} registros en Railway")
        else:
            print(f"⚠️ La tabla ya tiene {count} registros, no se insertó nada")
    finally:
        db.close()

@app.get("/")
def root():
    return {"status": "Base de datos inicializada"}
