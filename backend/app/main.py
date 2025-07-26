from fastapi import FastAPI
from app.database import engine, Base, SessionLocal
from app.models import FondoMutuo
from etl.pipeline import procesar_parquet, cargar_a_postgres_batch
from sqlalchemy.orm import Session

# Crear tablas si no existen
Base.metadata.create_all(bind=engine)

app = FastAPI()

# Ejecutar batch desde parquet al iniciar la app
@app.on_event("startup")
def run_batch_on_startup():
    try:
        print("🚀 Iniciando carga batch desde parquet...")
        df = procesar_parquet()
        cargar_a_postgres_batch(df)
    except Exception as e:
        print(f"⚠️ Error al ejecutar batch en startup: {e}")

@app.get("/")
def root():
    return {"status": "ok", "mensaje": "Dashboard FFMM Chile funcionando"}

# Endpoint opcional para contar registros en la tabla
@app.get("/fondos/count")
def count_fondos():
    db: Session = SessionLocal()
    try:
        total = db.query(FondoMutuo).count()
        return {"total_registros": total}
    finally:
        db.close()
