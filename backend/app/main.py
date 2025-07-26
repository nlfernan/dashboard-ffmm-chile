from fastapi import FastAPI
from app.database import engine, Base, SessionLocal
from app.models import FondoMutuo
from etl.pipeline import procesar_parquet_por_chunks
from sqlalchemy.orm import Session

# Crear tablas si no existen
Base.metadata.create_all(bind=engine)

app = FastAPI()

@app.on_event("startup")
def run_batch_on_startup():
    try:
        db: Session = SessionLocal()
        total = db.query(FondoMutuo).count()
        db.close()

        if total > 0:
            print(f"⚠️ La tabla fondos_mutuos ya tiene {total} registros. No se ejecuta el batch.")
            return

        print("🚀 Iniciando carga batch por chunks desde parquet...")
        procesar_parquet_por_chunks()
    except Exception as e:
        print(f"⚠️ Error al ejecutar batch en startup: {e}")

@app.get("/")
def root():
    return {"status": "ok", "mensaje": "Dashboard FFMM Chile funcionando"}

@app.get("/fondos/count")
def count_fondos():
    db: Session = SessionLocal()
    try:
        total = db.query(FondoMutuo).count()
        return {"total_registros": total}
    finally:
        db.close()
