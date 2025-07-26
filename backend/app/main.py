from fastapi import FastAPI
from app.database import engine, Base
from app.models import FondoMutuo
from etl.pipeline import procesar_fuentes, cargar_a_postgres_batch

# Crear tablas si no existen
Base.metadata.create_all(bind=engine)

app = FastAPI()

# Ejecutar batch al iniciar la app en Railway
@app.on_event("startup")
def run_batch_on_startup():
    try:
        print("🚀 Iniciando carga batch...")
        df = procesar_fuentes()
        cargar_a_postgres_batch(df)
    except Exception as e:
        print(f"⚠️ Error al ejecutar batch en startup: {e}")

@app.get("/")
def root():
    return {"status": "ok", "mensaje": "Dashboard FFMM Chile funcionando"}

# Endpoint opcional para contar registros
from sqlalchemy.orm import Session
from app.database import SessionLocal

@app.get("/fondos/count")
def count_fondos():
    db: Session = SessionLocal()
    try:
        total = db.query(FondoMutuo).count()
        return {"total_registros": total}
    finally:
        db.close()
