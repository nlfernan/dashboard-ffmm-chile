from fastapi import FastAPI
from app.routes import fondos, health

app = FastAPI(title="FFMM Chile API", version="1.0")

# Incluir rutas
app.include_router(health.router, prefix="/health", tags=["Health"])
app.include_router(fondos.router, prefix="/fondos", tags=["Fondos"])

@app.get("/")
def root():
    return {"status": "ok", "message": "API FFMM Chile lista"}
