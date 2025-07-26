from fastapi import FastAPI
import os

# Crear la app de FastAPI
app = FastAPI()

# Ruta de prueba
@app.get("/")
def home():
    return {"status": "ok", "mensaje": "Dashboard FFMM Chile funcionando en Railway"}

# Leer puerto dinámico que da Railway
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))  # 8000 para local, PORT para Railway
    uvicorn.run("main:app", host="0.0.0.0", port=port)