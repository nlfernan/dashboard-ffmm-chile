from fastapi import FastAPI
from etl.pipeline import procesar_parquet_por_chunks

app = FastAPI()

@app.on_event("startup")
async def startup_event():
    ruta_parquet = "/app/data_fuentes/ffmm_merged.parquet"
    tabla_destino = "fondos_mutuos"
    procesar_parquet_por_chunks(ruta_parquet, tabla_destino)

@app.get("/")
async def root():
    return {"message": "API Dashboard FFMM Chile funcionando"}
