from app.database import engine
from app.models import Base

# Crear tablas si no existen
Base.metadata.create_all(bind=engine)