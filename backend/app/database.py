import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Lee la variable de entorno DATABASE_URL
DATABASE_URL = os.getenv("DATABASE_URL")

# Crea el motor de conexión
engine = create_engine(DATABASE_URL)

# Configura la sesión
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base para los modelos
Base = declarative_base()
