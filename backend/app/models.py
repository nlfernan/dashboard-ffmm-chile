# Aquí definirás tus modelos de SQLAlchemy para las tablas
from sqlalchemy import Column, Integer, String
from app.database import Base

class FondoMutuo(Base):
    __tablename__ = "fondos_mutuos"
    id = Column(Integer, primary_key=True, index=True)
    nombre = Column(String, nullable=False)
    categoria = Column(String, nullable=False)