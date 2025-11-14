from sqlalchemy import (
    Column, Integer, Float, String, Boolean, Date, ForeignKey, UniqueConstraint
)
from .db import Base

# -------------------------
# Dimensiones (SK autoincremental)
# -------------------------
class Dim_Poblacion(Base):
    __tablename__ = "Dim_Poblacion"
    id_poblacion = Column(Integer, primary_key=True, autoincrement=True)  # SK
    sexo = Column(String(20), nullable=False)  # 'masculino', 'femenino', 'no_aplica'
    edad = Column(String(30), nullable=False)  # 'Menor 1 ano' | 'Menor 5 anos' | 'Mayor 60 anos' | 'Poblacion general'
    regimen_seguridad_social = Column(String(100), nullable=True)
    __table_args__ = (
        UniqueConstraint("sexo", "edad", "regimen_seguridad_social", name="uq_poblacion_bk"),
    )

class Dim_Fecha(Base):
    __tablename__ = "Dim_Fecha"
    id_fecha = Column(Integer, primary_key=True, autoincrement=True)
    dia = Column(Integer, nullable=False)
    mes = Column(Integer, nullable=False)
    ano = Column(Integer, nullable=False)
    fecha = Column(Date, nullable=False, unique=True)  # BK
    __table_args__ = (
        UniqueConstraint("dia", "mes", "ano", name="uq_fecha_dmy"),
    )

class Dim_Estacion(Base):
    __tablename__ = "Dim_Estacion"
    id_estacion = Column(Integer, primary_key=True, autoincrement=True)
    nombre = Column(String(255), nullable=False, unique=True)  # BK

class Dim_Ubicacion(Base):
    __tablename__ = "Dim_Ubicacion"
    id_ubicacion = Column(Integer, primary_key=True, autoincrement=True)
    localidad = Column(String(255), nullable=False, unique=True)  # BK

class Dim_Fuente(Base):
    __tablename__ = "Dim_Fuente"
    id_fuente = Column(Integer, primary_key=True, autoincrement=True)
    entidad = Column(String(255), nullable=False)
    confiabilidad = Column(String(100), nullable=True)
    tipo_dato = Column(String(100), nullable=True)
    __table_args__ = (
        UniqueConstraint("entidad", "confiabilidad", "tipo_dato", name="uq_fuente_bk"),
    )

# -------------------------
# Hechos (PK compuesta = FKs)
# -------------------------
class Hecho_Salud_Ambiental(Base):
    __tablename__ = "Hecho_Salud_Ambiental"

    # PK compuesta (sin población)
    id_fecha     = Column(Integer, ForeignKey("Dim_Fecha.id_fecha"), primary_key=True, nullable=False)
    id_fuente    = Column(Integer, ForeignKey("Dim_Fuente.id_fuente"), primary_key=True, nullable=False)
    id_ubicacion = Column(Integer, ForeignKey("Dim_Ubicacion.id_ubicacion"), primary_key=True, nullable=False)
    id_estacion  = Column(Integer, ForeignKey("Dim_Estacion.id_estacion"), primary_key=True, nullable=False)

    # Medidas
    promedio_pm25 = Column(Float, nullable=True)
    promedio_pm10 = Column(Float, nullable=True)
    promedio_co   = Column(Float, nullable=True)
    promedio_so2  = Column(Float, nullable=True)
    promedio_o3   = Column(Float, nullable=True)
    promedio_temperatura      = Column(Float, nullable=True)
    promedio_humedad          = Column(Float, nullable=True)
    promedio_precipitacion    = Column(Float, nullable=True)
    promedio_velocidad_viento = Column(Float, nullable=True)
    promedio_direccion_viento = Column(Float, nullable=True)

class Hecho_Infeccioso(Base):
    __tablename__ = "Hecho_Infeccioso"

    # PK compuesta (igual que antes)
    id_fecha     = Column(Integer, ForeignKey("Dim_Fecha.id_fecha"),         primary_key=True, nullable=False)
    id_fuente    = Column(Integer, ForeignKey("Dim_Fuente.id_fuente"),       primary_key=True, nullable=False)
    id_ubicacion = Column(Integer, ForeignKey("Dim_Ubicacion.id_ubicacion"), primary_key=True, nullable=False)
    id_poblacion = Column(Integer, ForeignKey("Dim_Poblacion.id_poblacion"), primary_key=True, nullable=False)

    # Medidas nuevas
    casos_morbilidad_ira = Column(Integer, nullable=True)  # osb semanal (conteos ya vienen)
    casos_mortalidad_ira = Column(Integer, nullable=True)  # osb anual (conteo por filas)
