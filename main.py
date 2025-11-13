from fastapi import FastAPI, Depends, Response
from fastapi.responses import StreamingResponse
from typing import List
import csv
from io import StringIO

from app.db import Base, engine, get_db
from app import models
from sqlalchemy.orm import Session
from sqlalchemy import select

from app.etl.load import run_etl

app = FastAPI(title="ETL SQLite - DW Salud")

@app.on_event("startup")
def on_startup():
    from app import models  # asegura que estén importados
    Base.metadata.create_all(bind=engine)

@app.post("/etl/run")
def etl_run():
    """Dispara ETL incremental (lee /data, hace upserts y carga hechos)."""
    result = run_etl()
    return result

# --------- Helpers ---------
def to_csv(rows: List[dict]):
    buffer = StringIO()
    if not rows:
        return ""
    writer = csv.DictWriter(buffer, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    for r in rows:
        writer.writerow(r)
    return buffer.getvalue()

def query_all(db: Session, model):
    data = db.execute(select(model)).scalars().all()
    # dict-ify
    rows = []
    for obj in data:
        d = {}
        for c in obj.__table__.columns:
            d[c.name] = getattr(obj, c.name)
        rows.append(d)
    return rows

# --------- Dims JSON/CSV ----------
@app.get("/dim/{name}")
def dim_json(name: str, db: Session = Depends(get_db), format: str = "json"):
    table_map = {
        "fecha": models.Dim_Fecha,
        "fuente": models.Dim_Fuente,
        "ubicacion": models.Dim_Ubicacion,
        "estacion": models.Dim_Estacion,
        "poblacion": models.Dim_Poblacion,
    }
    model = table_map.get(name.lower())
    if not model:
        return {"error": "Dimensión no existente"}
    rows = query_all(db, model)
    if format == "csv":
        return StreamingResponse(iter([to_csv(rows)]), media_type="text/csv")
    return rows

# --------- Hechos JSON/CSV ----------
@app.get("/hecho/{name}")
def hecho_json(name: str, db: Session = Depends(get_db), format: str = "json"):
    table_map = {
        "salud_ambiental": models.Hecho_Salud_Ambiental,
        "morbilidad": models.Hecho_Morbilidad,
    }
    model = table_map.get(name.lower())
    if not model:
        return {"error": "Hecho no existente"}
    rows = query_all(db, model)
    if format == "csv":
        return StreamingResponse(iter([to_csv(rows)]), media_type="text/csv")
    return rows

@app.get("/")
def root():
    return {"ok": True, "endpoints": [
        "POST /etl/run",
        "GET /dim/{fecha|fuente|ubicacion|estacion|poblacion}?format=json|csv",
        "GET /hecho/{salud_ambiental|morbilidad}?format=json|csv"
    ]}
