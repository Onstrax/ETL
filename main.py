from fastapi import FastAPI, Depends, Response, Query, HTTPException
from fastapi.responses import StreamingResponse, Response
from typing import List
import csv
from io import StringIO

from app.db import Base, engine, get_db, SessionLocal
from app import models
from sqlalchemy.orm import Session
from sqlalchemy import select, text

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
# @app.get("/hecho/{name}")
# def hecho_json(name: str, db: Session = Depends(get_db), format: str = "json"):
#     table_map = {
#         "salud_ambiental": models.Hecho_Salud_Ambiental,
#         "infeccioso": models.Hecho_Infeccioso,
#     }
#     model = table_map.get(name.lower())
#     if not model:
#         return {"error": "Hecho no existente"}
#     rows = query_all(db, model)
#     if format == "csv":
#         return StreamingResponse(iter([to_csv(rows)]), media_type="text/csv")
#     return rows

@app.get("/hecho/infeccioso")
def get_infeccioso(format: str = "json"):
    from app.db import SessionLocal
    from app import models
    session = SessionLocal()
    rows = session.query(models.Hecho_Infeccioso).all()
    data = [{c.name: getattr(r, c.name) for c in r.__table__.columns} for r in rows]
    if format == "csv":
        import io, csv
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=data[0].keys() if data else [])
        writer.writeheader(); writer.writerows(data)
        return Response(content=output.getvalue(), media_type="text/csv")
    return data

@app.get("/hecho/salud_ambiental/sample")
def sample_salud_ambiental(
    limit: int = Query(1000, gt=1, le=100_000),
    source: str | None = Query(None, description="IDEAM o SISAIRE"),
    date_from: str | None = Query(None, description="YYYY-MM-DD"),
    date_to: str | None = Query(None, description="YYYY-MM-DD"),
    format: str = Query("json", pattern="^(json|csv)$"),
):
    """
    Devuelve un subconjunto aleatorio de Hecho_Salud_Ambiental con filtros opcionales.
    Nota: ORDER BY RANDOM() limita rápido para validar carga, pero puede escanear la tabla completa si no filtras.
    """
    session = SessionLocal()
    try:
        # Construir filtros
        joins = []
        conds = []
        params: dict = {}

        # Filtrado por fuente legible (IDEAM/SISAIRE)
        if source:
            src = source.strip().upper()
            fuente = session.query(models.Dim_Fuente).filter_by(entidad=src, confiabilidad="Oficial", tipo_dato="Sensor").first()
            if not fuente:
                raise HTTPException(status_code=400, detail=f"Fuente no encontrada: {source}")
            conds.append("h.id_fuente = :id_fuente")
            params["id_fuente"] = fuente.id_fuente

        # Filtro por fecha usando Dim_Fecha para no tocar claves directamente
        if date_from or date_to:
            joins.append("JOIN Dim_Fecha f ON f.id_fecha = h.id_fecha")
            if date_from:
                conds.append("f.fecha >= :date_from")
                params["date_from"] = date_from
            if date_to:
                conds.append("f.fecha <= :date_to")
                params["date_to"] = date_to

        base = "SELECT h.* FROM Hecho_Salud_Ambiental h"
        if joins:
            base += " " + " ".join(joins)
        if conds:
            base += " WHERE " + " AND ".join(conds)

        # Muestra aleatoria
        sql = text(f"{base} ORDER BY RANDOM() LIMIT :limit")
        params["limit"] = limit

        rows = session.execute(sql, params).mappings().all()
        data = [dict(r) for r in rows]

        if format == "csv":
            import io, csv
            out = io.StringIO()
            fieldnames = list(data[0].keys()) if data else []
            writer = csv.DictWriter(out, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
            return Response(content=out.getvalue(), media_type="text/csv")
        return data
    finally:
        session.close()

@app.get("/hecho/salud_ambiental")
def get_salud_ambiental(
    limit: int = Query(1000, gt=1, le=100_000),
    offset: int = Query(0, ge=0),
    format: str = Query("json", pattern="^(json|csv)$"),
):
    session = SessionLocal()
    try:
        sql = text("SELECT * FROM Hecho_Salud_Ambiental LIMIT :limit OFFSET :offset")
        rows = session.execute(sql, {"limit": limit, "offset": offset}).mappings().all()
        data = [dict(r) for r in rows]
        if format == "csv":
            import io, csv
            out = io.StringIO()
            fieldnames = list(data[0].keys()) if data else []
            writer = csv.DictWriter(out, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
            return Response(content=out.getvalue(), media_type="text/csv")
        return data
    finally:
        session.close()

@app.get("/")
def root():
    return {"ok": True, "endpoints": [
        "POST /etl/run",
        "GET /dim/{fecha|fuente|ubicacion|estacion|poblacion}?format=json|csv",
        "GET /hecho/{salud_ambiental|infeccioso}?format=json|csv"
    ]}
