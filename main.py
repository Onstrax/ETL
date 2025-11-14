from fastapi import FastAPI, Depends, Response, Query, HTTPException
from fastapi.responses import StreamingResponse, Response
from typing import List, Optional, Literal
from math import log, exp, sqrt, isnan
from statistics import median, mean
import csv
from io import StringIO
import math

from app.db import Base, engine, get_db, SessionLocal
from app import models
from sqlalchemy.orm import Session
from sqlalchemy import select, text, func

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

def _percentile(values, q: float):
    vals = sorted(v for v in values if v is not None)
    if not vals:
        return None
    if q <= 0:
        return vals[0]
    if q >= 1:
        return vals[-1]
    pos = (len(vals) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return vals[lo]
    return vals[lo] + (vals[hi] - vals[lo]) * (pos - lo)

@app.get("/stats/or_pm25_morbilidad_u5_city_month")
def or_pm25_morbilidad_u5_city_month(
    year_from: int = Query(2020, ge=2000, le=2100),
    year_to: int = Query(2024, ge=2000, le=2100),
    source: Optional[Literal["IDEAM","SISAIRE","ALL"]] = Query("ALL", description="Fuente para PM2.5"),
    exposure_cut: Literal["median","mean","percentile"] = Query("median"),
    outcome_cut: Literal["median","mean","percentile","gt0"] = Query("median"),
    exposure_q: float = Query(0.6, ge=0.0, le=1.0, description="Cuantil para exposure_cut=percentile"),
    outcome_q: float = Query(0.6, ge=0.0, le=1.0, description="Cuantil para outcome_cut=percentile"),
):
    """
    OR entre exposición alta a PM2.5 (mensual) y morbilidad IRA <5 alta (mensual).
    Unidad: ciudad–mes, en el rango [year_from, year_to].
    """
    if year_to < year_from:
        raise HTTPException(status_code=400, detail="year_to debe ser >= year_from")

    s = SessionLocal()
    try:
        # ---------- PM2.5 mensual a nivel ciudad ----------
        q_pm = (
            s.query(
                models.Dim_Fecha.ano.label("ano"),
                models.Dim_Fecha.mes.label("mes"),
                func.avg(models.Hecho_Salud_Ambiental.promedio_pm25).label("pm25_mean")
            )
            .join(models.Dim_Fecha, models.Dim_Fecha.id_fecha == models.Hecho_Salud_Ambiental.id_fecha)
            .join(models.Dim_Fuente, models.Dim_Fuente.id_fuente == models.Hecho_Salud_Ambiental.id_fuente)
            .filter(models.Dim_Fecha.ano.between(year_from, year_to))
            .filter(models.Hecho_Salud_Ambiental.promedio_pm25.isnot(None))
            .group_by(models.Dim_Fecha.ano, models.Dim_Fecha.mes)
        )
        if source in ("IDEAM", "SISAIRE"):
            q_pm = q_pm.filter(models.Dim_Fuente.entidad == source)
        pm_rows = q_pm.all()
        pm_map = {(r.ano, r.mes): float(r.pm25_mean) for r in pm_rows if r.pm25_mean is not None}

        # ---------- Morbilidad IRA <5 mensual a nivel ciudad ----------
        m_rows = (
            s.query(
                models.Dim_Fecha.ano.label("ano"),
                models.Dim_Fecha.mes.label("mes"),
                func.sum(func.coalesce(models.Hecho_Infeccioso.casos_morbilidad_ira, 0)).label("ira_u5")
            )
            .join(models.Dim_Fecha, models.Dim_Fecha.id_fecha == models.Hecho_Infeccioso.id_fecha)
            .join(models.Dim_Poblacion, models.Dim_Poblacion.id_poblacion == models.Hecho_Infeccioso.id_poblacion)
            .filter(models.Dim_Poblacion.edad == "Menor 5 anos")
            .filter(models.Dim_Fecha.ano.between(year_from, year_to))
            .group_by(models.Dim_Fecha.ano, models.Dim_Fecha.mes)
            .all()
        )
        mort_map = {(r.ano, r.mes): int(r.ira_u5 or 0) for r in m_rows}

        # ---------- Unión por ciudad–mes ----------
        keys = sorted(set(pm_map.keys()) & set(mort_map.keys()))
        data = [{"ano": a, "mes": m, "pm25_mean": pm_map[(a,m)], "ira_u5": mort_map[(a,m)]} for (a,m) in keys]
        if not data:
            raise HTTPException(status_code=404, detail="No hay datos combinados ciudad–mes para el rango/fuente.")

        pm_vals = [d["pm25_mean"] for d in data]
        ira_vals = [d["ira_u5"] for d in data]

        # ---------- Cortes ----------
        if exposure_cut == "median":
            cut_pm = median(pm_vals)
        elif exposure_cut == "mean":
            cut_pm = mean(pm_vals)
        else:
            cut_pm = _percentile(pm_vals, exposure_q)

        if outcome_cut == "median":
            cut_ira = median(ira_vals); ira_high = lambda v: v > cut_ira
        elif outcome_cut == "mean":
            cut_ira = mean(ira_vals);   ira_high = lambda v: v > cut_ira
        elif outcome_cut == "percentile":
            cut_ira = _percentile(ira_vals, outcome_q); ira_high = lambda v: v > cut_ira
        else:  # gt0
            cut_ira = 0.0;              ira_high = lambda v: v > 0

        pm_high = lambda v: v > cut_pm

        # ---------- Tabla 2x2 ----------
        a = b = c = d = 0.0
        months = []
        for row in data:
            e = pm_high(row["pm25_mean"])
            o = ira_high(row["ira_u5"])
            months.append({"ano": row["ano"], "mes": row["mes"], "pm25": row["pm25_mean"], "ira_u5": row["ira_u5"], "exp_high": int(e), "out_high": int(o)})
            if e and o:          a += 1
            elif e and not o:    b += 1
            elif (not e) and o:  c += 1
            else:                d += 1

        haldane = (a==0 or b==0 or c==0 or d==0)
        if haldane:
            a += 0.5; b += 0.5; c += 0.5; d += 0.5

        # ---------- OR, IC95%, Wald p ----------
        OR = (a*d)/(b*c)
        lnOR = math.log(OR)
        SE = math.sqrt(1.0/a + 1.0/b + 1.0/c + 1.0/d)
        ci_low  = math.exp(lnOR - 1.96*SE)
        ci_high = math.exp(lnOR + 1.96*SE)
        z = lnOR/SE if SE > 0 else float("nan")
        phi = lambda z: 0.5 * (1 + math.erf(z / math.sqrt(2)))
        p = 2*(1 - phi(abs(z))) if SE > 0 else None

        return {
            "params": {
                "years": [year_from, year_to],
                "source": source,
                "exposure_cut": exposure_cut,
                "outcome_cut": outcome_cut,
                "exposure_q": exposure_q if exposure_cut=="percentile" else None,
                "outcome_q": outcome_q if outcome_cut=="percentile" else None
            },
            "sample": {
                "n_months": len(data),
                "a": a, "b": b, "c": c, "d": d,
                "haldane_anscombe_correction": haldane
            },
            "thresholds": {
                "pm25_cut": cut_pm,
                "ira_u5_cut": cut_ira
            },
            "results": {
                "odds_ratio": OR,
                "ci95_lower": ci_low,
                "ci95_upper": ci_high,
                "z_wald": z,
                "p_value_two_tailed": p,
                "reject_H0_at_alpha_0.05": (ci_low>1 or ci_high<1)
            },
            "unit": "city-month",
            "months_preview": months[:12]  # primeros 12 para inspección rápida
        }
    finally:
        s.close()

@app.get("/stats/or_pm25_mortalidad_u5_city")
def or_pm25_mortalidad_u5_city(
    year_from: int = Query(2020),
    year_to: int = Query(2024),
    source: Optional[Literal["IDEAM","SISAIRE","ALL"]] = Query("ALL"),
    exposure_cut: Literal["median","mean"] = Query("median"),
    outcome_cut: Literal["median","mean","gt0"] = Query("median"),
):
    if year_to < year_from:
        raise HTTPException(status_code=400, detail="year_to debe ser >= year_from")

    s = SessionLocal()
    try:
        # PM2.5 promedio anual a nivel ciudad
        q_pm = (
            s.query(
                models.Dim_Fecha.ano.label("ano"),
                func.avg(models.Hecho_Salud_Ambiental.promedio_pm25).label("pm25_mean")
            )
            .join(models.Dim_Fecha, models.Dim_Fecha.id_fecha==models.Hecho_Salud_Ambiental.id_fecha)
            .join(models.Dim_Fuente, models.Dim_Fuente.id_fuente==models.Hecho_Salud_Ambiental.id_fuente)
            .filter(models.Dim_Fecha.ano.between(year_from, year_to))
            .filter(models.Hecho_Salud_Ambiental.promedio_pm25.isnot(None))
            .group_by(models.Dim_Fecha.ano)
        )
        if source in ("IDEAM","SISAIRE"):
            q_pm = q_pm.filter(models.Dim_Fuente.entidad == source)
        pm = {r.ano: float(r.pm25_mean) for r in q_pm.all() if r.pm25_mean is not None}

        # Mortalidad IRA <5 a nivel ciudad
        q_m = (
            s.query(
                models.Dim_Fecha.ano.label("ano"),
                func.sum(func.coalesce(models.Hecho_Infeccioso.casos_mortalidad_ira,0)).label("mort_ira_u5")
            )
            .join(models.Dim_Fecha, models.Dim_Fecha.id_fecha==models.Hecho_Infeccioso.id_fecha)
            .join(models.Dim_Poblacion, models.Dim_Poblacion.id_poblacion==models.Hecho_Infeccioso.id_poblacion)
            .filter(models.Dim_Poblacion.edad=="Menor 5 anos")
            .filter(models.Dim_Fecha.ano.between(year_from, year_to))
            .group_by(models.Dim_Fecha.ano)
        )
        mort = {r.ano: int(r.mort_ira_u5 or 0) for r in q_m.all()}

        years = sorted(set(pm.keys()) & set(mort.keys()))
        data = [{"ano": y, "pm25_mean": pm[y], "mort_ira_u5": mort[y]} for y in years]
        if not data:
            raise HTTPException(status_code=404, detail="No hay datos combinados a nivel ciudad para el rango dado.")

        pm_vals = [d["pm25_mean"] for d in data]
        mort_vals = [d["mort_ira_u5"] for d in data]

        cut_pm = median(pm_vals) if exposure_cut=="median" else mean(pm_vals)
        if outcome_cut == "median":
            cut_m = median(mort_vals); mort_high = lambda v: v > cut_m
        elif outcome_cut == "mean":
            cut_m = mean(mort_vals);   mort_high = lambda v: v > cut_m
        else:
            cut_m = 0.0;               mort_high = lambda v: v > 0
        pm_high = lambda v: v > cut_pm

        a=b=c=d=0.0
        for row in data:
            e = pm_high(row["pm25_mean"])
            o = mort_high(row["mort_ira_u5"])
            if e and o:          a += 1
            elif e and not o:    b += 1
            elif (not e) and o:  c += 1
            else:                d += 1

        haldane = (a==0 or b==0 or c==0 or d==0)
        if haldane:
            a+=0.5; b+=0.5; c+=0.5; d+=0.5

        OR = (a*d)/(b*c)
        lnOR = math.log(OR)
        SE = math.sqrt(1.0/a + 1.0/b + 1.0/c + 1.0/d)
        ci_low  = math.exp(lnOR - 1.96*SE)
        ci_high = math.exp(lnOR + 1.96*SE)
        z = lnOR/SE if SE>0 else float("nan")
        phi = lambda z: 0.5 * (1 + math.erf(z/math.sqrt(2)))
        p = 2*(1 - phi(abs(z))) if SE>0 else None

        return {
            "params": {"years":[year_from,year_to], "source":source, "exposure_cut":exposure_cut, "outcome_cut":outcome_cut},
            "sample": {"n_years": len(data), "years_included": years, "a": a, "b": b, "c": c, "d": d, "haldane": haldane},
            "thresholds": {"pm25_cut": cut_pm, "mortality_cut": cut_m},
            "results": {
                "odds_ratio": OR,
                "ci95_lower": ci_low,
                "ci95_upper": ci_high,
                "z_wald": z,
                "p_value_two_tailed": p,
                "reject_H0_at_alpha_0.05": (ci_low>1 or ci_high<1)
            },
            "note": "Unidad: ciudad-año (agregación de todas las localidades)."
        }
    finally:
        s.close()