from pathlib import Path
import hashlib
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import select
from ..db import SessionLocal
from .. import models
from .extract import list_raw_files, read_any
from .transform import unify_all

CTL_PATH = Path("data/.etl_control.csv")
DEFAULT_LOCALIDAD = "Bogotá D.C"
# Población por defecto para hechos ambientales (no aplica)
DEFAULT_POBLACION = {
    "sexo": "masculino",            # placeholder obligatorio por tu modelo
    "menor_5_anos": False,
    "regimen_seguridad_social": "NA"
}

def file_checksum(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def load_control() -> pd.DataFrame:
    if CTL_PATH.exists():
        return pd.read_csv(CTL_PATH)
    return pd.DataFrame(columns=["path","checksum"])

def save_control(df: pd.DataFrame):
    df.to_csv(CTL_PATH, index=False)

def upsert_dim_value(db: Session, model, unique_filter: dict, defaults: dict):
    obj = db.execute(select(model).filter_by(**unique_filter)).scalar_one_or_none()
    if obj:
        for k, v in defaults.items():
            setattr(obj, k, v)
        db.add(obj)
        db.flush()
        return obj
    obj = model(**{**unique_filter, **defaults})
    db.add(obj)
    db.flush()
    return obj

def fk_id(db: Session, model, filter_by: dict, id_col: str):
    obj = db.execute(select(model).filter_by(**filter_by)).scalar_one_or_none()
    return getattr(obj, id_col) if obj else None

def run_etl():
    ctl = load_control()
    files = list_raw_files()
    to_process = []
    for f in files:
        cs = file_checksum(f)
        row = ctl[ctl["path"] == str(f)]
        if row.empty or row.iloc[0]["checksum"] != cs:
            to_process.append((f, cs))

    if not to_process:
        return {"processed": 0, "message": "Sin cambios"}

    files_and_dfs = [(p, read_any(p)) for p, _ in to_process]
    salud_ambiental, morbilidad, mortalidad = unify_all(files_and_dfs)

    with SessionLocal() as db:
        # FUENTES (igual)
        FUENTE_IDEAM    = {"entidad":"IDEAM",      "confiabilidad":"Oficial", "tipo_dato":"Sensor"}
        FUENTE_SISAIRE  = {"entidad":"SISAIRE",    "confiabilidad":"Oficial", "tipo_dato":"Sensor"}
        FUENTE_SALUD    = {"entidad":"SALUD_DATA", "confiabilidad":"Oficial", "tipo_dato":"Vigilancia"}
        for f in (FUENTE_IDEAM, FUENTE_SISAIRE, FUENTE_SALUD):
            upsert_dim_value(db, models.Dim_Fuente, f, {})
        db.commit()

        def id_fuente_from_tag(tag: str):
            if tag == "IDEAM": return fk_id(db, models.Dim_Fuente, FUENTE_IDEAM, "id_fuente")
            if tag == "SISAIRE": return fk_id(db, models.Dim_Fuente, FUENTE_SISAIRE, "id_fuente")
            return fk_id(db, models.Dim_Fuente, FUENTE_SALUD, "id_fuente")

        # DIMS desde SALUD_AMBIENTAL
        if salud_ambiental is not None and not salud_ambiental.empty:
            # Estación
            for name in salud_ambiental["nombre_estacion"].dropna().drop_duplicates().tolist():
                upsert_dim_value(db, models.Dim_Estacion, {"nombre": str(name).strip()}, {})

            # Ubicación
            for loc in salud_ambiental["localidad"].fillna("Bogota D C").drop_duplicates().tolist():
                upsert_dim_value(db, models.Dim_Ubicacion, {"localidad": str(loc).strip()}, {})

            # Fechas (todas las diarias detectadas en ambientales)
            fechas = pd.to_datetime(salud_ambiental["fecha"], errors="coerce")
            df_fecha = (
                pd.DataFrame({
                    "fecha": fechas.dt.date,
                    "dia": fechas.dt.day,
                    "mes": fechas.dt.month,
                    "ano": fechas.dt.year
                })
                .dropna()
                .drop_duplicates()
            )
            for _, r in df_fecha.iterrows():
                upsert_dim_value(
                    db, models.Dim_Fecha,
                    {"fecha": r["fecha"]},
                    {"dia": int(r["dia"]), "mes": int(r["mes"]), "ano": int(r["ano"])}
                )
            db.commit()

        # DIMS desde MORBILIDAD
        if not morbilidad.empty:
            fechas = pd.to_datetime(morbilidad["fecha"], errors="coerce")
            df_fecha = pd.DataFrame({"fecha": fechas.dt.date, "dia": fechas.dt.day, "mes": fechas.dt.month, "ano": fechas.dt.year}).dropna().drop_duplicates()
            for _, r in df_fecha.iterrows():
                upsert_dim_value(db, models.Dim_Fecha, {"fecha": r["fecha"]}, {"dia": int(r["dia"]), "mes": int(r["mes"]), "ano": int(r["ano"])})
            for loc in morbilidad["localidad"].dropna().drop_duplicates().tolist():
                upsert_dim_value(db, models.Dim_Ubicacion, {"localidad": loc}, {})
            pops = morbilidad[["sexo","edad","regimen_seguridad_social"]].drop_duplicates()
            for _, r in pops.iterrows():
                upsert_dim_value(db, models.Dim_Poblacion, {
                    "sexo": (str(r.get("sexo")).lower().strip() if pd.notna(r.get("sexo")) and str(r.get("sexo")).strip() != "" else "no_aplica"),
                    "edad": (str(r.get("edad")).strip() if pd.notna(r.get("edad")) and str(r.get("edad")).strip() != "" else "Poblacion general"),
                    "regimen_seguridad_social": (str(r.get("regimen_seguridad_social")).strip() if pd.notna(r.get("regimen_seguridad_social")) and str(r.get("regimen_seguridad_social")).strip() != "" else "NA")
                }, {})
            db.commit()

        # DIMS desde MORTALIDAD
        if not mortalidad.empty:
            fechas = pd.to_datetime(mortalidad["fecha"], errors="coerce")
            df_fecha = pd.DataFrame({"fecha": fechas.dt.date, "dia": fechas.dt.day, "mes": fechas.dt.month, "ano": fechas.dt.year}).dropna().drop_duplicates()
            for _, r in df_fecha.iterrows():
                upsert_dim_value(db, models.Dim_Fecha, {"fecha": r["fecha"]}, {"dia": int(r["dia"]), "mes": int(r["mes"]), "ano": int(r["ano"])})
            for loc in mortalidad["localidad"].dropna().drop_duplicates().tolist():
                upsert_dim_value(db, models.Dim_Ubicacion, {"localidad": loc}, {})
            pops = mortalidad[["sexo","edad","regimen_seguridad_social"]].drop_duplicates()
            for _, r in pops.iterrows():
                upsert_dim_value(db, models.Dim_Poblacion, {
                    "sexo": (str(r.get("sexo")).lower().strip() if pd.notna(r.get("sexo")) and str(r.get("sexo")).strip() != "" else "no_aplica"),
                    "edad": (str(r.get("edad")).strip() if pd.notna(r.get("edad")) and str(r.get("edad")).strip() != "" else "Poblacion general"),
                    "regimen_seguridad_social": (str(r.get("regimen_seguridad_social")).strip() if pd.notna(r.get("regimen_seguridad_social")) and str(r.get("regimen_seguridad_social")).strip() != "" else "NA")
                }, {})
            db.commit()

        # Helpers
        def id_fecha(d):   return fk_id(db, models.Dim_Fecha, {"fecha": pd.to_datetime(d).date()}, "id_fecha")
        def id_est(n):     return fk_id(db, models.Dim_Estacion, {"nombre": n}, "id_estacion")
        def id_loc(l):     return fk_id(db, models.Dim_Ubicacion, {"localidad": l}, "id_ubicacion")
        def id_pob(sexo, edad, reg):
            return fk_id(db, models.Dim_Poblacion, {
                "sexo": (str(sexo).lower().strip() if pd.notna(sexo) and str(sexo).strip() != "" else "no_aplica"),
                "edad": (str(edad).strip() if pd.notna(edad) and str(edad).strip() != "" else "Poblacion general"),
                "regimen_seguridad_social": (str(reg).strip() if pd.notna(reg) and str(reg).strip() != "" else "NA")
            }, "id_poblacion")

        # Hecho_Salud_Ambiental (sin cambios en lógica)

        # Hecho_Salud_Ambiental
        if salud_ambiental is not None and not salud_ambiental.empty:
            # compactar por si acaso (misma clave → promediamos)
            key_cols = ["fecha", "nombre_estacion", "localidad", "source"]
            value_cols = [c for c in salud_ambiental.columns if c not in key_cols]
            if value_cols:
                salud_ambiental = (
                    salud_ambiental
                    .groupby(key_cols, dropna=False)[value_cols]
                    .mean()
                    .reset_index()
                )

            for _, r in salud_ambiental.iterrows():
                fecha_id = id_fecha(r["fecha"])
                est_id   = id_est(r["nombre_estacion"])
                loc_id   = id_loc(r["localidad"])
                fuente_id= id_fuente_from_tag(str(r.get("source")))

                key = {
                    "id_fecha": fecha_id,
                    "id_fuente": fuente_id,
                    "id_ubicacion": loc_id,
                    "id_estacion": est_id,
                }

                stmt = select(models.Hecho_Salud_Ambiental).filter_by(**key)
                obj = db.execute(stmt).scalar_one_or_none()
                if not obj:
                    obj = models.Hecho_Salud_Ambiental(**key)
                    db.add(obj)

                for m in [
                    "promedio_pm25","promedio_pm10","promedio_co","promedio_so2","promedio_o3",
                    "promedio_temperatura","promedio_humedad","promedio_precipitacion",
                    "promedio_velocidad_viento","promedio_direccion_viento"
                ]:
                    if m in salud_ambiental.columns and pd.notna(r.get(m)):
                        setattr(obj, m, float(r[m]))
            db.commit()

        # Hecho_Infeccioso — MORBILIDAD (semanal)
        if not morbilidad.empty:
            fuente_id = id_fuente_from_tag("SALUD_DATA")
            rows = morbilidad.copy()
            for _, r in rows.iterrows():
                key = {
                    "id_fecha": id_fecha(r["fecha"]),
                    "id_fuente": fuente_id,
                    "id_ubicacion": id_loc(r["localidad"]),
                    "id_poblacion": id_pob(r.get("sexo"), r.get("edad"), r.get("regimen_seguridad_social")),
                }
                stmt = select(models.Hecho_Infeccioso).filter_by(**key)
                obj = db.execute(stmt).scalar_one_or_none()
                if not obj:
                    obj = models.Hecho_Infeccioso(**key)
                    db.add(obj)
                v = r.get("casos_morbilidad_ira")
                if pd.notna(v):
                    obj.casos_morbilidad_ira = int(float(v))
            db.commit()

        # Hecho_Infeccioso — MORTALIDAD (anual)
        if not mortalidad.empty:
            fuente_id = id_fuente_from_tag("SALUD_DATA")
            rows = mortalidad.copy()
            for _, r in rows.iterrows():
                key = {
                    "id_fecha": id_fecha(r["fecha"]),
                    "id_fuente": fuente_id,
                    "id_ubicacion": id_loc(r["localidad"]),
                    "id_poblacion": id_pob(r.get("sexo"), r.get("edad"), r.get("regimen_seguridad_social")),
                }
                stmt = select(models.Hecho_Infeccioso).filter_by(**key)
                obj = db.execute(stmt).scalar_one_or_none()
                if not obj:
                    obj = models.Hecho_Infeccioso(**key)
                    db.add(obj)
                v = r.get("casos_mortalidad_ira")
                if pd.notna(v):
                    obj.casos_mortalidad_ira = int(float(v))
            db.commit()

        # Marcar archivos como procesados
        for p, cs in to_process:
            ctl = ctl[ctl["path"] != str(p)]
            ctl = pd.concat([ctl, pd.DataFrame([{"path": str(p), "checksum": cs}])], ignore_index=True)
        save_control(ctl)

    return {"processed": len(to_process), "message": "OK"}
