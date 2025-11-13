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
    salud_ambiental, morbilidad = unify_all(files_and_dfs)

    with SessionLocal() as db:
        # ----- FUENTES (según tu mapeo) -----
        FUENTE_IDEAM    = {"entidad":"IDEAM",      "confiabilidad":"Oficial", "tipo_dato":"Sensor"}
        FUENTE_SISAIRE  = {"entidad":"SISAIRE",    "confiabilidad":"Oficial", "tipo_dato":"Sensor"}
        FUENTE_SALUD    = {"entidad":"SALUD_DATA", "confiabilidad":"Oficial", "tipo_dato":"Vigilancia"}

        for f in (FUENTE_IDEAM, FUENTE_SISAIRE, FUENTE_SALUD):
            upsert_dim_value(db, models.Dim_Fuente, f, {})
        db.commit()

        def id_fuente_from_tag(tag: str):
            if tag == "IDEAM":
                return fk_id(db, models.Dim_Fuente, FUENTE_IDEAM, "id_fuente")
            if tag == "SISAIRE":
                return fk_id(db, models.Dim_Fuente, FUENTE_SISAIRE, "id_fuente")
            if tag == "SALUD_DATA":
                return fk_id(db, models.Dim_Fuente, FUENTE_SALUD, "id_fuente")
            return None

        # ----- DIMENSIONES DESDE SALUD_AMBIENTAL -----
        if not salud_ambiental.empty:
            # Estacion
            for name in salud_ambiental["nombre_estacion"].dropna().drop_duplicates().tolist():
                upsert_dim_value(db, models.Dim_Estacion, {"nombre": name}, {})
            # Ubicacion
            for loc in salud_ambiental["localidad"].fillna(DEFAULT_LOCALIDAD).drop_duplicates().tolist():
                upsert_dim_value(db, models.Dim_Ubicacion, {"localidad": loc}, {})
            # Fecha
            fechas = pd.to_datetime(salud_ambiental["fecha"], errors="coerce")
            df_fecha = pd.DataFrame({"fecha": fechas.dt.date,
                                     "dia": fechas.dt.day,
                                     "mes": fechas.dt.month,
                                     "ano": fechas.dt.year}).dropna().drop_duplicates()
            for _, r in df_fecha.iterrows():
                upsert_dim_value(db, models.Dim_Fecha,
                                 {"fecha": r["fecha"]},
                                 {"dia": int(r["dia"]), "mes": int(r["mes"]), "ano": int(r["ano"])})
            db.commit()

        # ----- DIMENSIONES DESDE MORBILIDAD -----
        if morbilidad is not None and not morbilidad.empty:
            # Fecha (años)
            fechas = pd.to_datetime(morbilidad["fecha"], errors="coerce")
            df_fecha = pd.DataFrame({"fecha": fechas.dt.date,
                                     "dia": fechas.dt.day,
                                     "mes": fechas.dt.month,
                                     "ano": fechas.dt.year}).dropna().drop_duplicates()
            for _, r in df_fecha.iterrows():
                upsert_dim_value(db, models.Dim_Fecha,
                                 {"fecha": r["fecha"]},
                                 {"dia": int(r["dia"]), "mes": int(r["mes"]), "ano": int(r["ano"])})
            # Ubicacion
            for loc in morbilidad["localidad"].fillna(DEFAULT_LOCALIDAD).drop_duplicates().tolist():
                upsert_dim_value(db, models.Dim_Ubicacion, {"localidad": loc}, {})
            # Poblacion (neutra, viene en morbilidad std)
            pops = morbilidad[["sexo","menor_5_anos","regimen_seguridad_social"]].drop_duplicates()
            for _, r in pops.iterrows():
                sexo = (str(r["sexo"]).lower().strip() if pd.notna(r["sexo"]) and str(r["sexo"]).strip() != "" else "no_aplica")
                menor = (bool(r["menor_5_anos"]) if pd.notna(r["menor_5_anos"]) else False)
                regimen = (str(r["regimen_seguridad_social"]).strip() if pd.notna(r["regimen_seguridad_social"]) and str(r["regimen_seguridad_social"]).strip() != "" else "NA")
                upsert_dim_value(db, models.Dim_Poblacion, {
                    "sexo": sexo,
                    "menor_5_anos": menor,
                    "regimen_seguridad_social": regimen
                }, {})
            db.commit()

        # ----- Helpers IDs -----
        def id_fecha(d):   return fk_id(db, models.Dim_Fecha, {"fecha": pd.to_datetime(d).date()}, "id_fecha")
        def id_est(n):     return fk_id(db, models.Dim_Estacion, {"nombre": n}, "id_estacion")
        def id_loc(l):     return fk_id(db, models.Dim_Ubicacion, {"localidad": l}, "id_ubicacion")
        def id_pob(sexo, menor, reg):
            return fk_id(db, models.Dim_Poblacion, {
                "sexo": (str(sexo).lower().strip() if pd.notna(sexo) and str(sexo).strip() != "" else "no_aplica"),
                "menor_5_anos": bool(menor) if not pd.isna(menor) else False,
                "regimen_seguridad_social": (str(reg).strip() if pd.notna(reg) and str(reg).strip() != "" else "NA")
            }, "id_poblacion")

        # ----- Cargar HECHO_SALUD_AMBIENTAL (sin población) -----
        if not salud_ambiental.empty:
            for _, r in salud_ambiental.iterrows():
                fecha_id = id_fecha(r["fecha"])
                est_id   = id_est(r["nombre_estacion"])
                loc_id   = id_loc(r["localidad"] if pd.notna(r["localidad"]) else DEFAULT_LOCALIDAD)
                fuente_id= id_fuente_from_tag(r.get("source"))

                key = {
                    "id_fecha": fecha_id,
                    "id_fuente": fuente_id,
                    "id_ubicacion": loc_id,
                    "id_estacion": est_id,
                }
                obj = db.get(models.Hecho_Salud_Ambiental, key)
                if not obj:
                    obj = models.Hecho_Salud_Ambiental(**key)
                    db.add(obj)

                for m in ["promedio_pm25","promedio_pm10","promedio_co","promedio_so2","promedio_o3",
                          "promedio_temperatura","promedio_humedad","promedio_precipitacion",
                          "promedio_velocidad_viento","promedio_direccion_viento"]:
                    if m in salud_ambiental.columns and pd.notna(r.get(m)):
                        setattr(obj, m, float(r[m]))
            db.commit()

        # ----- Cargar HECHO_MORBILIDAD (conteo por año/localidad/población neutra) -----
        if morbilidad is not None and not morbilidad.empty:
            fuente_id = id_fuente_from_tag("SALUD_DATA")
            for _, r in morbilidad.iterrows():
                fecha_id = id_fecha(r["fecha"])
                loc_id   = id_loc(r["localidad"] if pd.notna(r["localidad"]) else DEFAULT_LOCALIDAD)
                pob_id   = id_pob(r.get("sexo"), r.get("menor_5_anos"), r.get("regimen_seguridad_social"))

                key = {"id_fecha": fecha_id, "id_fuente": fuente_id, "id_ubicacion": loc_id, "id_poblacion": pob_id}
                obj = db.get(models.Hecho_Morbilidad, key)
                if not obj:
                    obj = models.Hecho_Morbilidad(**key)
                    db.add(obj)

                if "casos_ira" in morbilidad.columns and pd.notna(r.get("casos_ira")):
                    obj.casos_ira = int(r["casos_ira"])
                if "casos_neumonia" in morbilidad.columns and pd.notna(r.get("casos_neumonia")):
                    obj.casos_neumonia = int(r["casos_neumonia"])
            db.commit()

        # Marcar archivos como procesados
        for p, cs in to_process:
            ctl = ctl[ctl["path"] != str(p)]
            ctl = pd.concat([ctl, pd.DataFrame([{"path": str(p), "checksum": cs}])], ignore_index=True)
        save_control(ctl)

    return {"processed": len(to_process), "message": "OK"}
