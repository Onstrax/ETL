import pandas as pd
import numpy as np
from pathlib import Path

DEFAULT_LOCALIDAD = "Bogotá D.C"

def _to_datetime_safe(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce")

# ---------------------------
#  Sensores ambientales
# ---------------------------
def _standardize_env(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """Z *.csv (IDEAM) -> promedio diario por fecha/estación/localidad + source='IDEAM'"""
    df = df.copy()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    need = {"fechaobservacion", "valorobservado", "nombreestacion"}
    if not need.issubset(set(df.columns)):
        # variaciones de nombre
        alias = {
            "fechaobservacion": [c for c in df.columns if c in ["fecha","fecha_observacion","fechaobservacion"]],
            "valorobservado": [c for c in df.columns if c in ["valor","valor_observado","valorobservado"]],
            "nombreestacion": [c for c in df.columns if c in ["nombreestacion","estacion","estación","nombre_estacion"]],
        }
        for k, alts in alias.items():
            if alts and k not in df.columns:
                df[k] = df[alts[0]]

    if not {"fechaobservacion","valorobservado","nombreestacion"}.issubset(set(df.columns)):
        return pd.DataFrame(columns=["fecha","nombre_estacion","localidad","source"])

    df["fecha"] = pd.to_datetime(
        df["fechaobservacion"],
        format="%Y %b %d %I:%M:%S %p",
        errors="coerce"
    ).dt.date
    df["nombre_estacion"] = df["nombreestacion"].astype(str).str.strip()
    # Municipio como localidad si existe
    loc_col = "municipio" if "municipio" in df.columns else None
    df["localidad"] = df[loc_col].astype(str).str.strip() if loc_col else DEFAULT_LOCALIDAD
    df["valor"] = pd.to_numeric(df["valorobservado"], errors="coerce")

    lower = filename.lower()
    measure = None
    if "precipit" in lower:
        measure = "promedio_precipitacion"
    elif "temperatura" in lower:
        measure = "promedio_temperatura"
    elif "humedad" in lower:
        measure = "promedio_humedad"
    elif "velocidad" in lower:
        measure = "promedio_velocidad_viento"
    elif "direcci" in lower:
        measure = "promedio_direccion_viento"
    else:
        return pd.DataFrame(columns=["fecha","nombre_estacion","localidad","source"])

    g = (df.dropna(subset=["fecha"])
           .groupby(["fecha","nombre_estacion","localidad"], dropna=False)["valor"]
           .mean()
           .reset_index()
           .rename(columns={"valor": measure}))
    g["source"] = "IDEAM"
    return g

def _standardize_sisaire(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """reporte_sisaire_*.csv (SISAIRE) -> promedio diario + source='SISAIRE'"""
    df = df.copy()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    if "estacion" not in df or "fecha_inicial" not in df:
        return pd.DataFrame(columns=["fecha","nombre_estacion","localidad","source"])

    df["nombre_estacion"] = df["estacion"].astype(str).str.replace('"', "").str.strip()
    df["fecha"] = pd.to_datetime(
        df["fecha_inicial"],
        format="%Y-%m-%d",
        errors="coerce"
    ).dt.date
    df["localidad"] = DEFAULT_LOCALIDAD

    lower = filename.lower()
    if "_co" in lower:
        val_col, measure = "co", "promedio_co"
    elif "_o3" in lower:
        val_col, measure = "o3", "promedio_o3"
    elif "_pm10" in lower:
        val_col, measure = "pm10", "promedio_pm10"
    elif "_pm25" in lower or "pm2.5" in lower:
        val_col, measure = "pm2.5", "promedio_pm25"
    elif "_so2" in lower:
        val_col, measure = "so2", "promedio_so2"
    else:
        return pd.DataFrame(columns=["fecha","nombre_estacion","localidad","source"])

    # encontrar columna con ese nombre (case-insensitive)
    if val_col not in df:
        alt = [c for c in df.columns if c.lower() == val_col]
        if alt:
            val_col = alt[0]
        else:
            return pd.DataFrame(columns=["fecha","nombre_estacion","localidad","source"])

    df[measure] = pd.to_numeric(df[val_col], errors="coerce")
    g = (df.dropna(subset=["fecha"])
           .groupby(["fecha","nombre_estacion","localidad"], dropna=False)[measure]
           .mean()
           .reset_index())
    g["source"] = "SISAIRE"
    return g

# ---------------------------
#  Morbilidad (conteos por año)
# ---------------------------
def _standardize_osb_count(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """
    osb_enf_trans_neumonia.csv  -> cuenta filas por ANO/LOCALIDAD/SEXO/REGIMEN => casos_neumonia ; menor_5_anos=False
    osb_enf_transm_ira5anos.csv -> cuenta filas por ANO/LOCALIDAD/SEXO/REGIMEN => casos_ira      ; menor_5_anos=True
    Fecha = 1-ene-ANO (sin día/mes reales). No hay estación.
    """
    import numpy as np
    df = df.copy()
    df.columns = [c.strip().upper() for c in df.columns]

    required = {"ANO", "LOCALIDAD", "SEXO", "REGIMEN_SEGURIDAD_SOCIAL"}
    if not required.issubset(set(df.columns)):
        # si faltan columnas clave, devolvemos DF vacío con esquema esperado
        return pd.DataFrame(columns=["fecha","localidad","sexo","menor_5_anos","regimen_seguridad_social","casos_ira","casos_neumonia"])

    # Normalizaciones
    df["fecha"] = pd.to_datetime(df["ANO"].astype(str) + "-01-01", errors="coerce").dt.date
    df["localidad"] = df["LOCALIDAD"].astype(str).str.strip()

    # SEXO -> masculino/femenino (si viene algo raro, lo dejamos tal cual en minúsculas)
    df["SEXO_NORM"] = (
        df["SEXO"].astype(str).str.strip().str.lower()
          .map({"m":"masculino","masculino":"masculino","f":"femenino","femenino":"femenino"})
          .fillna(df["SEXO"].astype(str).str.strip().str.lower())
    )

    # REGIMEN tal cual (limpio), sin inventar NA si viene uno real
    df["REGIMEN_NORM"] = df["REGIMEN_SEGURIDAD_SOCIAL"].astype(str).str.strip()

    lower = filename.lower()
    if "ira5anos" in lower:
        target_col = "casos_ira"
        menor5 = True
    elif "neumonia" in lower:
        target_col = "casos_neumonia"
        menor5 = False
    else:
        return pd.DataFrame(columns=["fecha","localidad","sexo","menor_5_anos","regimen_seguridad_social","casos_ira","casos_neumonia"])

    # Conteo por año/localidad/sexo/regimen
    g = (df.groupby(["fecha","localidad","SEXO_NORM","REGIMEN_NORM"], dropna=False)
           .size()
           .reset_index(name=target_col))

    g.rename(columns={"SEXO_NORM":"sexo","REGIMEN_NORM":"regimen_seguridad_social"}, inplace=True)
    g["menor_5_anos"] = menor5

    # Rellena la otra medida con NaN para facilitar merge
    other = "casos_neumonia" if target_col == "casos_ira" else "casos_ira"
    g[other] = np.nan

    return g[["fecha","localidad","sexo","menor_5_anos","regimen_seguridad_social","casos_ira","casos_neumonia"]]

def standardize_dispatch(df: pd.DataFrame, path: Path):
    name = path.name.lower()
    if name.startswith("z "):                 # IDEAM
        return _standardize_env(df, path.name), None
    if name.startswith("reporte_sisaire_"):   # SISAIRE
        return _standardize_sisaire(df, path.name), None
    if name.startswith("osb_"):               # SALUD_DATA
        return None, _standardize_osb_count(df, path.name)
    return pd.DataFrame(), pd.DataFrame()

def unify_all(files_and_dfs):
    """Combina todas las piezas:
       - Salud ambiental: merge por (fecha, estacion, localidad, source)
       - Morbilidad: suma por (fecha, localidad) y conserva población neutra (no_aplica/NA/flag<5)
    """
    env_parts = []
    morb_parts = []
    for p, df in files_and_dfs:
        env, morb = standardize_dispatch(df, p)
        if env is not None and not env.empty:
            env_parts.append(env)
        if morb is not None and not morb.empty:
            morb_parts.append(morb)

    # Ambientales: merge por keys + source (asignaremos fuente por fila en load)
    salud_ambiental = pd.DataFrame()
    if env_parts:
        salud_ambiental = env_parts[0]
        for part in env_parts[1:]:
            salud_ambiental = salud_ambiental.merge(
                part, on=["fecha","nombre_estacion","localidad","source"], how="outer"
            )

    # Morbilidad: unir neumonía + ira5años
    morbilidad = pd.DataFrame()
    if morb_parts:
        morbilidad = morb_parts[0]
        for part in morb_parts[1:]:
            morbilidad = morbilidad.merge(
                part, on=["fecha","localidad","sexo","menor_5_anos","regimen_seguridad_social"], how="outer"
            )
        # Sumar por año/localidad/población (casos_ira, casos_neumonia)
        agg_cols = [c for c in ["casos_ira","casos_neumonia"] if c in morbilidad.columns]
        morbilidad = (morbilidad
                      .groupby(["fecha","localidad","sexo","menor_5_anos","regimen_seguridad_social"], dropna=False)[agg_cols]
                      .sum(min_count=1)
                      .reset_index())

    # Enriquecer con d/m/a
    def enrich_date(df):
        if df is None or df.empty: return df
        dts = pd.to_datetime(df["fecha"], errors="coerce")
        df["dia"] = dts.dt.day
        df["mes"] = dts.dt.month
        df["ano"] = dts.dt.year
        return df

    salud_ambiental = enrich_date(salud_ambiental)
    morbilidad = enrich_date(morbilidad)

    return salud_ambiental, morbilidad
