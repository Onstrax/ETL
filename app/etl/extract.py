from pathlib import Path
import pandas as pd

DATA_DIR = Path("data")

def list_raw_files():
    return sorted(DATA_DIR.glob("*.csv"))

def read_any(path: Path) -> pd.DataFrame:
    name = path.name.lower()

    # Archivos osb_: separador fijo ';'
    if name.startswith("osb_"):
        for enc in ("utf-8", "latin-1"):
            try:
                return pd.read_csv(path, sep=";", engine="python", dtype=str, encoding=enc)
            except Exception:
                continue
        # último intento
        return pd.read_csv(path, sep=";", engine="python", dtype=str)

    # SISAIRE y Z*: coma; probamos utf-8 y latin-1
    for enc in ("utf-8", "latin-1"):
        try:
            return pd.read_csv(path, sep=",", engine="python", dtype=str, encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path, sep=",", engine="python", dtype=str)
