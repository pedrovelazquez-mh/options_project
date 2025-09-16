
import pandas as pd
import os
import re
import numpy as np
import sys 
import config_options as cfg
chosen_day = cfg.chosen_day

def cargar_csv_por_dia(cfg.carpeta, chosen_day):
    files = [f for f in os.listdir(carpeta) if chosen_day in f and f.endswith(".csv")]
    if files:
        full_path = os.path.join(cfg.carpeta, files[0])
        return pd.read_csv(full_path)
    else:
        print(f"Día {chosen_day} no encontrado en {cfg.carpeta}. Revisa la carpeta")
        return None
data_base = cargar_csv_por_dia(cfg.carpeta_md_opciones, chosen_day)
data_sub  = cargar_csv_por_dia(cfg.carpeta_md_subyacente, chosen_day)

underlying_close=pd.read_csv(direc_underlying_closes)


underlying_close["date"]=pd.to_datetime(underlying_close["date"])
underlying_close = underlying_close.loc[underlying_close['date'] == pd.to_datetime(chosen_day), "close"].iloc[0]

data_base["instrument"] = data_base["id_simbolo"].str.extract(r"GFG([CV])")
data_base["instrument"] = data_base["instrument"].map({"C": "call", "V": "put"})
def extraer_strike(row, S):
    match = re.search(r"GFG[CV](\d+)", row["id_simbolo"])
    if not match:
        return pd.Series([np.nan, np.nan, np.nan])

    K_raw = int(match.group(1))
    ultimo = float(row["ultimo_precio"])
    inst = row["instrument"]

    # Strike alternativo con coma
    K_b = K_raw / 10.0           

    # Valor intrínseco con strike sin coma
    if inst == "call":
        VI_raw = max(S - K_raw, 0)
    else:
        VI_raw = max(K_raw - S, 0)

    tol_pct = 0.15

    #  Filtro: nunca más de 6x el subyacente
    if K_raw > 3 * S:
        strike = K_b
        if inst == "call":
            VI = max(S - strike, 0)
        else:
            VI = max(strike - S, 0)

    # Condición de tolerancia sobre el strike sin coma
    elif ultimo >= (1 - tol_pct) * VI_raw:
        strike = float(K_raw)
        VI = VI_raw

    # Caso alternativo: usar strike con coma
    else:
        strike = K_b
        if inst == "call":
            VI = max(S - strike, 0)
        else:
            VI = max(strike - S, 0)

    VT = ultimo - VI
    return pd.Series([strike, VI, VT])

data_base[["strike", "valor_intrinseco", "valor_tiempo"]] = data_base.apply(
    lambda row: extraer_strike(row, underlying_close), axis=1)

def extraer_vencimiento(texto):
    base = re.search(r'([A-Z0-9]+)\s*-\s*24hs', texto)
    if base:
        ultimos = base.group(1)[-2:]  # últimos dos caracteres
        # Si ambos son letras → devolver ambos
        if ultimos.isalpha():
            return ultimos
        # Si uno es número y el otro letra → devolver solo la letra
        else:
            return ''.join([c for c in ultimos if c.isalpha()])
    return None

data_base["vencimiento"] = data_base["id_simbolo"].apply(extraer_vencimiento)
# data_base["vencimiento"].unique()
