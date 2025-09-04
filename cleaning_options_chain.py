import pandas as pd
import os
import re
import numpy as np

chosen_day = "2025-05-20"
direc_market_data = r"C:\Users\Pedro\Research\Options\market_data\ggal"
direc_underlying_closes=r"C:\Users\Pedro\Research\Fundamentals\Bloomberg\bbg data\prices\GGAL.BA.csv"



files = [f for f in os.listdir(direc_market_data) if chosen_day in f and f.endswith(".csv")]
if files:
    full_path = os.path.join(direc_market_data, files[0])
    data_base = pd.read_csv(full_path)
else:
    print("Día no encontrado. Revisa la carpeta")
    
underlying_close=pd.read_csv(direc_underlying_closes)
underlying_close["date"]=pd.to_datetime(underlying_close["date"])
underlying_close = underlying_close.loc[underlying_close['date'] == pd.to_datetime(chosen_day), "close"].iloc[0]
data_base["instrument"] = data_base["id_simbolo"].str.extract(r"GFG([CV])")
data_base["instrument"] = data_base["instrument"].map({"C": "call", "V": "put"})


def choose_strike(row, S):
    match = re.search(r"GFG[CV](\d+)", row["id_simbolo"])
    if not match:
        return pd.Series([np.nan, np.nan, np.nan])
    K_raw = int(match.group(1))
    ultimo = float(row["ultimo_precio"])
    inst = row["instrument"]
    K_b = K_raw / 10.0           #primero pruebo en agregarle una coma. Si Ultimo precio<Valor intrinseco, no se la saco. 
    if inst == "call":
        VI_b = max(S - K_b, 0)
    else:
        VI_b = max(K_b - S, 0)
    if ultimo >= VI_b or K_raw > 2*S:        
        strike = K_b        #agrego el caso donde el ultimo precio es apenas menor al valor intrinseco(yo tomo So al cierre.)
        VI = VI_b                     
    else:
        strike = float(K_raw)
        if inst == "call":
            VI = max(S - strike, 0)
        else:
            VI = max(strike - S, 0)
    VT = ultimo - VI
    return pd.Series([strike, VI, VT])
data_base[["strike", "valor_intrinseco", "valor_tiempo"]] = data_base.apply(
    lambda row: choose_strike(row, underlying_close), axis=1)






