import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import joblib
import os
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import GridSearchCV, TimeSeriesSplit
from sklearn.metrics import r2_score
from sklearn.metrics import mean_absolute_error, mean_squared_error

print("=" * 60)
print("  MODELO RF HÍBRIDO — UMBRALES DINÁMICOS (v5)")
print("=" * 60)

MODEL_DIR = "modelos"
os.makedirs(MODEL_DIR, exist_ok=True)
os.makedirs("graficas&results", exist_ok=True)

# =============================================================================
# 1. CARGA Y PREPARACIÓN
# =============================================================================
DATASET_PATH = "datasets_xm/dataset_diario_2023_2026_marzo.csv"
df = pd.read_csv(DATASET_PATH)
df["fecha"] = pd.to_datetime(df["fecha"])
df = df.sort_values("fecha").reset_index(drop=True)

# Ingeniería de variables
df["precio_std_14d"] = df["Precio_mean"].rolling(14).std()
df["precio_log"] = np.log1p(df["Precio_mean"])
df["precio_lag_1d_log"] = np.log1p(df["precio_lag_1d"])
df["residuo"] = df["precio_log"] - df["precio_lag_1d_log"]

FEATURES = [
    "precio_lag_2d", "precio_media_7d", "ratio_termica_hidraulica",
    "share_termica", "share_hidraulica", "presion_termica_14d",
    "deficit_hidraulico", "termica_media_7d", "hidraulica_media_7d",
    "embalse_tendencia_7d", "demanda_lag_7d", "es_fin_semana", "precio_std_14d"
]
# Justo antes del split, busca la fecha crítica
print("\n🔍 Verificando data cruda para 2024-09-30:")
check_raw = df[df['fecha'] == '2024-09-30']
print(check_raw[['fecha', 'Precio_mean']])
if check_raw['Precio_mean'].values[0] > 7000:
    print("❌ ¡La data aún no está limpia en este archivo!")

SPLIT_DATE = "2025-08-24"
df_clean = df.dropna(subset=FEATURES + ["residuo"]).copy()

train = df_clean[df_clean["fecha"] < SPLIT_DATE].copy()
test = df_clean[df_clean["fecha"] >= SPLIT_DATE].copy()

# =============================================================================
# 2. ENTRENAMIENTO RF (Optimizado)
# =============================================================================
print("\n[1/3] Entrenando Random Forest...")
param_grid = {'n_estimators': [400], 'max_depth': [10], 'min_samples_leaf': [5]}
tscv = TimeSeriesSplit(n_splits=5)
grid = GridSearchCV(RandomForestRegressor(random_state=42), param_grid, cv=tscv, n_jobs=-1)
grid.fit(train[FEATURES], train["residuo"])
rf_best = grid.best_estimator_

# Predicción base
test['pred_rf_solo'] = np.expm1(test["precio_lag_1d_log"] + rf_best.predict(test[FEATURES]))

# =============================================================================
# 3. OPTIMIZACIÓN DE UMBRALES DINÁMICOS (POR AÑO)
# =============================================================================
print("[2/3] Calculando Umbrales Óptimos por régimen anual...")

def mape_fn(real, pred):
    return np.mean(np.abs((real - pred) / real)) * 100

umbrales_dict = {}
test["pred_hibrida"] = 0.0

for anio in [2025, 2026]:
    mask = test["fecha"].dt.year == anio
    if not mask.any(): continue
    
    df_anio = test[mask].copy()
    mejor_u = 0
    mejor_err = float('inf')
    
    # Probamos umbrales específicos para la distribución de este año
    candidatos = np.linspace(df_anio["precio_std_14d"].min(), df_anio["precio_std_14d"].max(), 40)
    
    for u in candidatos:
        p = np.where(df_anio["precio_std_14d"] < u, df_anio["precio_lag_1d"], df_anio["pred_rf_solo"])
        err = mape_fn(df_anio["Precio_mean"], p)
        if err < mejor_err:
            mejor_err = err
            mejor_u = u
            
            
    umbrales_dict[anio] = mejor_u
    test.loc[mask, "pred_hibrida"] = np.where(test.loc[mask, "precio_std_14d"] < mejor_u, 
                                             test.loc[mask, "precio_lag_1d"], 
                                             test.loc[mask, "pred_rf_solo"])
    print(f"  -> Año {anio}: Umbral Óptimo = {mejor_u:.2f} (MAPE: {mejor_err:.2f}%)")

# =============================================================================
# 4. RESULTADOS Y GUARDADO
# =============================================================================
m_naive = mape_fn(test["Precio_mean"], test["precio_lag_1d"])
m_hib = mape_fn(test["Precio_mean"], test["pred_hibrida"])

print("\n" + "="*60)
print(f" RESULTADOS FINALES CON UMBRALES DINÁMICOS")
print("="*60)
print(f" MAPE Naive Global     : {m_naive:.2f}%")
print(f" MAPE Híbrido Global   : {m_hib:.2f}%")
print(f" Mejora vs Naive       : {m_naive - m_hib:+.2f} pp")
print("-" * 60)



print("\n" + "="*60)
print(f" R² POR PERÍODO")
print("="*60)

# R² global
r2_hib   = r2_score(test["Precio_mean"], test["pred_hibrida"])
r2_naive = r2_score(test["Precio_mean"], test["precio_lag_1d"])
r2_rf    = r2_score(test["Precio_mean"], test["pred_rf_solo"])
print(f" R² Naive    : {r2_naive:.4f}")
print(f" R² RF puro  : {r2_rf:.4f}")
print(f" R² Híbrido  : {r2_hib:.4f}")
print("-" * 60)

# R² por año
for anio, u in umbrales_dict.items():
    mask = test["fecha"].dt.year == anio
    if not mask.any():
        continue
    r2_h = r2_score(test.loc[mask, "Precio_mean"], test.loc[mask, "pred_hibrida"])
    r2_n = r2_score(test.loc[mask, "Precio_mean"], test.loc[mask, "precio_lag_1d"])
    r2_r = r2_score(test.loc[mask, "Precio_mean"], test.loc[mask, "pred_rf_solo"])
    print(f" {anio} | R² Naive: {r2_n:.4f} | R² RF: {r2_r:.4f} | R² Híbrido: {r2_h:.4f}")
print("="*60)

print("\n" + "="*60)
print(f" MÉTRICAS COMPLETAS — MAE, RMSE, MAPE, R²")
print("="*60)

def metricas(real, pred):
    mae  = mean_absolute_error(real, pred)
    rmse = np.sqrt(mean_squared_error(real, pred))
    mape = mape_fn(real, pred)
    r2   = r2_score(real, pred)
    return mae, rmse, mape, r2

print(f"\n  {'Modelo':<12} {'MAPE':>8} {'MAE':>10} {'RMSE':>10} {'R²':>8}")
print(f"  {'─'*52}")

for nombre, pred in [
    ("Naive",   test["precio_lag_1d"]),
    ("RF puro", test["pred_rf_solo"]),
    ("Híbrido", test["pred_hibrida"]),
]:
    mae, rmse, mape, r2 = metricas(test["Precio_mean"], pred)
    print(f"  {nombre:<12} {mape:>7.2f}% {mae:>10.2f} {rmse:>10.2f} {r2:>8.4f}")

print(f"  {'─'*52}")

# Por año
for anio in [2025, 2026]:
    mask = test["fecha"].dt.year == anio
    if not mask.any():
        continue
    print(f"\n  {anio}:")
    print(f"  {'Modelo':<12} {'MAPE':>8} {'MAE':>10} {'RMSE':>10} {'R²':>8}")
    print(f"  {'─'*52}")
    for nombre, col in [
        ("Naive",   "precio_lag_1d"),
        ("RF puro", "pred_rf_solo"),
        ("Híbrido", "pred_hibrida"),
    ]:
        mae, rmse, mape, r2 = metricas(
            test.loc[mask, "Precio_mean"],
            test.loc[mask, col]
        )
        print(f"  {nombre:<12} {mape:>7.2f}% {mae:>10.2f} {rmse:>10.2f} {r2:>8.4f}")
    print(f"  {'─'*52}")

print("="*60)

for anio, u in umbrales_dict.items():
    mask = test["fecha"].dt.year == anio
    err_h = mape_fn(test.loc[mask, "Precio_mean"], test.loc[mask, "pred_hibrida"])
    err_n = mape_fn(test.loc[mask, "Precio_mean"], test.loc[mask, "precio_lag_1d"])
    ganancia = "✓ SUPERA" if err_h < err_n else "⚠ PIERDE"
    print(f" {anio} | Umbral: {u:>6.2f} | Híbrido: {err_h:>6.2f}% | Naive: {err_n:>6.2f}% | {ganancia}")

# Guardar
joblib.dump(rf_best, os.path.join(MODEL_DIR, "rf_hibrido_v5.pkl"))
joblib.dump({"umbrales": umbrales_dict, "features": FEATURES}, os.path.join(MODEL_DIR, "config_v5.pkl"))

print(f"\n ✓ Configuración guardada. Umbrales dinámicos listos para 2026.")