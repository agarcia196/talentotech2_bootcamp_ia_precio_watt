# PrecioWatt Colombia - RF Hibrido v8

Aplicacion Flask del predictor real del precio de bolsa electrica en Colombia.

## Operacion en AWS

Esta version esta preparada para ejecutarse en EC2 y operar con:

- `S3` para `rf_hibrido_v5.pkl`, `config_v5.pkl` y raw descargados desde SIMEM
- `DynamoDB` para el contexto diario agregado usado por el modelo
- `DynamoDB` para auditoria de predicciones

Los archivos de `model_assets/` quedan como bootstrap inicial:

- si el bucket S3 esta vacio, la app sube modelo y config
- si la tabla `daily_feature` esta vacia, la app puede sembrarla desde el CSV historico

Despues de esa siembra inicial, la fuente operativa principal es AWS, no el filesystem local.

## Endpoints

- `GET /api/v1/health`
- `GET /api/v1/meta`
- `GET /api/v1/prediccion/status`
- `GET /api/v1/prediccion/historico?dias=60`
- `POST /api/v1/prediccion/diaria`

## Modos de prediccion

- `A`: historico previo al dataset base (`2023-01-01` a `2023-07-30`)
- `B`: futuro
- `C`: rango libre

## Reglas de sincronizacion

- backfill inicial desde `2023-01-01`
- sincronizacion diaria desacoplada del request
- lookback de features: `60` dias

## Variables de entorno relevantes

- `AWS_REGION`
- `APP_BUCKET_NAME`
- `MODEL_OBJECT_KEY`
- `CONFIG_OBJECT_KEY`
- `RAW_PREFIX`
- `DAILY_FEATURE_TABLE_NAME`
- `PREDICTION_AUDIT_TABLE_NAME`
- `SYNC_STATUS_TABLE_NAME`
- `MISSING_DATA_THRESHOLD_DAYS`
- `LOOKBACK_DAYS`
- `INITIAL_BACKFILL_START_DATE`
- `HISTORICAL_START_DATE`
- `DATASET_ANCHOR_START_DATE`

## Arranque local

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python run.py
```
