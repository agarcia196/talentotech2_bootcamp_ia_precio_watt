# Proyecto CDK Python para EC2 + S3 + DynamoDB

Este proyecto despliega en AWS la app real `preciowatt_v3_flask` sobre una EC2 Amazon Linux con Elastic IP. La aplicacion usa:

- `S3` para artefactos del modelo y raw descargados desde SIMEM
- `DynamoDB` para el contexto diario agregado usado por el predictor
- `DynamoDB` adicional para auditoria de predicciones
- `nginx` + `gunicorn` delante de Flask

## Estructura

- `app.py`: entrypoint de CDK
- `cdk.json`: parametros base
- `infrastructure/`: stacks y bootstrap de EC2
- `preciowatt_v3_flask/`: aplicacion productiva desplegada en EC2
- `flask_app/`: scaffold inicial, ya no forma parte del deploy principal

## Que despliega

### NetworkStack

- VPC minima
- una subnet publica
- Internet Gateway

### DataStack

- bucket S3 privado para:
  - `models/`
  - `config/`
  - `raw/simem/...`
- tabla DynamoDB `daily_feature`
- tabla DynamoDB `prediction_audit`

### ComputeStack

- security group
- IAM role / instance profile
- EC2 Amazon Linux
- Elastic IP
- `nginx`
- servicio `systemd` que arranca `preciowatt_v3_flask`

## Parametros configurables

Se manejan desde `cdk.json`.

- `project_name`
- `aws_region`
- `instance_type`
- `allowed_ssh_cidr`
- `app_port`
- `model_bucket_prefix`
- `model_object_key`
- `config_object_key`
- `raw_prefix`
- `lookback_days`
- `historical_start_date`
- `dataset_anchor_start_date`
- `initial_backfill_start_date`
- `key_name`
- `vpc_cidr`

## Flujo de datos

1. La app carga modelo y config desde S3.
2. Un backfill inicial sincroniza datos desde `2023-01-01` y los materializa en `daily_feature`.
3. Una sincronizacion diaria refresca la ventana operativa reciente.
4. La prediccion lee el contexto desde DynamoDB, recalcula features derivadas y genera el resultado.
5. Registra la auditoria en `prediction_audit`.

## Endpoints principales

- `GET /api/v1/health`
- `GET /api/v1/meta`
- `GET /api/v1/prediccion/status`
- `GET /api/v1/prediccion/historico?dias=60`
- `POST /api/v1/prediccion/diaria`

### Ejemplo de prediccion

```powershell
curl -X POST http://ELASTIC_IP/api/v1/prediccion/diaria `
  -H "Content-Type: application/json" `
  -d "{\"modo\":\"B\",\"dias\":7}"
```

La respuesta incluye metadata operativa:

- `data_source`
- `synced_range`
- `missing_days_filled`
- `ultima_hist_dynamodb`

Semantica de modos:

- `A`: historico previo al dataset base (`2023-01-01` a `2023-07-30`)
- `B`: futuro desde la ultima fecha disponible
- `C`: rango libre segun disponibilidad real

## Carga manual de artefactos

Si quieres cargar el modelo manualmente:

```powershell
aws s3 cp .\rf_hibrido_v5.pkl s3://NOMBRE_DEL_BUCKET/models/rf_hibrido_v5.pkl
aws s3 cp .\config_v5.pkl s3://NOMBRE_DEL_BUCKET/config/config_v5.pkl
```

## Validaciones esperadas

- `cdk synth` debe generar CloudFormation sin errores
- la EC2 debe arrancar `preciowatt_v3_flask`
- `GET /api/v1/health` debe responder `200`
- `POST /api/v1/prediccion/diaria` debe leer contexto ya materializado en DynamoDB
- S3 debe recibir raw de SIMEM
- DynamoDB debe recibir `daily_feature` y auditoria

## Limitaciones de esta fase

- una sola EC2
- solo HTTP publico
- sin ALB
- sin Route 53
- sin ACM
- sin Auto Scaling
- sin jobs desacoplados

La sincronizacion de SIMEM ocurre en la misma EC2, pero fuera del request de prediccion.
