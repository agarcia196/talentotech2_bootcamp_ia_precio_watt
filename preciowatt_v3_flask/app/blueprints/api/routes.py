from datetime import datetime, timezone
from uuid import uuid4

from flask import Blueprint, current_app, jsonify, request

api_bp = Blueprint("api", __name__)


@api_bp.get("/health")
def health():
    return jsonify({"status": "ok"})


@api_bp.get("/meta")
def meta():
    return jsonify(
        {
            "app": "PrecioWatt Colombia",
            "modelo": "RF Hibrido v8",
            "dataset": "SIMEM / XM - jul 2023 -> mar 2026",
            "features": 22,
        }
    )


@api_bp.get("/prediccion/status")
def prediccion_status():
    current_app.logger.info("predictor_status_requested")
    status = current_app.predictor.status()
    current_app.logger.info(
        "predictor_status_ready listo=%s ultima_hist_dynamodb=%s",
        status.get("listo"),
        status.get("ultima_hist_dynamodb"),
    )
    return jsonify(status)


@api_bp.get("/prediccion/historico")
def prediccion_historico():
    dias = int(request.args.get("dias", 60))
    dias = max(7, min(dias, 365))
    current_app.logger.info("predictor_historico_requested dias=%s", dias)
    historico = current_app.predictor.historico(dias)
    current_app.logger.info(
        "predictor_historico_ready dias=%s puntos=%s",
        dias,
        len(historico),
    )
    return jsonify(historico)


@api_bp.post("/prediccion/diaria")
def prediccion_diaria():
    body = request.get_json(silent=True) or {}
    modo = str(body.get("modo", "B")).upper()
    dias = int(body.get("dias", 7))
    fecha_inicio = body.get("fecha_inicio")
    fecha_fin = body.get("fecha_fin")

    request_id = str(uuid4())
    requested_at = datetime.now(timezone.utc).isoformat()

    current_app.logger.info(
        "prediction_requested request_id=%s modo=%s dias=%s fecha_inicio=%s fecha_fin=%s",
        request_id,
        modo,
        dias,
        fecha_inicio,
        fecha_fin,
    )

    resultado = current_app.predictor.predecir(
        modo=modo,
        dias=dias,
        fecha_ini_str=fecha_inicio,
        fecha_fin_str=fecha_fin,
    )

    current_app.audit_repository.put_record(
        {
            "request_id": request_id,
            "created_at": requested_at,
            "endpoint": "/api/v1/prediccion/diaria",
            "status": "ok" if resultado.get("ok") else "error",
            "request_payload": str(
                {
                    "modo": modo,
                    "dias": dias,
                    "fecha_inicio": fecha_inicio,
                    "fecha_fin": fecha_fin,
                }
            ),
            "response_summary": str(
                {
                    "ok": resultado.get("ok"),
                    "data_source": resultado.get("data_source"),
                    "synced_range": resultado.get("synced_range"),
                    "missing_days_filled": resultado.get("missing_days_filled"),
                    "ultima_hist_dynamodb": resultado.get("ultima_hist_dynamodb"),
                    "sync_ready_for_prediction": resultado.get("sync_ready_for_prediction"),
                    "backfill_status": resultado.get("backfill_status"),
                }
            ),
            "sync_mode": resultado.get("data_source", ""),
            "error_message": resultado.get("error", ""),
        }
    )

    if resultado.get("ok"):
        current_app.logger.info(
            "prediction_completed request_id=%s data_source=%s synced_range=%s missing_days_filled=%s",
            request_id,
            resultado.get("data_source"),
            resultado.get("synced_range"),
            resultado.get("missing_days_filled"),
        )
    else:
        current_app.logger.error(
            "prediction_failed request_id=%s error=%s",
            request_id,
            resultado.get("error"),
        )

    if resultado.get("ok"):
        status_code = 200
    elif resultado.get("error") == "sync_incomplete":
        status_code = 503
    else:
        status_code = 500
    return jsonify(resultado), status_code


@api_bp.get("/clima/resumen")
def clima_resumen():
    current_app.logger.info("clima_resumen_requested")
    return jsonify(
        {
            "pais": "Colombia",
            "dependencia_hidrica_aprox": 65,
            "mensaje": (
                "La variacion de lluvia, sequia y temperatura modifica la lectura "
                "energetica del sistema y la presion percibida sobre precios y tarifas finales."
            ),
            "nota": (
                "El precio de bolsa se entiende a nivel nacional; las tarifas finales "
                "pueden variar por comercializador, operador de red y mercado."
            ),
        }
    )
