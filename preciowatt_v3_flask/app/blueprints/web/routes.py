import json
from pathlib import Path

from flask import Blueprint, current_app, render_template, request

web_bp = Blueprint("web", __name__)

DATA_PATH = Path(__file__).resolve().parents[2] / "data" / "departamentos_colombia.json"
DEPARTAMENTOS = json.loads(DATA_PATH.read_text(encoding="utf-8"))


@web_bp.get("/")
def index():
    current_app.logger.info("web_index_requested")
    return render_template(
        "index.html",
        page_title="PrecioWatt Colombia - Prediccion del Precio de Bolsa Electrica con IA Hibrida",
    )


@web_bp.get("/favicon.ico")
def favicon():
    return ("", 204)


@web_bp.get("/clima")
def clima():
    selected_slug = request.args.get("departamento", "antioquia")
    current_app.logger.info("web_clima_requested departamento=%s", selected_slug)
    current = next((d for d in DEPARTAMENTOS if d["slug"] == selected_slug), DEPARTAMENTOS[0])

    kpi_impacto = {
        "Alta": {"label": "Estres climatico", "value": "Alto", "color": "var(--red)"},
        "Media": {"label": "Estres climatico", "value": "Medio", "color": "var(--amber)"},
        "Baja": {"label": "Estres climatico", "value": "Bajo", "color": "var(--green)"},
    }[current["nivel_alerta"]]

    return render_template(
        "clima.html",
        page_title="PrecioWatt Colombia - Clima y energia",
        departamentos=DEPARTAMENTOS,
        current=current,
        kpi_impacto=kpi_impacto,
    )
