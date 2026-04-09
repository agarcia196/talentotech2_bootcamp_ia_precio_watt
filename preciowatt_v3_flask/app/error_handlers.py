from __future__ import annotations

import traceback
from datetime import datetime, timezone
from uuid import uuid4

from flask import Flask, jsonify, request
from werkzeug.exceptions import HTTPException


def register_error_handlers(app: Flask) -> None:
    @app.errorhandler(HTTPException)
    def handle_http_error(exc: HTTPException):
        if exc.code == 404 and request.path == "/favicon.ico":
            return ("", 204)

        app.logger.warning(
            "http_error status=%s method=%s path=%s description=%s",
            exc.code,
            request.method,
            request.path,
            exc.description,
        )
        return (
            jsonify(
                {
                    "ok": False,
                    "error": exc.name.lower().replace(" ", "_"),
                    "message": exc.description,
                    "status_code": exc.code,
                }
            ),
            exc.code,
        )

    @app.errorhandler(Exception)
    def handle_unexpected_error(exc: Exception):
        error_id = str(uuid4())
        payload = request.get_json(silent=True)
        if payload is None:
            payload = request.values.to_dict(flat=True)

        app.logger.exception(
            "unhandled_exception error_id=%s method=%s path=%s",
            error_id,
            request.method,
            request.path,
        )

        audit_repository = getattr(app, "audit_repository", None)
        if audit_repository is not None:
            audit_repository.put_record(
                {
                    "request_id": error_id,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "endpoint": request.path,
                    "status": "error",
                    "request_payload": str(payload),
                    "response_summary": str(
                        {
                            "error": "internal_server_error",
                            "error_id": error_id,
                        }
                    ),
                    "sync_mode": "exception_handler",
                    "error_message": "".join(
                        traceback.format_exception_only(type(exc), exc)
                    ).strip()[:1000],
                }
            )

        return (
            jsonify(
                {
                    "ok": False,
                    "error": "internal_server_error",
                    "error_id": error_id,
                }
            ),
            500,
        )
