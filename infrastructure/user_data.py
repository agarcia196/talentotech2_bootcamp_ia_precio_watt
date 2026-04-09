from __future__ import annotations

import json
from textwrap import dedent

from infrastructure.config import ProjectConfig

APP_DIR_PATH = "/opt/preciowatt_v3_flask"

NGINX_CONF = dedent(
    """
    server {
        listen 80 default_server;
        server_name _;

        location / {
            proxy_pass http://127.0.0.1:8000;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
        }
    }
    """
).strip()


SYSTEMD_SERVICE = dedent(
    """
    [Unit]
    Description=PrecioWatt Flask API
    After=network.target

    [Service]
    User=ec2-user
    Group=ec2-user
    WorkingDirectory=/opt/preciowatt_v3_flask
    EnvironmentFile=/opt/preciowatt_v3_flask/.env
    ExecStart=/opt/preciowatt_v3_flask/venv/bin/gunicorn --workers 2 --bind 127.0.0.1:8000 --access-logfile - --error-logfile - --capture-output --log-level info wsgi:app
    Restart=always
    RestartSec=5

    [Install]
    WantedBy=multi-user.target
    """
).strip()

SYNC_SERVICE = dedent(
    """
    [Unit]
    Description=PrecioWatt Daily SIMEM Sync
    After=network.target

    [Service]
    Type=oneshot
    User=ec2-user
    Group=ec2-user
    WorkingDirectory=/opt/preciowatt_v3_flask
    EnvironmentFile=/opt/preciowatt_v3_flask/.env
    ExecStart=/opt/preciowatt_v3_flask/venv/bin/python -m app.jobs.sync daily
    """
).strip()

SYNC_TIMER = dedent(
    """
    [Unit]
    Description=Run PrecioWatt daily SIMEM sync at 08:00 America/Bogota

    [Timer]
    TimeZone=America/Bogota
    OnCalendar=*-*-* 08:00:00
    Persistent=true
    Unit=preciowatt-sync.service

    [Install]
    WantedBy=timers.target
    """
).strip()

BACKFILL_SERVICE = dedent(
    """
    [Unit]
    Description=PrecioWatt initial SIMEM backfill
    After=network.target preciowatt.service

    [Service]
    Type=simple
    User=ec2-user
    Group=ec2-user
    WorkingDirectory=/opt/preciowatt_v3_flask
    EnvironmentFile=/opt/preciowatt_v3_flask/.env
    ExecStart=/opt/preciowatt_v3_flask/venv/bin/python -m app.jobs.sync backfill-if-needed
    Restart=no

    [Install]
    WantedBy=multi-user.target
    """
).strip()

def _write_text_file(target_path: str, content: str) -> str:
    serialized_path = json.dumps(target_path)
    serialized_content = json.dumps(content + "\n")
    return dedent(
        f"""
        python3 - <<'PY'
        from pathlib import Path
        path = Path({serialized_path})
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text({serialized_content}, encoding="utf-8")
        PY
        """
    ).strip()


def build_user_data(
    config: ProjectConfig,
    *,
    bucket_name: str,
    daily_feature_table_name: str,
    audit_table_name: str,
    sync_status_table_name: str,
    app_asset_bucket_name: str,
    app_asset_object_key: str,
) -> str:
    env_file = dedent(
        f"""
        AWS_REGION={config.aws_region}
        APP_PORT={config.app_port}
        APP_BUCKET_NAME={bucket_name}
        MODEL_OBJECT_KEY={config.model_object_key}
        CONFIG_OBJECT_KEY={config.config_object_key}
        RAW_PREFIX={config.raw_prefix}
        DAILY_FEATURE_TABLE_NAME={daily_feature_table_name}
        PREDICTION_AUDIT_TABLE_NAME={audit_table_name}
        SYNC_STATUS_TABLE_NAME={sync_status_table_name}
        MISSING_DATA_THRESHOLD_DAYS={config.missing_data_threshold_days}
        LOOKBACK_DAYS={config.lookback_days}
        INITIAL_BACKFILL_START_DATE={config.initial_backfill_start_date}
        HISTORICAL_START_DATE={config.historical_start_date}
        DATASET_ANCHOR_START_DATE={config.dataset_anchor_start_date}
        DAILY_SYNC_LOOKBACK_DAYS={config.daily_sync_lookback_days}
        SYNC_SCHEDULE_TIME={config.sync_schedule_time}
        BACKFILL_CHUNK_DAYS={config.backfill_chunk_days}
        APP_LOG_LEVEL=INFO
        BOOTSTRAP_FROM_LOCAL_ASSETS=true
        """
    ).strip()

    commands = [
        "#!/bin/bash",
        "set -euxo pipefail",
        "exec > >(tee /var/log/preciowatt-bootstrap.log | logger -t user-data -s 2>/dev/console) 2>&1",
        f"APP_DIR={APP_DIR_PATH}",
        "dnf install -y python3.11 python3.11-pip python3.11-devel gcc gcc-c++ make git nginx unzip awscli",
        "mkdir -p ${APP_DIR}",
        f"aws s3 cp s3://{app_asset_bucket_name}/{app_asset_object_key} /tmp/preciowatt_v3_flask.zip",
        "unzip -oq /tmp/preciowatt_v3_flask.zip -d ${APP_DIR}",
        "rm -rf ${APP_DIR}/cache_simem || true",
        "find ${APP_DIR} -name '__pycache__' -type d -exec rm -rf {} + || true",
    ]

    commands.extend(
        [
            "python3.11 -m venv ${APP_DIR}/venv",
            "${APP_DIR}/venv/bin/pip install --upgrade pip",
            "${APP_DIR}/venv/bin/pip install -r ${APP_DIR}/requirements.txt",
            _write_text_file(f"{APP_DIR_PATH}/.env", env_file),
            _write_text_file("/etc/nginx/conf.d/preciowatt.conf", NGINX_CONF),
            _write_text_file("/etc/systemd/system/preciowatt.service", SYSTEMD_SERVICE),
            _write_text_file("/etc/systemd/system/preciowatt-sync.service", SYNC_SERVICE),
            _write_text_file("/etc/systemd/system/preciowatt-sync.timer", SYNC_TIMER),
            _write_text_file("/etc/systemd/system/preciowatt-backfill.service", BACKFILL_SERVICE),
            "test -f ${APP_DIR}/.env",
            "test -f /etc/nginx/conf.d/preciowatt.conf",
            "test -f /etc/systemd/system/preciowatt.service",
            "test -f /etc/systemd/system/preciowatt-sync.service",
            "test -f /etc/systemd/system/preciowatt-sync.timer",
            "test -f /etc/systemd/system/preciowatt-backfill.service",
            "rm -f /etc/nginx/conf.d/default.conf || true",
            "rm -f /etc/nginx/default.d/* || true",
            "chown -R ec2-user:ec2-user ${APP_DIR}",
            "systemctl daemon-reload",
            "systemctl enable --now nginx",
            "systemctl enable --now preciowatt.service",
            "systemctl enable --now preciowatt-sync.timer",
            "systemctl enable --now preciowatt-backfill.service",
            "systemctl is-active nginx",
            "systemctl is-active preciowatt.service",
        ]
    )

    return "\n".join(commands)
