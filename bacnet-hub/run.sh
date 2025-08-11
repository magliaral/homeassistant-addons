#!/usr/bin/env bash
set -e

# Mehrere mögliche Token-Namen akzeptieren
TOKEN="${SUPERVISOR_TOKEN:-${HOME_ASSISTANT_TOKEN:-${HOMEASSISTANT_TOKEN:-${HASSIO_TOKEN:-}}}}"

if [ -z "$TOKEN" ]; then
  echo "WARNING: No supervisor/homeassistant token found in environment."
  echo "Make sure 'hassio_api: true' OR 'homeassistant_api: true' is set in config.yaml."
  # wir starten trotzdem – server.py prüft erneut und loggt sauber
fi

exec /venv/bin/python /usr/src/app/server.py
