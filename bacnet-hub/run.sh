#!/usr/bin/env bash
set -e

# Akzeptiere mehrere mögliche Token-Namen
export HA_SUPERVISOR_TOKEN="${SUPERVISOR_TOKEN:-${HASSIO_TOKEN:-${HOME_ASSISTANT_TOKEN:-${HOMEASSISTANT_TOKEN:-}}}}"

# Debug-Ausgabe, um zu sehen, ob ein Token gesetzt ist (kannst du später entfernen)
echo "ENV TOKENS (masked):"
if [ -n "$HA_SUPERVISOR_TOKEN" ]; then
  echo "  HA_SUPERVISOR_TOKEN = *****"
else
  echo "  (kein Token gefunden)"
  echo "  Hinweis: Prüfe in config.yaml: hassio_api: true oder homeassistant_api: true"
fi

exec /venv/bin/python /usr/src/app/server.py
