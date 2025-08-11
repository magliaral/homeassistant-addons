#!/usr/bin/env bash
set -e

if [ -z "${SUPERVISOR_TOKEN}" ]; then
  echo "ERROR: SUPERVISOR_TOKEN not set. Start this as a Home Assistant add-on."
  exit 1
fi

exec /venv/bin/python /usr/src/app/server.py
