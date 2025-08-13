#!/usr/bin/env bash
set -e

# TZ aus Option/Environment anwenden
if [ -n "$TZ" ] && [ -e "/usr/share/zoneinfo/$TZ" ]; then
  ln -snf "/usr/share/zoneinfo/$TZ" /etc/localtime
  echo "$TZ" > /etc/timezone 2>/dev/null || true
fi

# optional: Uhrzeit ausgeben, zu Kontrolle
date  # nur zur Kontrolle
exec /venv/bin/python /usr/src/app/server.py
