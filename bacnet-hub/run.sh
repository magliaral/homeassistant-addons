#!/usr/bin/with-contenv bashio
set -e

TZ_VALUE=$(bashio::config 'TZ')
if [ -z "$TZ_VALUE" ]; then
    TZ_VALUE="Europe/Zurich"
fi

if [ -e "/usr/share/zoneinfo/$TZ_VALUE" ]; then
    ln -snf "/usr/share/zoneinfo/$TZ_VALUE" /etc/localtime
    echo "$TZ_VALUE" > /etc/timezone
else
    echo "WARN: Zeitzone $TZ_VALUE nicht gefunden, bleibe bei UTC"
fi

date  # nur zur Kontrolle
exec /venv/bin/python /usr/src/app/server.py
