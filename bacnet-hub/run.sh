#!/usr/bin/with-contenv bashio
# robust: stop on error (-e), fail pipelines (-o pipefail); KEIN -u hier
set -e -o pipefail

# 1) tzdata sicherstellen (falls nicht im Image installiert)
if ! [ -e /usr/share/zoneinfo/UTC ]; then
  if command -v apk >/dev/null 2>&1; then
    apk add --no-cache tzdata >/dev/null 2>&1 || true
  fi
fi

# 2) TZ aus Add-on-Option, sonst aus Env, sonst Default
TZ_VALUE=""
if bashio::config.has 'TZ'; then
  TZ_VALUE="$(bashio::config 'TZ')" || TZ_VALUE=""
fi
[ -z "$TZ_VALUE" ] && TZ_VALUE="${TZ:-}"
[ -z "$TZ_VALUE" ] && TZ_VALUE="Europe/Zurich"

# 3) Anwenden, wenn Zone existiert
if [ -e "/usr/share/zoneinfo/$TZ_VALUE" ]; then
  ln -snf "/usr/share/zoneinfo/$TZ_VALUE" /etc/localtime
  printf '%s\n' "$TZ_VALUE" > /etc/timezone 2>/dev/null || true
else
  echo "WARN: Zeitzone '$TZ_VALUE' nicht gefunden, bleibe bei UTC" >&2
fi

# 4) kurze Kontrolle
date

# 5) App starten
exec /venv/bin/python /usr/src/app/server.py
