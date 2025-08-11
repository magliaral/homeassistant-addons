# Home Assistant Add-on base image (wird vom Supervisor via BUILD_FROM gesetzt)
ARG BUILD_FROM
FROM ${BUILD_FROM}

# Systempakete
RUN apk add --no-cache python3 py3-pip bash

# Virtuelle Umgebung anlegen (um PEP 668 zu umgehen)
RUN python3 -m venv /venv
ENV PATH="/venv/bin:$PATH"

WORKDIR /usr/src/app

# Pip aktualisieren und Requirements installieren (in venv)
COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# App-Dateien
COPY server.py run.sh ./
RUN chmod +x /usr/src/app/run.sh

CMD ["/usr/src/app/run.sh"]
