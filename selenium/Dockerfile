FROM seleniarm/standalone-chromium:latest

# Optional: Installieren zusätzlicher Pakete oder Anpassungen
# RUN apt-get update && apt-get install -y <paketname>

# Kopieren Sie das Startskript
COPY run.sh /run.sh
RUN chmod +x /run.sh

# Starten Sie das Add-on über das Startskript
CMD ["/run.sh"]
