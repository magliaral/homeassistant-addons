ARG BUILD_FROM
FROM ${BUILD_FROM}

RUN apk add --no-cache python3 py3-pip bash

WORKDIR /usr/src/app
COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt
COPY server.py run.sh ./
RUN chmod +x /usr/src/app/run.sh

CMD ["/usr/src/app/run.sh"]
