FROM arm64v8/alpine:3.14
FROM arm64v8/python:3.7-alpine

WORKDIR /usr/src/database-cron

COPY requirements.txt ./
COPY publish-remote.py ./
COPY clean-local.py ./
COPY entrypoint.sh ./

RUN chmod +x ./entrypoint.sh
RUN ls -alh
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT /usr/src/database-cron/entrypoint.sh
