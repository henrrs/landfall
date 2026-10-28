FROM python:3.12-slim
ARG LANDFALL_UID=1000
ARG LANDFALL_GID=1000
RUN apt-get update && apt-get install -y --no-install-recommends \
      libltdl7 libkrb5-3 libgssapi-krb5-2 passwd \
    && groupadd --gid "$LANDFALL_GID" landfall \
    && useradd --uid "$LANDFALL_UID" --gid "$LANDFALL_GID" --create-home \
      --shell /bin/bash landfall \
    && rm -rf /var/lib/apt/lists/*
COPY dist/landfall-*.whl /tmp/
RUN pip install --no-cache-dir /tmp/landfall-*.whl azure-storage-file-datalake azure-keyvault-secrets azure-identity
COPY lab/runner /app/lab/runner
COPY lab/platform-config /app/lab/platform-config
COPY lab/sqlserver/init /app/lab/sqlserver/init
COPY contracts/examples /app/contracts/examples
WORKDIR /app
ENV HOME=/home/landfall
ENTRYPOINT ["python", "/app/lab/runner/e2e.py"]
