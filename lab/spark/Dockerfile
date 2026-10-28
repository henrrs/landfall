FROM python:3.12-slim
ARG LANDFALL_UID=1000
ARG LANDFALL_GID=1000

RUN apt-get update && apt-get install -y --no-install-recommends \
      openjdk-21-jre-headless libltdl7 libkrb5-3 libgssapi-krb5-2 passwd \
    && groupadd --gid "$LANDFALL_GID" landfall \
    && useradd --uid "$LANDFALL_UID" --gid "$LANDFALL_GID" --create-home \
      --shell /bin/bash landfall \
    && rm -rf /var/lib/apt/lists/*

COPY dist/landfall-*.whl /tmp/
RUN pip install --no-cache-dir /tmp/landfall-*.whl \
      azure-storage-file-datalake azure-keyvault-secrets azure-identity pyspark==4.0.1 \
      'pandas>=2.0,<3'

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 \
    SPARK_HOME=/usr/local/lib/python3.12/site-packages/pyspark \
    PATH=/usr/local/lib/python3.12/site-packages/pyspark/bin:/usr/local/bin:$PATH \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

RUN python -c 'import landfall, mssql_python, pyarrow, pyspark; print(landfall.__version__, pyarrow.__version__, pyspark.__version__)'

COPY lab/runner /app/lab/runner
COPY lab/platform-config /app/lab/platform-config
COPY lab/sqlserver/init /app/lab/sqlserver/init
COPY contracts/examples /app/contracts/examples
WORKDIR /app
ENV HOME=/home/landfall
