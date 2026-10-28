FROM python:3.12-slim
COPY dist/landfall-*.whl /tmp/
RUN pip install --no-cache-dir /tmp/landfall-*.whl azure-storage-file-datalake azure-keyvault-secrets azure-identity
RUN apt-get update && apt-get install -y --no-install-recommends libltdl7 libkrb5-3 libgssapi-krb5-2 && rm -rf /var/lib/apt/lists/*
COPY lab/runner /app/lab/runner
COPY lab/platform-config /app/lab/platform-config
COPY lab/sqlserver/init /app/lab/sqlserver/init
COPY contracts/examples /app/contracts/examples
WORKDIR /app
ENTRYPOINT ["python", "/app/lab/runner/e2e.py"]
