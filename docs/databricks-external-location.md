# Databricks and Unity Catalog External Location

Install the same wheel in a Databricks Python wheel task. Pass the exact contract JSON or YAML path and a separate Platform Config for the domain. The job's identity needs permission to use a Unity Catalog Storage Credential and External Location mapped to the domain's ADLS landing container. The External Location and storage account never belong in the Ingestion Contract.

The current ADLS adapter uses Azure SDK `DefaultAzureCredential` when no connection string is supplied. In Databricks this requires an identity path that the SDK can use. A future Databricks storage adapter should resolve the authorized External Location URI and use runtime-supported access without embedding keys or `dbutils.secrets.get()` in contract or core. A Spark executor adapter is also future work; V1 wheel execution uses the local thread pool even inside a Databricks driver.
