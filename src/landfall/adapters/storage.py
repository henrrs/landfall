"""Landing storage adapters. Keys are relative to the configured landing container."""

from __future__ import annotations

import fcntl
import hashlib
import os
import shutil
import tempfile
from pathlib import Path


class ConcurrentUpdate(RuntimeError):
    pass


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class LocalStorage:
    def __init__(self, root: str, account: str, container: str):
        self.root = Path(root) / account / container
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        if key.startswith("/") or ".." in Path(key).parts:
            raise ValueError("invalid storage key")
        return self.root / key

    def exists(self, key: str) -> bool:
        return self._path(key).exists()

    def read(self, key: str) -> bytes:
        return self._path(key).read_bytes()

    def etag(self, key: str) -> str | None:
        path = self._path(key)
        return _sha(path.read_bytes()) if path.exists() else None

    def write(self, key: str, data: bytes, expected_etag: str | None = None) -> str:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        lock = path.with_name(path.name + ".lock")
        with lock.open("a+b") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            current = self.etag(key)
            if expected_etag is not None and current != (expected_etag or None):
                raise ConcurrentUpdate(f"state changed concurrently: {key}")
            with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
                temp_name = tmp.name
                tmp.write(data)
                tmp.flush()
                os.fsync(tmp.fileno())
            os.replace(temp_name, path)
            return _sha(data)

    def publish_file(self, file: Path, key: str) -> int:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            raise FileExistsError(f"immutable capture exists: {key}")
        partial = path.with_name(path.name + ".partial")
        with file.open("rb") as source, partial.open("wb") as target:
            shutil.copyfileobj(source, target, 1024 * 1024)
            target.flush()
            os.fsync(target.fileno())
        os.replace(partial, path)
        return path.stat().st_size

    def list(self, prefix: str) -> list[str]:
        path = self._path(prefix)
        if not path.exists():
            return []
        if path.is_file():
            return [prefix]
        return sorted(str(p.relative_to(self.root)) for p in path.rglob("*") if p.is_file())


class AdlsStorage:
    """ADLS Gen2 hierarchical namespace adapter; Azure imports remain optional."""

    def __init__(self, account: str, container: str, *, connection_string: str | None = None,
                 endpoint: str | None = None, dfs_host_header: str | None = None):
        from azure.storage.blob import BlobServiceClient
        from azure.storage.filedatalake import DataLakeServiceClient

        kwargs = {}
        if dfs_host_header:
            from azure.core.pipeline.transport import RequestsTransport

            class DfsHostTransport(RequestsTransport):
                def send(self, request, **options):
                    request.headers["Host"] = dfs_host_header
                    return super().send(request, **options)

            kwargs["transport"] = DfsHostTransport()
        if connection_string:
            if dfs_host_header:
                from azure.core.credentials import AzureNamedKeyCredential

                parts = dict(item.split("=", 1) for item in connection_string.split(";") if "=" in item)
                dfs_endpoint = parts.get("DfsEndpoint", parts.get("BlobEndpoint", ""))
                if not dfs_endpoint.endswith("/" + account):
                    raise ValueError("emulator DFS endpoint must end with account name")
                service = DataLakeServiceClient(
                    account_url=dfs_endpoint[: -len(account) - 1],
                    credential=AzureNamedKeyCredential(account, parts["AccountKey"]), **kwargs)
            else:
                service = DataLakeServiceClient.from_connection_string(connection_string, **kwargs)
            blob_service = BlobServiceClient.from_connection_string(connection_string)
        else:
            from azure.identity import DefaultAzureCredential

            credential = DefaultAzureCredential()
            service = DataLakeServiceClient(account_url=endpoint or f"https://{account}.dfs.core.windows.net",
                                            credential=credential, **kwargs)
            blob_service = BlobServiceClient(account_url=f"https://{account}.blob.core.windows.net",
                                             credential=credential)
        self.container = container
        self.fs = service.get_file_system_client(container)
        self.blob_container = blob_service.get_container_client(container)
        try:
            self.blob_container.create_container()
        except Exception as exc:
            from azure.core.exceptions import ResourceExistsError

            if not isinstance(exc, ResourceExistsError):
                raise

    def _file(self, key: str):
        if key.startswith("/") or ".." in Path(key).parts:
            raise ValueError("invalid storage key")
        return self.fs.get_file_client(key)

    def _dirs(self, key: str) -> None:
        from azure.core.exceptions import ResourceExistsError

        parts = key.split("/")[:-1]
        for i in range(1, len(parts) + 1):
            try:
                self.fs.get_directory_client("/".join(parts[:i])).create_directory()
            except ResourceExistsError:
                pass

    def exists(self, key: str) -> bool:
        from azure.core.exceptions import ResourceNotFoundError

        try:
            self._file(key).get_file_properties()
            return True
        except ResourceNotFoundError:
            return False

    def read(self, key: str) -> bytes:
        return self._file(key).download_file().readall()

    def etag(self, key: str) -> str | None:
        from azure.core.exceptions import ResourceNotFoundError

        try:
            return self._file(key).get_file_properties().etag
        except ResourceNotFoundError:
            return None

    def write(self, key: str, data: bytes, expected_etag: str | None = None) -> str:
        from azure.core import MatchConditions
        from azure.core.exceptions import ResourceExistsError, ResourceModifiedError

        self._dirs(key)
        kwargs = {}
        if expected_etag is not None:
            if expected_etag:
                kwargs = {"etag": expected_etag, "match_condition": MatchConditions.IfNotModified}
            else:
                kwargs = {"if_none_match": "*"}
        try:
            self._file(key).upload_data(data, overwrite=True, **kwargs)
        except (ResourceModifiedError, ResourceExistsError) as exc:
            raise ConcurrentUpdate(f"state changed concurrently: {key}") from exc
        return self.etag(key) or ""

    def publish_file(self, file: Path, key: str) -> int:
        import uuid

        if self.exists(key):
            raise FileExistsError(f"immutable capture exists: {key}")
        self._dirs(key)
        temporary = key + ".partial." + uuid.uuid4().hex
        with file.open("rb") as source:
            self._file(temporary).upload_data(source, overwrite=True, max_concurrency=1)
        self._file(temporary).rename_file(f"{self.container}/{key}")
        return file.stat().st_size

    def list(self, prefix: str) -> list[str]:
        return sorted(blob.name for blob in self.blob_container.list_blobs(name_starts_with=prefix + "/"))


def make_storage(config: dict, account: str, container: str):
    storage = config.get("storage", {"type": "local", "root": ".landfall"})
    if storage.get("type") == "local":
        return LocalStorage(storage.get("root", ".landfall"), account, container)
    if storage.get("type") == "adls":
        env_name = storage.get("connectionStringEnv")
        return AdlsStorage(account, container,
                           connection_string=os.environ.get(env_name) if env_name else None,
                           endpoint=storage.get("endpoint"),
                           dfs_host_header=storage.get("dfsHostHeader"))
    raise ValueError("unknown storage type")
