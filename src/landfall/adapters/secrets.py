"""Connection reference resolution without secret persistence."""

from __future__ import annotations

import json
import os
import re
from typing import Any


class EnvSecrets:
    def resolve(self, connection_ref: str) -> dict[str, Any]:
        key = "LANDFALL_CONNECTION_" + re.sub(r"[^A-Za-z0-9]", "_", connection_ref).upper()
        raw = os.environ.get(key)
        if not raw:
            raise ValueError(f"connectionRef {connection_ref!r} is not resolvable via {key}")
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise TypeError("connection secret must be a JSON object")
        return value


class _EmulatorCredential:
    def get_token(self, *scopes, **kwargs):
        import base64
        import time

        from azure.core.credentials import AccessToken

        def encode(value):
            return base64.urlsafe_b64encode(json.dumps(value).encode()).decode().rstrip("=")

        expires = int(time.time()) + 3600
        token = encode({"alg": "none", "typ": "JWT"}) + "." + encode({"aud": "https://vault.azure.net", "exp": expires}) + ".signature"
        return AccessToken(token, expires)


class KeyVaultSecrets:
    def __init__(self, vault_url: str, references: dict[str, str], emulator: bool = False):
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient

        self.references = references
        credential = _EmulatorCredential() if emulator else DefaultAzureCredential()
        self.client = SecretClient(vault_url=vault_url, credential=credential,
                                   verify_challenge_resource=not emulator)

    def resolve(self, connection_ref: str) -> dict[str, Any]:
        if connection_ref not in self.references:
            raise ValueError(f"connectionRef {connection_ref!r} not mapped in platform secrets")
        return json.loads(self.client.get_secret(self.references[connection_ref]).value)


def make_secret_provider(config: dict[str, Any]):
    options = config.get("secrets", {"provider": "env"})
    if options.get("provider") == "env":
        return EnvSecrets()
    if options.get("provider") == "keyvault":
        return KeyVaultSecrets(options["vaultUrl"], options["references"],
                               bool(options.get("emulator", False)))
    raise ValueError("unknown secret provider")
