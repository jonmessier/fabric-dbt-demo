"""Shared helpers for the Fabric scripts (provision.py, load_bronze.py, ...).

Everything is looked up by name at run time; no workspace, item or tenant IDs are stored
in the repo. Authentication is the Azure CLI sign-in (`az login`), the same identity dbt
uses with `authentication: CLI`.

Not meant to be run directly.
"""
from __future__ import annotations

import base64
import json
import struct
import sys
import time
from itertools import chain, repeat
from pathlib import Path
from typing import Any, Iterator

import requests
import yaml
from azure.identity import AzureCliCredential

REPO_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = REPO_ROOT / ".env"
CONFIG_FILE = REPO_ROOT / "config" / "environments.yml"

FABRIC_API = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
STORAGE_SCOPE = "https://storage.azure.com/.default"
SQL_SCOPE = "https://database.windows.net/.default"
ONELAKE_DFS = "https://onelake.dfs.fabric.microsoft.com"


class FabricError(RuntimeError):
    pass


# --------------------------------------------------------------------------- config / .env

def load_config() -> dict:
    return yaml.safe_load(CONFIG_FILE.read_text(encoding="utf-8"))


def env_config(env: str) -> dict:
    cfg = load_config()
    if env not in cfg["environments"]:
        raise FabricError(f"unknown environment '{env}' (config/environments.yml)")
    return {**cfg, "env": env, **cfg["environments"][env]}


def read_env_file() -> dict[str, str]:
    values: dict[str, str] = {}
    if not ENV_FILE.exists():
        return values
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip().strip('"')
    return values


def set_env_value(key: str, value: str) -> bool:
    """Set KEY=value in .env, keeping comments and order. Returns True if the file changed."""
    lines = ENV_FILE.read_text(encoding="utf-8").splitlines() if ENV_FILE.exists() else []
    new_line = f"{key}={value}"
    for i, line in enumerate(lines):
        if line.strip().startswith(f"{key}="):
            if line.strip() == new_line:
                return False
            lines[i] = new_line
            break
    else:
        lines.append(new_line)
    ENV_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return True


def finish(summary: dict, exit_code: int | None = None) -> None:
    """Print the one-line JSON summary every script ends with, then exit."""
    print(json.dumps(summary, default=str))
    sys.exit(exit_code if exit_code is not None else (0 if summary.get("ok") else 1))


def log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


# --------------------------------------------------------------------------- auth

class TokenCache:
    def __init__(self) -> None:
        self._cred = AzureCliCredential(process_timeout=60)
        self._tokens: dict[str, Any] = {}

    def get(self, scope: str) -> str:
        tok = self._tokens.get(scope)
        if tok is None or tok.expires_on - time.time() < 300:
            tok = self._cred.get_token(scope)
            self._tokens[scope] = tok
        return tok.token

    def tenant_id(self) -> str:
        payload = self.get(FABRIC_SCOPE).split(".")[1]
        payload += "=" * (-len(payload) % 4)
        return json.loads(base64.urlsafe_b64decode(payload))["tid"]


# --------------------------------------------------------------------------- Fabric REST

class FabricClient:
    def __init__(self, tokens: TokenCache | None = None) -> None:
        self.tokens = tokens or TokenCache()
        self.session = requests.Session()

    def request(self, method: str, path: str, *, json_body: Any = None, params: dict | None = None,
                lro: bool = True, ok: tuple[int, ...] = (200, 201, 202), timeout_s: int = 900) -> Any:
        """Call the Fabric API. Retries on 429/5xx, follows long-running operations when lro=True."""
        url = path if path.startswith("https://") else f"{FABRIC_API}/{path.lstrip('/')}"
        for attempt in range(8):
            resp = self.session.request(
                method, url, json=json_body, params=params, timeout=120,
                headers={"Authorization": f"Bearer {self.tokens.get(FABRIC_SCOPE)}"})
            if resp.status_code == 429 or resp.status_code >= 500:
                wait = int(resp.headers.get("Retry-After", 2 ** attempt + 1))
                log(f"  {resp.status_code} from {method} {path}; retrying in {wait}s")
                time.sleep(min(wait, 60))
                continue
            break
        if resp.status_code not in ok:
            raise FabricError(f"{method} {path} -> {resp.status_code}: {resp.text[:800]}")
        if resp.status_code == 202 and lro and resp.headers.get("x-ms-operation-id"):
            return self._wait_operation(resp, timeout_s)
        if not resp.content:
            return {"_status": resp.status_code, "_location": resp.headers.get("Location")}
        body = resp.json()
        if isinstance(body, dict):
            body.setdefault("_status", resp.status_code)
            body.setdefault("_location", resp.headers.get("Location"))
        return body

    def _wait_operation(self, resp: requests.Response, timeout_s: int) -> Any:
        op_id = resp.headers["x-ms-operation-id"]
        deadline = time.time() + timeout_s
        delay = int(resp.headers.get("Retry-After", 3))
        while time.time() < deadline:
            time.sleep(delay)
            op = self.request("GET", f"operations/{op_id}", lro=False)
            status = op.get("status")
            if status == "Succeeded":
                try:
                    return self.request("GET", f"operations/{op_id}/result", lro=False)
                except FabricError:
                    return op  # some operations have no result body
            if status in ("Failed", "Undefined"):
                raise FabricError(f"operation {op_id} {status}: {json.dumps(op.get('error'))}")
            delay = min(delay * 2, 15)
        raise FabricError(f"operation {op_id} did not finish in {timeout_s}s")

    def list(self, path: str) -> Iterator[dict]:
        params: dict[str, str] = {}
        while True:
            body = self.request("GET", path, params=params, lro=False)
            yield from body.get("value", [])
            token = body.get("continuationToken")
            if not token:
                return
            params = {"continuationToken": token}

    def find(self, path: str, name: str) -> dict | None:
        matches = [x for x in self.list(path) if x.get("displayName") == name]
        if len(matches) > 1:
            raise FabricError(f"more than one '{name}' under {path}")
        return matches[0] if matches else None

    # convenience lookups ----------------------------------------------------
    def workspace(self, name: str, required: bool = True) -> dict | None:
        ws = self.find("workspaces", name)
        if ws is None and required:
            raise FabricError(f"workspace '{name}' not found. Run: .\\tasks.ps1 setup -Env <env>")
        return ws

    def item(self, ws_id: str, collection: str, name: str, required: bool = True) -> dict | None:
        it = self.find(f"workspaces/{ws_id}/{collection}", name)
        if it is None and required:
            raise FabricError(f"'{name}' not found in workspace {ws_id} ({collection})")
        return it


# --------------------------------------------------------------------------- OneLake (ADLS Gen2 DFS API)

class OneLake:
    """Minimal OneLake file client. Paths are '<item id>/Files/...' inside a workspace."""

    def __init__(self, workspace_id: str, tokens: TokenCache) -> None:
        self.ws = workspace_id
        self.tokens = tokens
        self.session = requests.Session()

    def _headers(self, extra: dict | None = None) -> dict:
        h = {"Authorization": f"Bearer {self.tokens.get(STORAGE_SCOPE)}", "x-ms-version": "2023-11-03"}
        h.update(extra or {})
        return h

    def _call(self, method: str, url: str, **kw) -> requests.Response:
        for attempt in range(6):
            resp = self.session.request(method, url, headers=self._headers(kw.pop("headers", None)),
                                        timeout=300, **kw)
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(2 ** attempt)
                continue
            return resp
        return resp

    def list_files(self, directory: str) -> dict[str, int]:
        """{path: size} for every file under directory (recursive). Empty if it doesn't exist."""
        out: dict[str, int] = {}
        params = {"resource": "filesystem", "recursive": "true", "directory": directory}
        while True:
            resp = self._call("GET", f"{ONELAKE_DFS}/{self.ws}", params=params)
            if resp.status_code == 404:
                return out
            if resp.status_code != 200:
                raise FabricError(f"OneLake list {directory} -> {resp.status_code}: {resp.text[:400]}")
            for p in resp.json().get("paths", []):
                if str(p.get("isDirectory", "false")).lower() != "true":
                    out[p["name"]] = int(p.get("contentLength", 0))
            cont = resp.headers.get("x-ms-continuation")
            if not cont:
                return out
            params = {**params, "continuation": cont}

    def upload(self, path: str, data: bytes) -> None:
        url = f"{ONELAKE_DFS}/{self.ws}/{path}"
        resp = self._call("PUT", url, params={"resource": "file"}, headers={"Content-Length": "0"})
        if resp.status_code not in (200, 201):
            raise FabricError(f"OneLake create {path} -> {resp.status_code}: {resp.text[:400]}")
        if data:
            resp = self._call("PATCH", url, params={"action": "append", "position": "0"}, data=data)
            if resp.status_code != 202:
                raise FabricError(f"OneLake append {path} -> {resp.status_code}: {resp.text[:400]}")
        resp = self._call("PATCH", url, params={"action": "flush", "position": str(len(data))},
                          headers={"Content-Length": "0"})
        if resp.status_code != 200:
            raise FabricError(f"OneLake flush {path} -> {resp.status_code}: {resp.text[:400]}")

    def delete(self, path: str) -> bool:
        """Delete a file or folder (recursively). False if it didn't exist."""
        resp = self._call("DELETE", f"{ONELAKE_DFS}/{self.ws}/{path}", params={"recursive": "true"})
        if resp.status_code == 404:
            return False
        if resp.status_code != 200:
            raise FabricError(f"OneLake delete {path} -> {resp.status_code}: {resp.text[:400]}")
        return True

    def read(self, path: str) -> bytes | None:
        resp = self._call("GET", f"{ONELAKE_DFS}/{self.ws}/{path}")
        if resp.status_code == 404:
            return None
        if resp.status_code != 200:
            raise FabricError(f"OneLake read {path} -> {resp.status_code}: {resp.text[:400]}")
        return resp.content


# --------------------------------------------------------------------------- SQL (warehouse)

ODBC_DRIVER = "ODBC Driver 18 for SQL Server"


def sql_connect(server: str, database: str, tokens: TokenCache):
    """Connect to a Fabric SQL endpoint with the az login token, the way dbt-fabric 1.10 does
    (pyodbc + ODBC Driver 18, token passed as SQL_COPT_SS_ACCESS_TOKEN)."""
    import pyodbc

    token = tokens.get(SQL_SCOPE).encode("utf-8")
    encoded = bytes(chain.from_iterable(zip(token, repeat(0))))
    attrs_before = {1256: struct.pack("<i", len(encoded)) + encoded}  # SQL_COPT_SS_ACCESS_TOKEN
    conn_str = (f"Driver={{{ODBC_DRIVER}}};Server={server},1433;Database={database};"
                f"Encrypt=yes;TrustServerCertificate=no")
    return pyodbc.connect(conn_str, attrs_before=attrs_before, autocommit=True, timeout=60)
