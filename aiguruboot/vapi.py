"""Standard-library client for the complete virtual API management surface."""
from __future__ import annotations
import json
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

class VapiError(RuntimeError): pass

class VapiClient:
    def __init__(self, base_url="http://127.0.0.1:11434", token=None, timeout=15):
        self.base=base_url.rstrip("/"); self.token=token; self.timeout=timeout
    def request(self, method, path="", payload=None):
        headers={"Accept":"application/json"}
        if self.token: headers["Authorization"]="Bearer "+self.token
        data=json.dumps(payload).encode() if payload is not None else None
        if data is not None: headers["Content-Type"]="application/json"
        try:
            with urlopen(Request(self.base+"/v1/vapis"+path, data=data, headers=headers, method=method), timeout=self.timeout) as r:
                return json.loads(r.read().decode() or "{}")
        except HTTPError as exc:
            try: detail = exc.read().decode("utf-8", "replace")
            except OSError: detail = ""
            raise VapiError(f"HTTP {exc.code}: {detail or exc.reason}") from exc
        except (URLError, ValueError) as exc:
            raise VapiError(str(exc)) from exc
    @staticmethod
    def _require_id(ident):
        if not ident or "/" in ident or len(ident) > 64: raise VapiError("a valid virtual API id is required")
        return ident
    def list(self): return self.request("GET")
    def get(self, ident): return self.request("GET", "/"+self._require_id(ident))
    def create(self, label, models=None, ident=None, **options):
        body={"label":label, **options}
        if models is not None: body["models"]=models
        if ident: body["id"]=ident
        return self.request("POST", payload=body)
    def update(self, ident, **fields): return self.request("POST", "/%s/update"%self._require_id(ident), fields)
    def rotate(self, ident): return self.request("POST", "/%s/rotate"%self._require_id(ident), {})
    def revoke(self, ident): return self.request("POST", "/%s/revoke"%self._require_id(ident), {})
    def enable(self, ident): return self.request("POST", "/%s/enable"%self._require_id(ident), {})
    def disable(self, ident): return self.request("POST", "/%s/disable"%self._require_id(ident), {})
    def connection(self, ident): return self.request("GET", "/%s/connection"%self._require_id(ident))
    def usage(self, ident): return self.request("GET", "/%s/usage"%self._require_id(ident))

class VapiRuntimeClient:
    """Client for inference through one virtual API namespace."""
    def __init__(self, base_url, vapi_id, api_key, timeout=120):
        if not vapi_id or "/" in vapi_id: raise VapiError("invalid virtual API id")
        self.base = base_url.rstrip("/") + "/v1/a/" + vapi_id
        self.api_key, self.timeout = api_key, timeout
    def call(self, endpoint, payload):
        if endpoint not in ("chat/completions", "responses", "messages"):
            raise VapiError("endpoint must be chat/completions, responses, or messages")
        data=json.dumps(payload).encode("utf-8")
        req=Request(self.base+"/v1/"+endpoint, data=data,
                    headers={"Accept":"application/json", "Content-Type":"application/json",
                             "Authorization":"Bearer "+self.api_key}, method="POST")
        try:
            with urlopen(req, timeout=self.timeout) as response:
                return json.loads(response.read().decode("utf-8") or "{}")
        except HTTPError as exc:
            raise VapiError(f"HTTP {exc.code}: {exc.read().decode('utf-8', 'replace')}") from exc
        except (URLError, ValueError) as exc:
            raise VapiError(str(exc)) from exc
