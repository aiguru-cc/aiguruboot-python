"""Portable replacements for the Swift configuration and environment services."""
from __future__ import annotations
import hashlib, json, os, secrets, time, uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

def data_dir() -> Path:
    override = os.environ.get("AIGURUBOOT_DATA_DIR")
    if override: return Path(override).expanduser()
    if os.name == "nt": return Path(os.environ.get("APPDATA", Path.home())) / "AiguruBoot"
    if os.sys.platform == "darwin": return Path.home() / "Library" / "Application Support" / "AiguruBoot"
    return Path(os.environ.get("XDG_DATA_HOME", Path.home()/".local/share")) / "aiguruboot"

class JsonConfig:
    def __init__(self, path: Path | None = None):
        self.path = path or data_dir()/"config.json"; self.path.parent.mkdir(parents=True, exist_ok=True)
    def load(self) -> dict[str, Any]:
        try: return json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError): return {}
    def update(self, values: dict[str, Any]) -> dict[str, Any]:
        current=self.load(); current.update(values); tmp=self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(current, ensure_ascii=False, indent=2)+"\n", encoding="utf-8"); os.replace(tmp, self.path); self.path.chmod(0o600); return current

@dataclass
class Credential:
    id: str; label: str; enabled: bool = True; key_hint: str = ""; key_hash: str = ""; created_at: float = 0.0

class CredentialStore:
    def __init__(self, path: Path | None = None):
        self.path=path or data_dir()/"credentials.json"; self.path.parent.mkdir(parents=True, exist_ok=True)
    def _read(self):
        try: return json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError): return {}
    def list(self): return list(self._read().values())
    def create(self, label: str, ident: str | None = None):
        ident=ident or "agent-"+secrets.token_hex(6); key="aig_"+secrets.token_urlsafe(24)
        value=asdict(Credential(ident,label,True,key[-6:],hashlib.sha256(key.encode()).hexdigest(),time.time()))
        values=self._read(); values[ident]=value; self.path.write_text(json.dumps(values,indent=2),encoding="utf-8"); self.path.chmod(0o600); return value | {"api_key":key}
    def revoke(self, ident: str) -> bool:
        values=self._read(); item=values.get(ident)
        if not item: return False
        item["enabled"]=False; self.path.write_text(json.dumps(values,indent=2),encoding="utf-8"); self.path.chmod(0o600); return True

class OperationLog:
    def __init__(self, path: Path | None = None): self.path=path or data_dir()/"operations.log"; self.path.parent.mkdir(parents=True,exist_ok=True)
    def append(self, action: str, target: str, result: str, detail: str | None = None):
        row={"id":str(uuid.uuid4()),"timestamp":time.time(),"action":action,"target":target,"result":result}
        if detail: row["detail"]=detail
        with self.path.open("a",encoding="utf-8") as f: f.write(json.dumps(row,ensure_ascii=False)+"\n")
        self.path.chmod(0o600)
    def read(self):
        try: return [json.loads(x) for x in self.path.read_text(encoding="utf-8").splitlines() if x.strip()]
        except OSError: return []
