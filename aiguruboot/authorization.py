from __future__ import annotations
import hashlib,hmac,json,secrets,time
from pathlib import Path
from .runtime import data_dir
class FamilyAuthorization:
    def __init__(self,directory:Path|None=None): self.root=directory or data_dir(); self.root.mkdir(parents=True,exist_ok=True); self.auth=self.root/'model-family-authorization.json'; self.key=self.root/'model-family-authorization.key'
    def authorize(self,family):
        if not family or len(family)>64 or any(ord(c)<32 for c in family): raise ValueError('invalid model family')
        if not self.key.exists(): self.key.write_text(secrets.token_hex(32)); self.key.chmod(0o600)
        payload={'version':1,'allowed_family':family,'issued_at':time.time(),'nonce':secrets.token_hex(16)}
        sig=hmac.new(bytes.fromhex(self.key.read_text()),json.dumps(payload,sort_keys=True).encode(),hashlib.sha256).hexdigest()
        self.auth.write_text(json.dumps(payload|{'signature':sig},indent=2)); self.auth.chmod(0o600)
    def status(self):
        if not self.auth.exists(): return {'status':'unrestricted'}
        try:
            value=json.loads(self.auth.read_text()); expected=hmac.new(bytes.fromhex(self.key.read_text()),json.dumps({k:value[k] for k in ('version','allowed_family','issued_at','nonce')},sort_keys=True).encode(),hashlib.sha256).hexdigest()
            return {'status':'authorized','family':value['allowed_family']} if hmac.compare_digest(expected,value['signature']) else {'status':'invalid'}
        except (OSError,ValueError,KeyError): return {'status':'invalid'}
