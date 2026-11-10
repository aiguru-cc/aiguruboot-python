from __future__ import annotations
import json
from urllib.request import urlopen
from urllib.error import URLError
class Cluster:
    def __init__(self,nodes=None): self.nodes=nodes or []
    def add(self,url,name=None): self.nodes.append({'url':url.rstrip('/'),'name':name or url}); return self.nodes[-1]
    def health(self,timeout=5):
        result=[]
        for node in self.nodes:
            try:
                with urlopen(node['url']+'/health',timeout=timeout) as r: result.append({**node,'ok':r.status==200})
            except (OSError, URLError, ValueError) as exc: result.append({**node,'ok':False,'error':str(exc)})
        return result
    def endpoint(self, path="/v1/chat/completions"):
        healthy=self.health()
        for node in healthy:
            if node.get('ok'): return node['url']+path
        raise RuntimeError('no healthy cluster node')
