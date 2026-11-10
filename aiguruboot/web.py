from __future__ import annotations
import json
from pathlib import Path
from urllib.parse import urlparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from .hardware import inspect
from .audit import linux_preflight
from .engine import EngineManager
from .runtime import data_dir
from .vllm import VLLMConfig
from .discovery import discover_nodes
from .vapi import VapiClient, VapiError
from .vllm_env import download_model
from .cluster import Cluster

PAGE='''<!doctype html><meta charset="utf-8"><title>AiguruBoot 管理</title><style>body{font:16px sans-serif;max-width:1100px;margin:2em auto}pre{background:#f4f4f4;padding:1em;border-radius:8px;white-space:pre-wrap}button{padding:.6em 1em;margin:.2em}</style><h1>AiguruBoot 管理面</h1><p>模型、虚拟 API、引擎、vLLM 和集群</p><button onclick="load()">刷新</button><button onclick="engine('start')">启动 vLLM</button><button onclick="engine('stop')">停止 vLLM</button><pre id="out">加载中…</pre><script>async function load(){let r=await Promise.all(['/api/status','/api/hardware','/api/preflight','/api/models','/api/vapis','/api/engines','/api/vllm','/api/cluster'].map(x=>fetch(x).then(y=>y.json())));out.textContent=JSON.stringify({status:r[0],hardware:r[1],preflight:r[2],models:r[3],vapis:r[4],engines:r[5],vllm:r[6],cluster:r[7]},null,2)}async function engine(a){await fetch('/api/engines/vllm/'+a,{method:'POST'});load()}load()</script>'''
class Handler(BaseHTTPRequestHandler):
    def log_message(self,*args): pass
    def do_GET(self):
        if self.path=='/': data=PAGE.encode(); typ='text/html; charset=utf-8'
        elif self.path=='/status':
            path=Path(__file__).resolve().parent/'resources'/'status.html'
            if not path.exists(): path=Path(__file__).resolve().parents[1]/'Sources/AiguruBootCore/Resources/status.html'
            try: data=path.read_bytes(); typ='text/html; charset=utf-8'
            except OSError: self.send_error(404); return
        elif self.path=='/api/hardware': data=json.dumps(inspect()).encode(); typ='application/json'
        elif self.path=='/api/preflight': data=json.dumps(linux_preflight()).encode(); typ='application/json'
        elif self.path=='/api/status': data=json.dumps({'ok':True,'headless':True}).encode(); typ='application/json'
        elif self.path=='/api/models':
            path=data_dir()/"models.json"
            try: data=path.read_bytes()
            except OSError: data=b'[]'
            typ='application/json'
        elif self.path=='/api/vapis': data=json.dumps({'endpoint':'/v1/vapis','management':'use control token'}).encode(); typ='application/json'
        elif self.path=='/api/engines': data=json.dumps(EngineManager().status('vllm')).encode(); typ='application/json'
        elif self.path=='/api/vllm': data=json.dumps({'command':'aiguruboot vllm start MODEL','config':'use CLI or API'}).encode(); typ='application/json'
        elif self.path=='/api/cluster': data=json.dumps(discover_nodes()).encode(); typ='application/json'
        else: self.send_error(404); return
        self.send_response(200); self.send_header('Content-Type',typ); self.send_header('Content-Length',str(len(data))); self.end_headers(); self.wfile.write(data)
    def do_POST(self):
        length=int(self.headers.get('Content-Length','0') or 0)
        try: payload=json.loads(self.rfile.read(length) or b'{}')
        except (ValueError,TypeError): self.send_error(400,'invalid JSON'); return
        if self.path=='/api/models/download':
            try: result=str(download_model(payload['url'],Path(payload['destination']),payload.get('sha256'))); self._json(200,{'path':result})
            except (KeyError,OSError,ValueError) as exc: self._json(400,{'error':str(exc)})
            return
        if self.path=='/api/vapis':
            try: self._json(201,VapiClient(payload.get('base_url','http://127.0.0.1:11434'),payload.get('token')).create(payload.get('label','Agent'),payload.get('models')))
            except (VapiError,TypeError) as exc: self._json(502,{'error':str(exc)})
            return
        if self.path=='/api/vllm/config':
            path=data_dir()/'vllm.json'; path.write_text(json.dumps(payload,indent=2)); self._json(200,payload); return
        if self.path=='/api/cluster/nodes':
            path=data_dir()/'nodes.json'; nodes=json.loads(path.read_text()) if path.exists() else []
            if payload.get('action')=='remove': nodes=[n for n in nodes if n.get('url')!=payload.get('url')]
            else: nodes.append({'url':payload['url'],'name':payload.get('name',payload['url'])})
            path.write_text(json.dumps(nodes,indent=2)); self._json(200,nodes); return
        if self.path.startswith('/api/vapis/') and self.path.endswith('/rotate'):
            try: self._json(200,VapiClient(payload.get('base_url','http://127.0.0.1:11434'),payload.get('token')).rotate(self.path.split('/')[3]))
            except (VapiError,IndexError) as exc: self._json(502,{'error':str(exc)})
            return
        if self.path in ('/api/engines/vllm/start','/api/engines/vllm/stop'):
            manager=EngineManager()
            try:
                result=manager.start('vllm',['vllm','serve','local']) if self.path.endswith('/start') else manager.stop('vllm')
                self._json(200,result)
            except (OSError,RuntimeError) as exc: self.send_error(409,str(exc))
            return
        self.send_error(404)
    def _json(self,status,value):
        data=json.dumps(value,ensure_ascii=False).encode(); self.send_response(status); self.send_header('Content-Type','application/json'); self.send_header('Content-Length',str(len(data))); self.end_headers(); self.wfile.write(data)
def serve(host='127.0.0.1',port=8080):
    server=ThreadingHTTPServer((host,port),Handler)
    try: server.serve_forever()
    except KeyboardInterrupt: pass
    finally: server.server_close()
