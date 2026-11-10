from __future__ import annotations
import json, os, signal, subprocess
from pathlib import Path
from .runtime import data_dir

class EngineManager:
    def __init__(self, state_dir: Path | None = None):
        self.root=(state_dir or data_dir()/"engines"); self.root.mkdir(parents=True,exist_ok=True); self.processes={}
    def _pid(self,name): return self.root/(name+".pid")
    def status(self,name):
        try: pid=int(self._pid(name).read_text())
        except (OSError,ValueError): return {"name":name,"running":False}
        running=True
        if os.name == "nt":
            try: os.kill(pid,0)
            except OSError: running=False
        else:
            try: os.kill(pid,0)
            except OSError: running=False
        if not running:
            try: self._pid(name).unlink()
            except OSError: pass
        return {"name":name,"running":running,"pid":pid if running else None}
    def start(self,name,command):
        if self.status(name)["running"]: raise RuntimeError("engine already running")
        log=self.root/(name+".log"); out=log.open("ab")
        try:
            process=subprocess.Popen(command, stdout=out, stderr=subprocess.STDOUT,
                                     start_new_session=(os.name != "nt"))
        finally:
            # The child inherits the descriptor. The parent must close its copy
            # or every engine start leaks one descriptor until the CLI exits.
            out.close()
        self.processes[name]=process; self._pid(name).write_text(str(process.pid)); return self.status(name)
    def stop(self,name):
        state=self.status(name)
        if not state["running"]: return state
        try: os.kill(state["pid"], signal.SIGTERM)
        except OSError: pass
        process=self.processes.pop(name, None)
        if process is not None:
            try: process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill(); process.wait(timeout=2)
        try:
            os.waitpid(state["pid"], os.WNOHANG)
        except (AttributeError, ChildProcessError, OSError):
            pass
        try: self._pid(name).unlink()
        except OSError: pass
        return {"name":name,"running":False,"pid":None}
