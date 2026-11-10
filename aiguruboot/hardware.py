from __future__ import annotations
import os, platform, shutil
def inspect() -> dict:
    physical = None
    if hasattr(os, "sysconf"):
        try: physical=os.sysconf("SC_PHYS_PAGES")*os.sysconf("SC_PAGE_SIZE")
        except (ValueError,OSError): pass
    gpu=shutil.which("nvidia-smi") is not None
    return {"architecture":platform.machine(),"cpu_count":os.cpu_count() or 1,"physical_memory_bytes":physical,"gpu":gpu,"model_budget_bytes":int(physical*.75) if physical else None}
