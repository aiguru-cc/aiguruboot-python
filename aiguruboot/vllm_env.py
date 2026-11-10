from __future__ import annotations
import hashlib, os, platform, shutil, subprocess, sys
from pathlib import Path
from urllib.request import Request, urlopen

def detect_environment() -> dict:
    gpu = shutil.which("nvidia-smi") is not None
    cuda = shutil.which("nvcc") is not None
    return {"python": sys.version.split()[0], "platform": platform.system().lower(),
            "architecture": platform.machine(), "gpu": gpu, "cuda": cuda,
            "vllm": shutil.which("vllm"), "pip": shutil.which("pip") or shutil.which("pip3")}

def install_command(cuda: bool | None = None) -> list[str]:
    use_cuda = detect_environment()["cuda"] if cuda is None else cuda
    return [sys.executable, "-m", "pip", "install", "vllm"] + ([] if use_cuda else ["--extra-index-url", "https://download.pytorch.org/whl/cpu"])

def download_model(url: str, destination: Path, sha256: str | None = None, chunk_size: int = 1024 * 1024) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".part")
    digest = hashlib.sha256()
    if temporary.exists():
        with temporary.open("rb") as existing:
            while True:
                block=existing.read(chunk_size)
                if not block: break
                digest.update(block)
    offset=temporary.stat().st_size if temporary.exists() else 0
    headers={"User-Agent":"aiguruboot/1.0"}
    if offset: headers["Range"]=f"bytes={offset}-"
    mode="ab" if offset else "wb"
    with urlopen(Request(url, headers=headers), timeout=60) as response, temporary.open(mode) as output:
        if offset and getattr(response,"status",206) != 206:
            output.close(); temporary.unlink(missing_ok=True); offset=0
            return download_model(url,destination,sha256,chunk_size)
        while True:
            block = response.read(chunk_size)
            if not block: break
            output.write(block); digest.update(block)
    if sha256 and digest.hexdigest().lower() != sha256.lower():
        temporary.unlink(missing_ok=True); raise ValueError("model checksum mismatch")
    os.replace(temporary, destination)
    return destination
