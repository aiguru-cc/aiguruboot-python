from __future__ import annotations
import argparse
from dataclasses import dataclass, field

@dataclass
class VLLMConfig:
    model: str
    host: str = "127.0.0.1"
    port: int = 8000
    served_model_name: str | None = None
    tensor_parallel_size: int = 1
    max_model_len: int | None = None
    quantization: str | None = None
    api_key: str | None = None
    extra: list[str] = field(default_factory=list)

    def command(self) -> list[str]:
        if not self.model.strip(): raise ValueError("vLLM model is required")
        if not 1 <= self.port <= 65535 or self.tensor_parallel_size < 1: raise ValueError("invalid vLLM port or tensor parallel size")
        cmd=["vllm","serve",self.model,"--host",self.host,"--port",str(self.port),"--tensor-parallel-size",str(self.tensor_parallel_size)]
        if self.served_model_name: cmd += ["--served-model-name",self.served_model_name]
        if self.max_model_len is not None: cmd += ["--max-model-len",str(self.max_model_len)]
        if self.quantization: cmd += ["--quantization",self.quantization]
        if self.api_key: cmd += ["--api-key",self.api_key]
        return cmd + list(self.extra)

def add_vllm_arguments(parser: argparse.ArgumentParser):
    parser.add_argument("model"); parser.add_argument("--host",default="127.0.0.1"); parser.add_argument("--port",type=int,default=8000)
    parser.add_argument("--served-model-name"); parser.add_argument("--tensor-parallel-size",type=int,default=1)
    parser.add_argument("--max-model-len",type=int); parser.add_argument("--quantization"); parser.add_argument("--api-key")
    parser.add_argument("extra",nargs=argparse.REMAINDER)
