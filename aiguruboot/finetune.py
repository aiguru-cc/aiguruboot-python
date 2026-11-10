from dataclasses import dataclass, asdict
import json
import subprocess
@dataclass
class FineTuneConfig:
    iters:int=200; learning_rate:float=1e-5; batch_size:int=4; lora_layers:int=4; rank:int=8; max_seq_length:int=2048; steps_per_report:int=10; save_every:int=50; note:str=''
    def validate(self):
        if self.iters<1 or self.learning_rate<=0 or self.batch_size<1 or self.rank<1 or self.max_seq_length<1: raise ValueError('invalid fine-tune configuration')
    def to_json(self): self.validate(); return json.dumps(asdict(self),ensure_ascii=False,indent=2)
    def command(self, model, dataset):
        self.validate(); return ['python','-m','mlx_lm','lora','--model',model,'--data',dataset,'--iters',str(self.iters),'--batch-size',str(self.batch_size),'--learning-rate',str(self.learning_rate),'--lora-layers',str(self.lora_layers)]
