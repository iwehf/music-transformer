from io import BytesIO
from typing import Dict

import torch


def load_data(data_bytes: bytes) -> Dict[str, torch.Tensor]:
    f = BytesIO(data_bytes)
    return torch.load(f, map_location=torch.device("cpu"))


def dump_data(data_bytes: Dict[str, torch.Tensor]) -> bytes:
    f = BytesIO()
    torch.save(data_bytes, f)
    return f.getvalue()
