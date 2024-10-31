from io import BytesIO
from os import PathLike
from typing import IO, BinaryIO, Dict, Union

import torch

FILE_LIKE = Union[str, PathLike, BinaryIO, IO[bytes]]


def loads_data(data_bytes: bytes) -> Dict[str, torch.Tensor]:
    f = BytesIO(data_bytes)
    return torch.load(f, map_location=torch.device("cpu"))


def dumps_data(data_bytes: Dict[str, torch.Tensor]) -> bytes:
    f = BytesIO()
    torch.save(data_bytes, f)
    return f.getvalue()


def load_data(file: FILE_LIKE) -> Dict[str, torch.Tensor]:
    return torch.load(file, map_location=torch.device("cpu"))


def dump_data(data: Dict[str, torch.Tensor], file: FILE_LIKE):
    torch.save(data, file)
