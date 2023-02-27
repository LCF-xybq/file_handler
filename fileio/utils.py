import os
import os.path as osp
from pathlib import Path

def is_str(x):
    return isinstance(x, str)

def is_filepath(x):
    return is_str(x) or isinstance(x, Path)

def has_method(obj: object, method: str) -> bool:
    """check whether the object has a method."""
    return hasattr(obj, method) and callable(getattr(obj, method))

def mkdir_or_exist(dir_name, mode=0o777):
    """0o777 = 511 permission"""
    if dir_name == '':
        return
    dir_name = osp.expanduser(dir_name)
    os.makedirs(dir_name, mode=mode, exist_ok=True)
