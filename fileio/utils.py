import os
import os.path as osp
from pathlib import Path
from collections import abc

def is_str(x):
    return isinstance(x, str)

def is_filepath(x):
    return is_str(x) or isinstance(x, Path)

def is_seq_of(seq, expected_type, seq_type=None):
    if seq_type is None:
        exp_seq_type = abc.Sequence
    else:
        assert isinstance(seq_type, type)
        exp_seq_type = seq_type
    if not isinstance(seq, exp_seq_type):
        return False
    for item in seq:
        if not isinstance(item, expected_type):
            return False
    return True

def is_list_of(seq, expected_type):
    return is_seq_of(seq, expected_type, seq_type=list)

def has_method(obj: object, method: str) -> bool:
    """check whether the object has a method."""
    return hasattr(obj, method) and callable(getattr(obj, method))

def mkdir_or_exist(dir_name, mode=0o777):
    """0o777 = 511 permission"""
    if dir_name == '':
        return
    dir_name = osp.expanduser(dir_name)
    os.makedirs(dir_name, mode=mode, exist_ok=True)
