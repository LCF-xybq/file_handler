from pathlib import Path
from fileio import JsonHandler
from typing import Any, Callable, Dict, List, Optional, TextIO, Union

import numpy as np

FileLikeObject = Union[TextIO]

file_handlers = {
    'json': JsonHandler(),
}

def load(file: Union[str, Path, FileLikeObject],
         file_format: Optional[str] = None,
         **kwargs):
    """
    load from json/yaml/picle.

    :param file: filename or file-like object.
    :param file_format: json, yaml/yml, pickle/pkl.
    :param kwargs:
    :return: contents of the file

    >>> load('/path_to_file')

    """
    if isinstance(file, Path):
        file = str(file)
    if file_format is None and isinstance(file, str):
        file_format = file.split('.')[-1]
    if file_format not in file_handlers:
        raise TypeError(f'Unsupported format: {file_format}')

    handler = file_handlers[file_format]
    f: FileLikeObject
    if isinstance(file, str):
        pass

if __name__ == '__main__':
    pass