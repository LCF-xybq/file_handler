from io import BytesIO, StringIO
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, TextIO, Union

from .utils import is_list_of
from .file_client import FileClient
from .handlers import BaseFileHandler, JsonHandler


FileLikeObject = Union[TextIO, StringIO, BytesIO]

file_handlers = {
    'json': JsonHandler()
}





def dump(obj: Any,
         file: Optional[Union[str, Path, FileLikeObject]] = None,
         file_format: Optional[str] = None,
         file_client_args: Optional[Dict] = None,
         **kwargs):
    """
    Dump data to json strings or files.
    Args:
        obj (any): The python object to be dumped.
        file (str or :obj:`Path` or file-like object, optional): If not
            specified, then the object is dumped to a str, otherwise to a file
            specified by the filename or file-like object.
        file_format:
        file_client_args:
        **kwargs:

    Examples:
        >>> dump('hello world', '/path/of/your/file')  # disk

    Returns:
        bool: True for success, False otherwise.
    """
    if isinstance(file, Path):
        file = str(file)
    if file_format is None:
        if isinstance(file, str):
            file_format = file.split('.')[-1]
        elif file is None:
            raise ValueError(
                'file_format must be specified since file is None')
    if file_format not in file_handlers:
        raise TypeError(f'Unsupported format: {file_format}')
    f: FileLikeObject
    handler = file_handlers[file_format]
    if file is None:
        return handler.dump_to_str(obj, **kwargs)
    elif isinstance(file, str):
        file_client = FileClient.infer_client(file_client_args, file)
        if handler.str_like:
            with StringIO() as f:
                handler.dump_to_fileobj(obj, f, **kwargs)
                file_client.put_text(f.getvalue(), file)
        else:
            with BytesIO() as f:
                handler.dump_to_fileobj(obj, f, **kwargs)
                file_client.put(f.getvalue(), file)
    elif hasattr(file, 'write'):
        handler.dump_to_fileobj(obj, file, **kwargs)
    else:
        raise TypeError('"file" must be a filename str or a file-object')
