import os
import os.path as osp
from pathlib import Path
from pathlib import Path
from fileio import JsonHandler
from typing import Any, Generator, Iterator, Optional, Tuple, Union


def list_dir_or_file(
                     dir_path: Union[str, Path],
                     list_dir: bool = True,
                     list_file: bool = True,
                     suffix: Optional[Union[str, Tuple[str]]] = None,
                     recursive: bool = False) -> Iterator[str]:
    """scan a directory to find the interested directories or
    files in arbitrary order.
    Note:
        return the relative to 'dir_path'
    Args:
        dir_path (str | Path): Path of the directory.
        list_dir (bool): List the directories. Default: True.
        list_file (bool): List the path of files. Default: True.
        suffix (str or tuple[str], optional):  File suffix
            that we are interested in. Default: None.
        recursive (bool): If set to True, recursively scan the
            directory. Default: False.
    Yields:
        Iterable[str]: A relative path to ``dir_path``.
    """
    if list_dir and suffix is not None:
        raise TypeError('`suffix` should be None when `list_dir` is True')

    if (suffix is not None) and not isinstance(suffix, (str, tuple)):
        raise TypeError('`suffix` must be a string or tuple of strings')

    root = dir_path

    def _list_dir_or_file(dir_path, list_dir, list_file,
                          suffix, recursive):
        for entry in os.scandir(dir_path):
            if not entry.name.startswith('.') and entry.is_file():
                rel_path = osp.relpath(entry.path, root)
                if (suffix is None
                    or rel_path.endswith(suffix)) and list_file:
                    yield rel_path
                elif osp.isdir(entry.path):
                    if list_dir:
                        rel_dir = osp.relpath(entry.path, root)
                        yield rel_dir
                    if recursive:
                        yield from _list_dir_or_file(entry.path, list_dir,
                                                     list_file, suffix,
                                                     recursive)

    return _list_dir_or_file(dir_path, list_dir, list_file,
                             suffix, recursive)


if __name__ == '__main__':
    pth = r'D:\Program_self\Datasets\UIEB\raw-890\aa'
    ite = list_dir_or_file(
        dir_path=pth,
        list_dir=True,
        recursive=True,
    )
    print(next(ite))
    print(next(ite))
    print(next(ite))