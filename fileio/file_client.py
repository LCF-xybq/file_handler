import os
import os.path as osp
import tempfile
import inspect
from pathlib import Path
from urllib.request import urlopen
from contextlib import contextmanager
from abc import ABCMeta, abstractmethod
from typing import Any, Generator, Iterator, Optional, Tuple, Union
from .utils import mkdir_or_exist, is_filepath


class BaseStorgeBackend(metaclass=ABCMeta):
    """
    get(): reads the file as a byte stream.
    get_text(): reads the file as texts.
    """
    _allow_symlink = False

    """@property创建只读属性"""
    @property
    def name(self):
        return self.__class__.__name__

    @property
    def allow_symlink(self):
        return self._allow_symlink

    @abstractmethod
    def get(self, filepath):
        pass

    @abstractmethod
    def get_text(self, filepath):
        pass


class MemcachedBackend(BaseStorgeBackend):
    """Memcached storage backend. need large memory.
    Attributes:
        server_list_cfg (str): Config file for memcached server list.
        client_cfg (str): Config file for memcached client.
        sys_path (str | None): Additional path to be appended to `sys.path`.
            Default: None.
    """
    def __init__(self, server_list_cfg, client_cfg, sys_path=None):
        if sys_path is not None:
            import sys
            sys.path.append(sys_path)
        try:
            import mc
        except ImportError:
            raise ImportError(
                f'Please install memcached to enable MemcachedBackend.'
            )

        self.server_list_cfg = server_list_cfg
        self.client_cfg = client_cfg
        self._client = mc.MemcachedClient.GetInstance(self.server_list_cfg,
                                                      self.client_cfg)
        # mc.pyvector servers as a point which points to a memory cache
        self._mc_buffer = mc.pyvector()

    def get(self, filepath):
        filepath = str(filepath)
        import mc
        self._client.Get(filepath, self._mc_buffer)
        value_buf = mc.ConvertBuffer(self._mc_buffer)
        return value_buf

    def get_text(self, filepath, encoding=None):
        raise NotImplementedError


class LmdbBackend(BaseStorgeBackend):
    """Lmdb storage backend.

    Args:
        db_path (str): Lmdb database path.
        readonly (bool, optional): Lmdb environment parameter. If True,
            disallow any write operations. Default: True.
        lock (bool, optional): Lmdb environment parameter. If False, when
            concurrent access occurs, do not lock the database. Default: False.
        readahead (bool, optional): Lmdb environment parameter. If False,
            disable the OS filesystem readahead mechanism, which may improve
            random read performance when a database is larger than RAM.
            Default: False.

    Attributes:
        db_path (str): Lmdb database path.
    """
    def __init__(self,
                 db_path,
                 readonly=True,
                 lock=False,
                 readahead=False,
                 **kwargs):
        try:
            import lmdb
        except ImportError:
            raise ImportError('Please install lmdb to enable LmdbBackend.')

        self.dp_path = str(db_path)
        self.readonly = readonly
        self.lock = lock
        self.readahead = readahead
        self.kwargs = kwargs
        self._client = None

    def get(self, filepath):
        """Get values according to the filepath.

        Args:
            filepath (str | obj:`Path`): Here, filepath is the lmdb key.
        """
        if self._client is None:
            self._client = self._get_client()

        with self._client.begin(write=False) as txn:
            value_buf = txn.get(str(filepath).encode('utf-8'))
        return value_buf

    def get_text(self, filepath):
        raise NotImplementedError


    def _get_client(self):
        import lmdb

        return lmdb.open(
            self.dp_path,
            readonly=self.readonly,
            lock=self.lock,
            readahead=self.readahead,
            **self.kwargs
        )

    def __del__(self):
        self._client.close()


class HardDiskBackend(BaseStorgeBackend):
    _allow_symlink = True

    def get(self, filepath: Union[str, Path]) -> bytes:
        with open(filepath, 'rb') as f:
            value_buf = f.read()
        return value_buf

    def get_text(self,
                 filepath: Union[str, Path],
                 encoding: str = 'utf-8') -> str:
        with open(filepath, encoding=encoding) as f:
            value_buf = f.read()
        return value_buf

    def put(self,
            obj: bytes,
            filepath: Union[str, Path]) -> None:
        mkdir_or_exist(osp.dirname(filepath))
        with open(filepath, 'wb') as f:
            f.write(obj)

    def put_text(self,
                 obj: str,
                 filepath: Union[str, Path],
                 encoding: str = 'utf-8') -> None:
        mkdir_or_exist(osp.dirname(filepath))
        with open(filepath, 'w', encoding=encoding) as f:
            f.write(obj)

    def remove(self, filepath: Union[str, Path]) -> None:
        os.remove(filepath)

    def exists(self, filepath: Union[str,Path]) -> bool:
        return osp.exists(filepath)

    def isdir(self, filepath: Union[str,Path]) -> bool:
        return osp.isdir(filepath)

    def isfile(self, filepath: Union[str,Path]) -> bool:
        return osp.isfile(filepath)

    def join_path(self, filepath: Union[str,Path],
                  *filepaths: Union[str, Path]) -> str:
        return osp.join(filepath, *filepaths)

    @contextmanager
    def get_local_path(self,
                       filepath: Union[str,
                       Path]) -> Generator[Union[str, Path], None, None]:
        """for unified API and do nothing"""
        yield filepath

    def list_dir_or_file(self,
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


class HTTPBackend(BaseStorgeBackend):
    def get(self, filepath):
        value_buf = urlopen(filepath).read()
        return value_buf

    def get_text(self, filepath, encoding='utf-8'):
        value_buf = urlopen(filepath).read()
        return value_buf.decode(encoding)

    @contextmanager
    def get_local_path(self, filepath: str) -> Generator[Union[str, Path], None, None]:
        """
        Download a file from ``filepath``.

        ``get_local_path`` is decorated by :meth:`contxtlib.contextmanager`. It
        can be called with ``with`` statement, and when exists from the
        ``with`` statement, the temporary path will be released. !!!

        Args:
            filepath (str): Download a file from ``filepath``.

        Examples:
            >>> client = HTTPBackend()
            >>> # After existing from the ``with`` clause,
            >>> # the path will be removed
            >>> with client.get_local_path('http://path/of/your/file') as path:
            ...     # do something here
        """
        try:
            f = tempfile.NamedTemporaryFile(delete=False)
            f.write(self.get(filepath))
            f.close()
            yield f.name
        finally:
            os.remove(f.name)

class FileClient:
    """A general file client to access files in different backends.

        The client loads a file or text in a specified backend from its path
        and returns it as a binary or text file. There are two ways to choose a
        backend, the name of backend and the prefix of path. Although both of them
        can be used to choose a storage backend, ``backend`` has a higher priority
        that is if they are all set, the storage backend will be chosen by the
        backend argument. If they are all `None`, the disk backend will be chosen.
        Note that It can also register other backend accessor with a given name,
        prefixes, and backend class. In addition, We use the singleton pattern to
        avoid repeated object creation. If the arguments are the same, the same
        object will be returned.

        Args:
            backend (str, optional): The storage backend type. Options are "disk",
                "memcached", "lmdb", and "http". Default: None.
            prefix (str, optional): The prefix of the registered storage backend.
                Options are "http", "https". Default: None.

        Examples:
            >>> # only set backend
            >>> file_client = FileClient(backend='petrel')
            >>> # only set prefix
            >>> file_client = FileClient(prefix='s3')
            >>> # set both backend and prefix but use backend to choose client
            >>> file_client = FileClient(backend='petrel', prefix='s3')
            >>> # if the arguments are the same, the same object is returned
            >>> file_client1 = FileClient(backend='petrel')
            >>> file_client1 is file_client
            True

        Attributes:
            client (:obj:`BaseStorageBackend`): The backend object.
        """
    _backends = {
        'disk': HardDiskBackend,
        'memcached': MemcachedBackend,
        'lmdb': LmdbBackend,
        'http': HTTPBackend,
    }

    _prefix_to_backends = {
        'http': HTTPBackend,
        'https': HTTPBackend,
    }

    _instances: dict = {}

    client: Any

    def __new__(cls, backend=None, prefix=None, **kwargs):
        if backend is None and prefix is None:
            backend = 'disk'
        if backend is not None and backend not in cls._backends:
            raise ValueError(
                f'Backned {backend} is not supported.'
            )
        if prefix is not None and prefix not in cls._prefix_to_backends:
            raise ValueError(
                f'prefix {prefix} is not supported.'
            )

        # concatenate the arguments to a unique key for determining whether
        # objects with the same arguments were created
        arg_key = f'{backend}:{prefix}'
        for key, value in kwargs.items():
            arg_key += f':{key}:{value}'

        if arg_key in cls._instances:
            _instance = cls._instances[arg_key]
        else:
            # create a new object and put it to _instance
            _instance = super().__new__(cls)
            if backend is not None:
                _instance.client = cls._backends[backend](**kwargs)
            else:
                _instance.client = cls._prefix_to_backends[prefix](**kwargs)

            cls._instances[arg_key] = _instance

        return _instance

    @property
    def name(self):
        return self.client.name

    @property
    def allow_symlink(self):
        return self.client.allow_symlink

    @staticmethod
    def parse_uri_prefix(uri: Union[str, Path]) -> Optional[str]:
        """Parse the prefix of a uri.

        Args:
            uri (str | Path): Uri to be parsed that contains the file prefix.

        Examples:
            >>> FileClient.parse_uri_prefix('s3://path/of/your/file')
            's3'
        """
        assert is_filepath(uri)
        uri = str(uri)
        if '://' not in uri:
            return None
        else:
            # do not support prefix like 'xxx:yyy'
            prefix, _ = uri.split('://')
            return prefix

    @classmethod
    def infer_client(cls,
                     file_client_args: Optional[dict] = None,
                     uri: Optional[Union[str, Path]] = None) -> 'FileClient':
        """Infer a suitable file client based on the URI and arguments.
            file_client_args = {'backend': 'disk/http/...'}
        Returns:
            FileClient: Instantiated FileClient object.
        """
        assert  file_client_args is not None or uri is not None
        if file_client_args is None:
            file_prefix = cls.parse_uri_prefix(uri)
            return  cls(prefix=file_prefix)
        else:
            return cls(**file_client_args)

    @classmethod
    def _register_backend(cls, name, backend, force=False, prefixes=None):
        if not isinstance(name, str):
            raise TypeError('name should be a string.')
        if not inspect.isclass(backend):
            raise TypeError('backend should be a class.')
        if not issubclass(backend, BaseStorgeBackend):
            raise TypeError('backend should be a subclass of BaseStorageBackend.')
        if not force and name in cls._backends:
            raise KeyError(f'{name} is already registered, '
                    'set force=True to override it.')

        if name in cls._backends and force:
            for arg_key, instance in list(cls._instances.items()):
                if isinstance(instance.client, cls._backends[name]):
                    cls._instances.pop(arg_key)
        cls._backends[name] = backend

        if prefixes is not None:
            if isinstance(prefixes, str):
                prefixes = [prefixes]
            else:
                assert isinstance(prefixes, (list, tuple))
            for prefix in prefixes:
                if prefix not in cls._prefix_to_backends:
                    cls._prefix_to_backends[prefix] = backend
                elif (prefix in cls._prefix_to_backends) and force:
                    overridden_backend = cls._prefix_to_backends[prefix]
                    if isinstance(overridden_backend, list):
                        overridden_backend = tuple(overridden_backend)
                    for arg_key, instance in list(cls._instances.items()):
                        if isinstance(instance.client, overridden_backend):
                            cls._instances.pop(arg_key)
                    cls._prefix_to_backends[prefix] = backend
                else:
                    raise KeyError(f'{prefix} is already registered, '
                            'set force=True to override it.')

    @classmethod
    def register_backend(cls, name, backend=None, force=False, prefixes=None):
        """
        force (bool, optional): Whether to override the backend if the name
                has already been registered. Defaults to False.
        """
        if backend is not None:
            cls._register_backend(name, backend,
                                  force=force, prefixes=prefixes)
            return

        def _register(backend_cls):
            cls._register_backend(name, backend_cls,
                                  force=force, prefixes=prefixes)
            return backend_cls

        return _register

    def get(self, filepath: Union[str, Path]) -> Union[bytes, memoryview]:
        """Read data from a given ``filepath`` with 'rb' mode.

        Note:
            There are two types of return values for ``get``, one is ``bytes``
            and the other is ``memoryview``. The advantage of using memoryview
            is that you can avoid copying, and if you want to convert it to
            ``bytes``, you can use ``.tobytes()``.

        Args:
            filepath (str or Path): Path to read data.

        Returns:
            bytes | memoryview: Expected bytes object or a memory view of the
            bytes object.
        """
        return self.client.get(filepath)

    def get_text(self, filepath: Union[str, Path], encoding='utf-8') -> str:
        """Read data from a given ``filepath`` with 'r' mode."""
        return self.client.get_text(filepath, encoding)

    def put(self, obj: bytes, filepath: Union[str, Path]) -> None:
        """Write data to a given ``filepath`` with 'wb' mode.

        Note:
            ``put`` should create a directory if the directory of ``filepath``
            does not exist.

        Args:
            obj (bytes): Data to be written.
            filepath (str or Path): Path to write data.
        """
        self.client.put(obj, filepath)

    def put_text(self, obj: str, filepath: Union[str, Path]) -> None:
        """Write data to a given ``filepath`` with 'w' mode."""
        self.client.put_text(obj, filepath)

    def remove(self, filepath: Union[str, Path]) -> None:
        self.client.remove(filepath)

    def exists(self, filepath: Union[str, Path]) -> bool:
        self.client.exists(filepath)

    def isdir(self, filepath: Union[str, Path]) -> bool:
        self.client.isdir(filepath)

    def isfile(self, filepath: Union[str, Path]) -> bool:
        self.client.isfile(filepath)

    def join_path(self, filepath: Union[str, Path],
                  *filepaths: Union[str, Path]) -> str:
        return self.join_path(filepath, *filepaths)

    @contextmanager
    def get_local_path(
            self,
            filepath: Union[str, Path]) -> Generator[Union[str, Path], None, None]:
        """Download data from ``filepath`` and write the data to local path.
        Note:
            If the ``filepath`` is a local path, just return itself.
        Args:
            filepath (str or Path): Path to be read data.
        Examples:
            >>> file_client = FileClient(prefix='s3')
            >>> with file_client.get_local_path('s3://bucket/abc.jpg') as path:
            ...     # do something here
        Yields:
            Iterable[str]: Only yield one path.
        """
        with self.client.get_local_path(str(filepath)) as local_path:
            yield local_path

    def list_dir_or_file(self,
                         dir_path: Union[str, Path],
                         list_dir: bool = True,
                         list_file: bool = True,
                         suffix: Optional[Union[str, Tuple[str]]] = None,
                         recursive: bool = False) -> Iterator[str]:
        yield from self.client.list_dir_or_file(dir_path, list_dir,
                                                list_file, suffix, recursive)
