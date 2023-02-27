from abc import ABCMeta, abstractmethod

# 继承ABCMeta元类，使其无法直接实例化
class BaseFileHandler(metaclass=ABCMeta):
    """
    json 处理 string类型对象；
    pickle 处理 bytes类型对象。
    用 str_like 来表明处理类型
    """
    str_like = True

    @abstractmethod
    def load_from_fileobj(self, file, **kwargs):
        pass

    @abstractmethod
    def dump_to_fileobj(self, obj, file, **kwargs):
        pass

    @abstractmethod
    def dump_to_str(self, obj, **kwargs):
        pass

    # 以下两个是对外接口
    def load_from_path(self, filepath: str, mode: str = 'r', **kwargs):
        with open(filepath, mode) as f:
            return self.load_from_fileobj(f, **kwargs)

    def dump_to_paht(self, obj, filepath: str, mode: str = 'w', **kwargs):
        with open(filepath, mode) as f:
            self.dump_to_fileobj(obj, f, **kwargs)
