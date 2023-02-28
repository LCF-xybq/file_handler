import os
import os.path as osp
from fileio import FileClient


if __name__ == '__main__':
    fc_web = FileClient(prefix='https')
    fc_local = FileClient(backend='disk')
    web_pth = 'https://github.com/IPNUISTlegal/underwater-test-dataset-U45-/blob/master/upload/U45/U45/1.png?raw=true'
    local_pth = r'D:\Program_self\file_handler\get_from_http.png'

    # with fc_web.client.get_local_path(web_pth) as path:
    #     print(path)

    result = fc_web.client.get(web_pth)
    fc_local.put(result, local_pth)