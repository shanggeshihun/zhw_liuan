
# @Time      : 2022/8/1  21:44
# @Author    : An
# @File      : decorators_demo_1.py
# @Software  : PyCharm

import os
import sys
import time


def file_name(file_dir):
    """
    :param file_dir:文件夹
    :return:返回文件夹下所有的文件名,以及文件完整路径
    """
    file_name_list = []
    file_path_list=[]
    for root, dirs, files in os.walk(file_dir):
        print(root,dirs,files,"\n")
        for file in files:
            # 只获取excel文件
            if '.py' in file:
                file_name_list.append(file)
                file_path_list.append(os.path.join(root,file))
        break
    return file_name_list,file_path_list

if __name__ == '__main__':
    file_dir = r"E:\工作文件\在刀锋\dofun\Python\Python\zhw_liuan\zhw\zhw_analysis\zhw_month_report\ZhwReport"
    file_name = file_name(file_dir)
    file_path_list = file_name[1]

    os.chdir(r"E:\工作文件\在刀锋\dofun\Python\Python\zhw_liuan\zhw\zhw_analysis\zhw_month_report\ZhwReport")
    for py_file in file_path_list:
        print("正在执行：",py_file)
        os.system("python {}".format(py_file))
        time.sleep(2)
