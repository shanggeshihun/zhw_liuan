# _*_coding:utf-8 _*_
# @Time　　 :2022/6/21   17:27
# @Author　 :
# @File　　 :SplitToSublist.py
# @Theme    :

import numpy as np
class SplitToSublist:
    def __init__(self,lst,step):
        """
        :param lst: 单个元素组成的列表
        :param step:切分的子列表的元素的数量
        """
        self.lst = lst
        self.step = step

    def split_to_sublist(self):
        """
        :return:以子列表作为列表元素的新列表
        """
        sublist =  []
        sublist_length = int(np.ceil(len(self.lst)/self.step))
        for i in range(sublist_length):
            start_index = i * self.step
            end_index = (i+1) * self.step
            tmp_end_index = len(self.lst)
            end_index = min(end_index,tmp_end_index)
            sublist.append(self.lst[start_index:end_index])
        return  sublist


if __name__ == '__main__':
    lst = [1,2,3,4,4,54,5,6,6,7,8,98]
    step = 5
    split_to_sublist= SplitToSublist(lst,step)
    split_to_sublist = split_to_sublist.split_to_sublist()
    print(split_to_sublist)