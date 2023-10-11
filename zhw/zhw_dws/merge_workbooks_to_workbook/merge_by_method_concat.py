# _*_coding:utf-8 _*_
# @Time　　 :2021/5/20   0:58
# @Author　 :
# @File　　 :merge_by_method_concat.py
# @Theme    :PyCharm

import os,xlrd,xlsxwriter


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
            if '.xlsx' in file:
                file_name_list.append(file)
                file_path_list.append(os.path.join(root,file))
    return file_name_list,file_path_list

def get_allsheet_row_values(workbook_file_path):
    """
    :param workbook_file_path:工作簿路径
    :return:工作簿中所有的工作表的数据
    """
    workbook_nrows_values=[]
    data=xlrd.open_workbook(workbook_file_path)
    sheet_name_list=data.sheet_names()
    for sheet in sheet_name_list:
        sheet_data=data.sheet_by_name(sheet)
        data_nrows=sheet_data.nrows
        for i in range(data_nrows):
            workbook_nrows_values.append(sheet_data.row_values(i))
    return  workbook_nrows_values

def write_to_workbook(workbook_path,list_nest_list):
    """
    :param workbook_path: 合并到目标工作簿
    :param list_nest_list:以列表作为元素的列表
    :return:
    """
    workbook = xlsxwriter.Workbook(workbook_path)
    bold = workbook.add_format({'bold': 1})
    worksheet = workbook.add_worksheet('total')
    for row_num, row_data in enumerate(list_nest_list):
        worksheet.write_row(row_num, 0, row_data)
    workbook.close()

if __name__ == '__main__':
    dir_path=r'./excel_files'
    file_name_list,file_path_list=file_name(dir_path)
    file_zip=zip(file_name_list,file_path_list)

    all_workbook_nrows_values=[]
    for file_name,file_path in zip(file_name_list,file_path_list):
        workbook_nrows_values=get_allsheet_row_values(file_path)
        all_workbook_nrows_values.extend(workbook_nrows_values)

    save_workbook_path=r'./save_dir/merge.xlsx'
    write_to_workbook(save_workbook_path, all_workbook_nrows_values)