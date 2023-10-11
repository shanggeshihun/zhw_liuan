# _*_coding:utf-8 _*_

# @Time      : 2022/5/19  19:02
# @Author    : An
# @File      : zhw_main.py
# @Software  : PyCharm


import sys, os, time, datetime, platform

plat = platform.system().lower()
if plat == 'windows':
    sys.path.append("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig")
    sys.path.append("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/zhw/zhw_ads")
    sys.path.append("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/zhw/zhw_dws")
    sys.path.append("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/zhw/zhw_dwm")
elif plat == 'linux':
    sys.path.append("/work/project/zhw_product/liuan/PublicConfig")
    sys.path.append("/work/project/zhw_product/liuan/zhw/zhw_ads")
    sys.path.append("/work/project/zhw_product/liuan/zhw/zhw_dws")
    sys.path.append("/work/project/zhw_product/liuan/zhw/zhw_dwm")
else:
    sys.exit()

from SchedualToMysql import SchedualInfo

from AdsStatistics import Ads
from DwsStatistics import Dws
from DwmStatistics import Dwm


def running_result(class_instance_name, class_method, part_day):
    """
    :param class_instance_name: 实例化的名称
    :param class_method: 示例类的方法名称
    :param part_day: 参数
    :return: 执行结果写入MySQL
    """
    start_time = time.time()

    try:
        exec("{0}.{1}('{2}')".format(class_instance_name, class_method, part_day))
    except Exception as e:
        status = "失败"
        info = e
        end_time = time.time()
    else:
        status, info = "成功", "成功"
        end_time = time.time()
    running_seconds = end_time - start_time
    sche = SchedualInfo()
    sche.schedual_to_mysql(part_day, class_method.replace("_real", ""),
                           time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time)),
                           time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time)), running_seconds, '成功', '执行成功')


if __name__ == '__main__':

    today = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
    print('程序执行日期：', datetime.datetime.now())
    #####################  dim 维度表  #####################

    #####################  dwm 主题表  #####################
    for i in [1]:
        part_day = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
        dwm = Dwm()
        class_method = "zhw_dwm_dau_day"
        running_result('dwm', class_method, part_day)



    #####################  ads 主题表  #####################
    # for i in [1]:
    #     part_day = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
    #     ads = Ads()
    #     class_method = "zhw_withdraw_charge_warning_popup_day_statistics"
    #     running_result('ads', class_method, part_day)
    #
    #     ads = Ads()
    #     class_method = "zhw_withdraw_charge_warning_popup_month_statistics"
    #     running_result('ads', class_method, part_day)

    #####################  dws 主题表  #####################
    # 短信召召回效果——数据（每日 以4日为单位重跑）
    # lst = [4 * i for i in range(1, 14)]
    # lst.reverse()
    # for i in lst:
    #     start_time = time.time()
    #     day_last_1 = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y-%m-%d')  # t-1
    #
    #     dws = Dws()
    #     class_method = "zhw_dws_msg_recall_statistics"
    #     running_result('dws', class_method, day_last_1)
    #
    #     end_time = time.time()
    #     print('执行完成：', day_last_1, '耗时:', end_time - start_time)

    # 短信召回效果——数据（重跑所有短信发送量）
    # day_last_1 = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    #
    # dws = Dws()
    # class_method = "zhw_dws_red_packet_type_id_msg"
    # running_result('dws', class_method, day_last_1)

    # PUSH召回效果——数据（每日 以4日为单位重跑）
    # lst = [4 * i for i in range(1, 14)]
    # lst.reverse()
    # for i in lst:
    #     start_time = time.time()
    #
    #     day_last_1 = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y-%m-%d')  # t-1
    #
    #     dws = Dws()
    #     class_method = "zhw_dws_push_recall_statistics"
    #     running_result('dws', class_method, day_last_1)
    #
    #     end_time = time.time()
    #     print('执行完成：', day_last_1, '耗时:', end_time - start_time)
