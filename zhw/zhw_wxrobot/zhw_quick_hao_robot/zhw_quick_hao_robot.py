# _*_coding:utf-8 _*_
# @Time　　  : 2022/12/26   09:10
# @Author　  : Liuan
# @ File　　  : zhw_quick_hao_robot.py
# @Software   :
import os
import time, datetime, platform, sys
from WorkWeixinRobot.work_weixin_robot import WWXRobot
import numpy as np
import pandas as pd
import configparser
import warnings
# import yagmail
from sqlalchemy import create_engine
from pyhive import presto
import pymysql
import pandas as pd
import numpy as np

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 1000)

plat = platform.system().lower()
if plat == 'windows':
    sys.path.append("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig")
elif plat == 'linux':
    sys.path.append("/work/project/zhw_product/liuan/PublicConfig")
else:
    sys.exit()
from OperatePresto import OperatePresto
from OperateMysql import OperateMysql

import sys

if int(datetime.datetime.now().strftime('%H')) == 6:
    sys.exit()

warnings.filterwarnings("ignore")
# ------------------------数据库配置读取----------------------------
cf = configparser.ConfigParser()
if cf.read("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig/config.ini", encoding='utf-8') == []:
    """服务器模式"""
    cf.read("/home/zhwom/config/config.ini", encoding='utf-8')
    os.chdir("/work/project/zhw_product/liuan/zhw/zhw_wxrobot/zhw_quick_hao_robot")
else:
    """本地模式"""
    cf.read("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig/config.ini", encoding='utf-8')

# ----------------------------------数据库（presto+redis）配置读取---------------------------------------------------
today = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
today_hour = (datetime.datetime.now()).strftime('%Y%m%d%H')  # 今日日期小时
today_last_hour = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y%m%d%H')  # h
today_last_hour_ = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y-%m-%d-%H')  # h

now_hour = datetime.datetime.now().hour
now_last_hour = now_hour - 1

day_last_1 = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')  # t-1
day_last_2 = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')  # t-2
day_last_3 = (datetime.datetime.now() - datetime.timedelta(days=3)).strftime('%Y-%m-%d')  # t-3
day_last_4 = (datetime.datetime.now() - datetime.timedelta(days=4)).strftime('%Y-%m-%d')  # t-4
day_last_5 = (datetime.datetime.now() - datetime.timedelta(days=5)).strftime('%Y-%m-%d')  # t-5
day_last_6 = (datetime.datetime.now() - datetime.timedelta(days=6)).strftime('%Y-%m-%d')  # t-6
day_last_7 = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')  # t-7
day_last_8 = (datetime.datetime.now() - datetime.timedelta(days=8)).strftime('%Y-%m-%d')  # t-8
day_last_15 = (datetime.datetime.now() - datetime.timedelta(days=15)).strftime('%Y-%m-%d')  # t-15
day_last_30 = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')  # t-30
day_last_90 = (datetime.datetime.now() - datetime.timedelta(days=90)).strftime('%Y-%m-%d')  # t-60

####hive数据库
host = cf.get("hive_presto_kudu", "host")
username = cf.get("hive_presto_kudu", "username")
port = cf.get("hive_presto_kudu", "port")
schema = cf.get("hive_presto_kudu", "schema")
catalog = cf.get("hive_presto_kudu", "catalog")
db = presto.connect(host=host, port=port, username=username, schema=schema, catalog=catalog)

# Presto 连接配置
####hive数据库
presto_kudu_host = cf.get("hive_presto_kudu", "host")
presto_kudu_username = cf.get("hive_presto_kudu", "username")
presto_kudu_port = cf.get("hive_presto_kudu", "port")
presto_kudu_schema = cf.get("hive_presto_kudu", "schema")
presto_kudu_catalog = cf.get("hive_presto_kudu", "catalog")

# 实例化presto
operate_presto = OperatePresto(
    username=presto_kudu_username,
    host_ip=presto_kudu_host,
    port=int(presto_kudu_port),
    catalog=presto_kudu_catalog,
    schema=presto_kudu_schema
)

# 运营数据库
host = cf.get("Mysql-data_yunying", "host")
user = cf.get("Mysql-data_yunying", "user")
password = cf.get("Mysql-data_yunying", "password")
DB = cf.get("Mysql-data_yunying", "DB")
port = cf.get("Mysql-data_yunying", "port")
cnx_mysql = create_engine("mysql+pymysql://" + user + ":" + password + "@" + host + ":" + port + "/" + DB, echo=False)
# 实例化mysql
operate_mysql = OperateMysql(
    username=user,
    password=password,
    host_ip=host,
    port=int(port),
    database=DB
)


def float_to_unit_w(float_value):
    if float_value > 10000:
        return str(round(float_value / 10000, 2)) + "万"
    else:
        return str(round(float_value, 0))


def float_to_percent_sign(float_value):
    return str(round(float_value * 100, 2)) + "%"


def float_to_percent_nosign(float_value):
    return [str(round(float_value * 100, 2)) if float_value > 0 else str(round(float_value * 100 * (-1), 2))][0] + "%"


def float_to_float2(float_value):
    return [str(round(float_value, 2)) if float_value > 0 else str(round(float_value * (-1), 2))][0]


def float_to_percent_point(float_value):
    return [str(round(float_value * 100, 2)) if float_value > 0 else str(round(float_value * 100 * (-1), 2))][
               0] + "个百分点"


def value_mapping_fontcolor(values):
    """
    :param values: 数值
    :return: 大于0 绿色，小于0 橙色，等于0 灰色
    """
    if values > 0:
        return "info"
    elif values < 0:
        return "warning"
    else:
        return "comment"


def value_mapping_updown(values):
    """
    :param values: 数值
    :return: 大于0 上升，小于0 下降，等于0 持平
    """
    if isinstance(values,int) or isinstance(values,float):
        if values > 0:
            return "上升"
        elif values < 0:
            return "下降"
        else:
            return "持平"
    else:
        return ""


def get_rule_result(sql_file_name, start_day, end_day):
    """
    :param cur_ts_str: 时间戳（字符型)
    :return:返回DataFrame结果
    """

    file = open(r"./{}.sql".format(sql_file_name), mode='r', encoding='utf-8')
    presto_sql = file.read()
    sql = presto_sql.format(start_day, end_day)
    result = operate_presto.query_data(sql)

    if not result:
        result_df = pd.DataFrame()
    else:
        result_df = pd.DataFrame(result)
        result_index = operate_presto.query_data_index()
        result_df.columns = result_index

    operate_presto.close_conn()
    return result_df


from WorkWeixinRobot.work_weixin_robot import WWXRobot
import json
import requests


def work_wxrobot(content, webhook):
    """
    :param content:
    :param webhook:
    :return:
    """
    headers = {"Content-Type": "application/json"}
    form_data = {
        "msgtype": "markdown",
        "markdown": {
            "content": content,
            "mentioned_list": ["liuan@jld1141.wecom.work"]
        }
    }

    form_data_json = json.dumps(form_data)
    work_wxrobot_result = requests.post(url=webhook, data=form_data_json, headers=headers, verify=False)
    return work_wxrobot_result


if __name__ == '__main__':

    today = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
    print('程序执行日期：', datetime.datetime.now())

    # 当日(datetime)
    today_dt = datetime.datetime.now()
    # 昨日(datetime)
    last_day_dt = datetime.datetime.now() - datetime.timedelta(days=1)

    # 今日，今日的days，今日星期
    today_day = today_dt.strftime('%Y-%m-%d')
    today_days = int(today_dt.strftime('%d'))
    today_weekday = datetime.datetime.now().isoweekday()

    # 昨日，昨日所在月的首日，昨日所有周的首日
    last_day = last_day_dt.strftime('%Y-%m-%d')
    last_day_monthfirstday = (datetime.date(year=last_day_dt.year, month=last_day_dt.month, day=1)).strftime('%Y-%m-%d')
    last_day_month = last_day_dt.strftime('%Y-%m')

    last_day_weekfirstday = (last_day_dt - datetime.timedelta(days=6)).strftime('%Y-%m-%d')
    last_week = (last_day_dt - datetime.timedelta(days=6)).strftime('%m%d') + '-' + last_day_dt.strftime('%m%d')

    # 昨日的days
    last_day_days = int(last_day_dt.strftime('%d'))

    # 昨日星期几
    lastday_weekday = last_day_dt.isoweekday()

    if today_days == 1:
        report_dim = '月'
        start_day, end_day = last_day_monthfirstday, last_day
        report_dim_daytype = last_day_month

        end_day_h_dt = (datetime.datetime.strptime(last_day_monthfirstday,'%Y-%m-%d'))-datetime.timedelta(days=1)
        end_day_h = end_day_h_dt.strftime('%Y-%m-%d')
        start_day_h = (datetime.date(year=end_day_h_dt.year, month=end_day_h_dt.month, day=1)).strftime('%Y-%m-%d')

    elif today_weekday == 1:
        report_dim = '周'
        start_day, end_day = last_day_weekfirstday, last_day
        report_dim_daytype = last_week

        end_day_h = ((datetime.datetime.strptime(last_day,'%Y-%m-%d'))-datetime.timedelta(days=7)).strftime('%Y-%m-%d')
        start_day_h = ((datetime.datetime.strptime(last_day,'%Y-%m-%d'))-datetime.timedelta(days=13)).strftime('%Y-%m-%d')

    else:
        report_dim = '日'
        start_day, end_day = last_day, last_day
        report_dim_daytype = last_day

        end_day_h = ((datetime.datetime.strptime(last_day,'%Y-%m-%d'))-datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        start_day_h = end_day_h

    # 测试
    test_webhook = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=d38dfede-dfc8-4bb1-b8d6-bcf888bf945e"
    # 正式
    webhook = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=67eac70a-f9ea-46ef-8c77-624d36f8734f"


    # TOP游戏撤单率，上号成功率
    st = time.time()
    game_withdraw_quick_hao_result_d = get_rule_result('game_withdraw_quick_hao_sql', start_day, end_day)
    game_withdraw_quick_hao_result_h = get_rule_result('game_withdraw_quick_hao_sql', start_day_h, end_day_h)
    if not len(game_withdraw_quick_hao_result_h):
        game_withdraw_quick_hao_result_h = pd.DataFrame({
            'gameid': 99999999, 'game_name': '99999999', 'orders': 0, 'withdraw_orders': 0, 'succ_orders': 0,
            'quick_orders': 0
        })
    game_withdraw_quick_hao_result = pd.merge(game_withdraw_quick_hao_result_d, game_withdraw_quick_hao_result_h,
                                              how = 'left', left_on = ['gameid'], right_on= ['gameid'],
                                              suffixes= ['','_h']
    )

    game_withdraw_quick_hao_result.sort_values(by='orders', ascending=False, inplace=True)
    game_withdraw_quick_hao_result.reset_index(inplace=True, drop=True)
    et = time.time()
    print('\t game_withdraw_quick_hao_result 执行完成耗时:{0} s'.format(et - st))

    withdraw_content = '## 一、TOP游戏 撤单率&上号成功率 【{0} {1}】'.format(report_dim, report_dim_daytype)
    pattern = "\t{0:<10}\t{1:<6}\t{2:<8}\t{3:8}\t{4:8}"
    withdraw_info = '**' + pattern.format('[游戏名称]', '[撤单率]','[撤单率环比]', '[上号成功率]', '[上号成功率环比]') + '**' + '\n'
    for idx, item in game_withdraw_quick_hao_result.iterrows():
        game_name = item['game_name']
        orders, orders_h = item['orders'], 0 if np.isnan(item['orders_h']) else item['orders_h']
        withdraw_orders, withdraw_orders_h = item['withdraw_orders'], 0 if np.isnan(item['withdraw_orders_h']) else item['withdraw_orders_h']
        withdraw_ratio = withdraw_orders / orders

        succ_orders, succ_orders_h = item['succ_orders'], 0 if np.isnan(item['succ_orders_h']) else item['succ_orders_h']
        quick_orders, quick_orders_h = item['quick_orders'], 0 if np.isnan(item['quick_orders_h']) else item['quick_orders_h']

        withdraw_ratio_tmp = float_to_percent_sign(withdraw_ratio)
        withdraw_ratio_h_tmp = '' if orders_h == 0 else float_to_percent_sign(withdraw_orders_h / orders_h)
        if orders ==0:
            withdraw_ratio_c_h_tmp = ''
        elif orders >0 and orders_h == 0:
            withdraw_ratio_c_h_tmp = float_to_percent_sign(1)
        else:
            withdraw_ratio_c_h_tmp = float_to_percent_sign((withdraw_orders / orders) - (withdraw_orders_h / orders_h))

        quick_ratio_tmp = '' if quick_orders == 0 else float_to_percent_sign(succ_orders / quick_orders)
        quick_ratio_h_tmp = '' if quick_orders_h == 0 else float_to_percent_sign(succ_orders_h / quick_orders_h)
        if quick_orders == 0:
            quick_ratio_c_h_tmp = ''
        elif quick_orders >0 and quick_orders_h == 0:
            quick_ratio_c_h_tmp = float_to_percent_sign(1)
        else:
            quick_ratio_c_h_tmp = float_to_percent_sign((succ_orders / quick_orders) - (succ_orders_h / quick_orders_h))

        withdraw_info = withdraw_info + pattern.format(game_name, withdraw_ratio_tmp, withdraw_ratio_c_h_tmp, quick_ratio_tmp, quick_ratio_c_h_tmp) + '\n'

    withdraw_content = withdraw_content + '\n' + withdraw_info
    work_wxrobot(withdraw_content, webhook)


    # TOP游戏 每款游戏的撤单率及上号成功率
    st = time.time()
    game_quick_hao_result_d = get_rule_result('game_channel_serve_quick_hao_sql', start_day, end_day)
    game_quick_hao_result_h = get_rule_result('game_channel_serve_quick_hao_sql', start_day_h, end_day_h)
    if not len(game_quick_hao_result_h):
        game_quick_hao_result_h = pd.DataFrame({
            'gameid': 99999999, 'game_name': '99999999',
            'serve': '99999999', 'channel': '99999999',
            'orders': 0, 'withdraw_orders': 0, 'succ_orders': 0,'quick_orders': 0
        })
    game_quick_hao_result = pd.merge(
        game_quick_hao_result_d, game_quick_hao_result_h,
        how = 'left', left_on = ['gameid','serve', 'channel'],
        right_on= ['gameid','serve', 'channel'],suffixes= ['','_h']
    )
    game_quick_hao_result.sort_values(by=['gameid', 'orders'], ascending=[False, False], inplace=True)
    game_quick_hao_result.reset_index(inplace=True, drop=True)
    et = time.time()
    print('\t game_channel_serve_quick_hao_sql 执行完成耗时:{0} s'.format(et - st))

    game_list = [443, 11, 683, 560, 446, 17, 581, 699, 1088, 698]
    for gameid in game_list:
        the_game_quick_hao_result = game_quick_hao_result[game_quick_hao_result.gameid == gameid]
        if len(the_game_quick_hao_result) == 0:
            continue
        the_game_quick_hao_result.sort_values(by=['channel'], ascending=[False], inplace=True)
        the_game_quick_hao_result.reset_index(inplace=True, drop=True)

        pattern = "{0:<6}\t{1:<4}\t{2:<5}\t{3:<6}\t{4:8}\t{5:8}"
        quick_info = '**' + pattern.format('[渠道]', '[大区]', '[撤单率]', '[撤单率环比]', '[上号成功率]', '[上号成功率环比]') + '**' + '\n'
        for idx, item in the_game_quick_hao_result.iterrows():
            game_name = item['game_name']
            channel = item['channel']
            serve = item['serve']
            orders, orders_h = item['orders'], item['orders_h']
            withdraw_orders, withdraw_orders_h = item['withdraw_orders'], item['withdraw_orders_h']
            quick_orders, quick_orders_h = item['quick_orders'], item['quick_orders_h']
            succ_orders, succ_orders_h = item['succ_orders'], item['succ_orders_h']

            withdraw_ratio = withdraw_orders / orders
            withdraw_ratio_tmp = float_to_percent_sign(withdraw_ratio)
            withdraw_ratio_h_tmp = '' if orders_h == 0 else float_to_percent_sign(withdraw_orders_h / orders_h)

            if orders == 0:
                withdraw_ratio_c_h_tmp = ''
            elif orders > 0 and orders_h == 0:
                withdraw_ratio_c_h_tmp = float_to_percent_sign(1)
            else:
                withdraw_ratio_c_h_tmp = float_to_percent_sign(
                    (withdraw_orders / orders) - (withdraw_orders_h / orders_h))

            quick_ratio_tmp = '' if quick_orders == 0 else float_to_percent_sign(succ_orders / quick_orders)
            quick_ratio_h_tmp = '' if quick_orders_h == 0 else float_to_percent_sign(succ_orders_h / quick_orders_h)
            if quick_orders == 0:
                quick_ratio_c_h_tmp = ''
            elif quick_orders > 0 and quick_orders_h == 0:
                quick_ratio_c_h_tmp = float_to_percent_sign(1)
            else:
                quick_ratio_c_h_tmp = float_to_percent_sign(
                    (succ_orders / quick_orders) - (succ_orders_h / quick_orders_h))

            quick_info = quick_info + pattern.format(
                channel, serve, withdraw_ratio_tmp, withdraw_ratio_c_h_tmp,
                quick_ratio_tmp, quick_ratio_c_h_tmp
            ) + '\n'

        quick_content = '## 二、TOP游戏-{2} 上号情况 【{0} {1}】'.format(report_dim, report_dim_daytype,game_name) + '\n' + quick_info
        work_wxrobot(quick_content, webhook)



