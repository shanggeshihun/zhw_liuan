# _*_coding:utf-8 _*_

# @Time      : 2023/7/6  15:34
# @Author    : An
# @File      : gg_fenghao_orders_group.py
# @Software  : GG租号三方导致封号的租号玩订单推送

import time, datetime, configparser, warnings, math, platform , psycopg2
import sys
import pandas as pd
import pymysql, presto
import pyhive.presto as pypresto
from WorkWeixinRobot.work_weixin_robot import WWXRobot
from sqlalchemy import create_engine
import json, requests

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 1000)

plat = platform.system().lower()
if plat == 'windows':
    sys.path.append("E:/工作文件\在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig")
elif plat == 'linux':
    sys.path.append("/work/project/zhw_product/liuan/PublicConfig")
else:
    sys.exit()

from OperateMysql import OperateMysql
from OperatePresto import OperatePresto
from SchedualToMysql import SchedualInfo
from OperateHologresNew import OperateHologresNew

from QqexmailSmtpAttach import QqExmailSmtp

warnings.filterwarnings("ignore")
# ------------------------数据库配置读取----------------------------
cf = configparser.ConfigParser()
if cf.read("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig/config.ini", encoding='utf-8') == []:
    """服务器模式"""
    cf.read("/home/zhwom/config/config.ini", encoding='utf-8')
else:
    """本地模式"""
    cf.read("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig/config.ini", encoding='utf-8')

# hologres数据库
holo_host = cf.get("hologres-dofun", "host")
holo_port = cf.get("hologres-dofun", "port")
holo_database = cf.get("hologres-dofun", "DB")
holo_user = cf.get("hologres-dofun", "user")
holo_password = cf.get("hologres-dofun", "password")

holo_cnx = create_engine("postgresql+psycopg2://"+holo_user+":"+holo_password+"@"+holo_host+":"+holo_port+"/" + holo_database )

def gg_fenghao_orders_group(start_day, end_day, group_id):
    '''
    :param day_last_1:
    :param now_day:
    :param group_id:
    :return:
    '''
    sql = """    	
        select 
            distinct 
            {2} as fk_target_id,
            1 as fk_module_type,
            e.id as fk_uid,
            c.userid,
            c.ip as userip,
            c.gameid as gameid,
            c.id as did,
            0 as user_mark,
            0 as push_no,
            cast (now() as timestamp)  as push_time,
            0 as is_inc,
            0 as deal_flag 
        from ods_zhw.zhw_thrd_report_order a 
        join ods_zhw.zhw_hao_lock_details b 
        on a.oid = b.order_id and b.order_id>0
        join ods_zhw.zhw_dingdan c  
        on a.oid = c.id and c.userid = '18528028441'
        and c.part_day between to_char(date('{0}') - 3,'yyyy-mm-dd') and '{1}' 
        left join ods_zhw.safe_center_lock_target_activity_user_info d 
        on c.id = d.did and d.fk_target_id = {2}
        left join ods_zhw.zhw_user e  
        on c.userid = e.jkx_userid
        where a.part_day between to_char(date('{0}') - 3,'yyyy-mm-dd') and '{1}'
        and b.start_time between cast('{0} 00:00:00' as timestamp) and cast('{1} 23:59:59' as timestamp) 
        and d.did is null
    """.format(start_day, end_day, group_id)

    report = pd.read_sql(sql, con=holo_cnx)

    print(report)

    report.columns = ['fk_target_id', 'fk_module_type', 'fk_uid', 'userid', 'userip', 'gameid', 'did',
                      'user_mark', 'push_no', 'push_time', 'is_inc', 'deal_flag']

    report.to_sql(name='safe_center_lock_target_activity_user_info', con=cnx, if_exists='append', index=False)

if __name__ == '__main__':

    """2023-07-06，
    group_name GG租号三方导致封号的租号玩订单推送 54
    """
    now = (datetime.datetime.now()).strftime('%Y%m%d')  # 今日日期
    now_day = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
    day_last_1 = (datetime.datetime.now() - datetime.timedelta(days=3)).strftime('%Y-%m-%d')  # t-1

    try:
        gg_fenghao_orders_group(day_last_1, now_day, 54)
    except Exception as e:
        print('gg_fenghao_orders_group 异常信息:\n', e)
    pass