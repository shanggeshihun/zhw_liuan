# _*_coding:utf-8 _*_

# @Time      : 2023/7/21  15:34
# @Author    : An
# @File      : shouyou_fenghao_disable_appversion_group.py
# @Software  : 安卓停用版本上号风险用户

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

def shouyou_fenghao_disable_appversion_group(start_day, end_day, group_id):
    '''
    :param day_last_1:
    :param now_day:
    :param group_id:
    :return:
    '''
    sql = """    	
        -- 2023/7/10 11:12 对安卓部分历史(高封号低单量)版本停用
        -- 安卓主版本400100000 版本'6291', '6150', '6300', '6250', '6180', '6310', '6120', '6260', '6270', '6190','6140'
        select 
            {2} as fk_target_id,
            1 as fk_module_type,
            f.id as fk_uid,
            c.userid,
            c.ip as userip,
            c.gameid,
            a.order_id as did,
            0 as user_mark,
            0 as push_no,
            cast (now() as timestamp)  as push_time,
            0 as is_inc,
            0 as deal_flag 
        from ods_zhw.zhw_quick_zhw_quick_queue a 
        left join ods_zhw.zhw_dict_item b 
        on a.attr_ext::json->>'app_id' = b.item_value and b.dict_id = 124
        join ods_zhw.zhw_dingdan c 
        on a.order_id = c.id and c.part_day between '{0}' and '{1}'
        left join 
        (
            select userid
            from ads.zhw_shanghu_type_all
            where true 
            and save_date = to_char(current_date - 1,'yyyy-mm-dd')
            group by 1 
        ) d 
        on c.userid = d.userid 
        left join ods_zhw.zhw_fx_sublet_kf e 
        on c.userid = e.userid and e.status = 1
        left join ods_zhw.zhw_user f 
        on c.userid = f.jkx_userid 
        left join ods_zhw.safe_center_lock_target_activity_user h 
        on a.order_id = h.did and h.fk_target_id = {2}
        where a.part_day between '{0}' and '{1}'
        and a.attr_ext::json->>'app_version' in ('6291', '6150', '6300', '6250', '6180', '6310', '6120', '6260', '6270', '6190','6140')
        and a.attr_ext::json->>'app_id_real' in  ('400100000') -- 安卓-普通版
        and to_char(a.create_time,'yyyy-mm-dd hh24:mi:ss')>'2023-07-10 11:12:00'
        and a.type in (3,4)
        and d.userid is null and e.userid is null -- 普通用户
        and h.did is null
    """.format(start_day, end_day, group_id)

    report = pd.read_sql(sql, con=holo_cnx)

    if len(report)>50:
        return

    print(report)

    # report.columns = ['fk_target_id', 'fk_module_type', 'fk_uid', 'userid', 'userip', 'gameid', 'did',
    #                   'user_mark', 'push_no', 'push_time', 'is_inc', 'deal_flag']
    #
    # report.to_sql(name='safe_center_lock_target_activity_user_info', con=cnx, if_exists='append', index=False)

if __name__ == '__main__':

    """2023-07-21，
    group_name 安卓停用版本上号风险用户(普通用户) 64
    7/10 11:12 已对部分安卓版本停用，但该版本后续仍有上号订单，该部分订单无数美记录。高度疑似篡改设备信息。
    """
    now = (datetime.datetime.now()).strftime('%Y%m%d')  # 今日日期
    end_day = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
    start_day = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')  # t-1
    group_id = 64

    try:
        shouyou_fenghao_disable_appversion_group(start_day, end_day, group_id)
    except Exception as e:
        print('shouyou_fenghao_disable_appversion_group 异常信息:\n', e)
    pass