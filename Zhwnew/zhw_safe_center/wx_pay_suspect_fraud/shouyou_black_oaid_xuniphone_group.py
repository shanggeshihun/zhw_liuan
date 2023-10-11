# _*_coding:utf-8 _*_

# @Time      : 2023/4/21  9:40
# @Author    : An
# @File      : shouyou_black_oaid_xuniphone_group.py
# @Software  : PyCharm

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

def shouyou_black_oaid_xuniphone_group(start_day, end_day, group_id):
    sql = """
        select
            {2} as fk_target_id,
            1 as fk_module_type,
            t2.id as fk_uid,
            t1.userid,
            ' ' as userip,
            11 as gameid,
            0 as did,
            0 as user_mark,
            0 as push_no,
            cast (now() as timestamp)  as push_time,
            0 as is_inc,
            0 as deal_flag 
            from
        (
            select userid
            from ods_zhw.zhw_oaid_deal_log a
            left join  ods_zhw.zhw_app_oaid b on a.oaid=b.oaid_sh
            left join ods_zhw.zhw_user c on b.userid=c.jkx_userid
            where to_char(add_time,'yyyy-mm-dd')  between  '{0}' and '{1}'
            and (jkx_userphone like '1700%%' or jkx_userphone like '1701%%' or jkx_userphone like '1702%%' or jkx_userphone like '162%%' or jkx_userphone like '1703%%' or jkx_userphone like '1705%%' or jkx_userphone like '1706%%' or jkx_userphone like '165%%' or jkx_userphone like '1704%%' or jkx_userphone like '1707%%' or jkx_userphone like '1708%%' or jkx_userphone like '1709%%' or jkx_userphone like '171%%' or jkx_userphone like '167%%' or jkx_userphone like '1349%%' or jkx_userphone like '174%%' or jkx_userphone like '140%%' or jkx_userphone like '141%%' or jkx_userphone like '144%%' or jkx_userphone like '146%%' or jkx_userphone like '148%%')
            group by 1
        ) t1 
        left join
        (
            select id ,jkx_userid from ods_zhw.zhw_user
        ) t2
        on t1.userid=t2.jkx_userid
        left join
        (
            select userid from public.zhw_shanghu_type_all where save_date = to_char(current_date - interval '1 day','yyyy-mm-dd')
        ) t3
        on t1.userid=t3.userid
        left join
        (
            select * from ods_zhw.safe_center_lock_target_activity_user where fk_target_id = {2}
        ) t4
        on t1.userid=t4.userid
        left join
        (
            select  userid from ods_zhw.zhw_fx_sublet_kf where status=1
        ) t5
        on t1.userid=t5.userid
        where t3.userid is null
        and t4.userid is null
        and t5.userid is null
    """.format(start_day, end_day, group_id)

    report = pd.read_sql(sql, con=holo_cnx)
    print('结果数量:',len(report))
    print('返回结果如下:\n',report)
    # report.columns = ['fk_target_id', 'fk_module_type', 'fk_uid', 'userid', 'userip', 'gameid', 'did','user_mark', 'push_no', 'push_time', 'is_inc', 'deal_flag']
    # report.to_sql(name='safe_center_lock_target_activity_user', con=cnx, if_exists='append', index=False)
    


if __name__ == '__main__':
    """
    手游黑oaid虚拟号段用户（封禁永久） 30
    """
    end_day = (datetime.datetime.now()).strftime('%Y-%m-%d')
    start_day = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

    """2022-08-09，手游黑oaid虚拟号段用户（封禁永久） 30"""
    try:
        shouyou_black_oaid_xuniphone_group(start_day, end_day, 30)
    except Exception as e:
        print(e)
        pass