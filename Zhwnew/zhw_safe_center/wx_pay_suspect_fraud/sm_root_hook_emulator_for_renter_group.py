# _*_coding:utf-8 _*_

# @Time      : 2023/3/24  17:30
# @Author    : An
# @File      : sm_root_hook_emulator_for_renter_group.py
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


# ------------------------参数配置----------------------------
now = (datetime.datetime.now()).strftime('%Y%m%d')  # 今日日期
now_day = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
day_last_1 = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')  # t-1


def sm_root_hook_emulator_for_renter_group(day_last_1, now_day, group_id):
    '''
    :param day_last_1:
    :param now_day:
    :param group_id:
    :return:针对数美root设备|是否hook设备|PC模拟器命中用户且普通租客订单 封禁上号OAID
    '''
    sql = """    	
        with sm_userid as (
                select a.user_id
                from 
                (
                    select user_id,operation,trigger_type,create_time,part_day
                    from ods_zhw.zhw_safe_defense_detail 
                    where true 
                    and is_success = 'REJECT'
                    and part_day between to_char(date('{0}') - interval '1 days','yyyy-mm-dd') and '{1}'
                    and create_time >= (current_timestamp - interval '25 minutes')
                    and trigger_type ~ '(root设备|是否hook设备|PC模拟器)'--这是数美返回的用户标签
                    and operation = '启动游戏'
                    and user_id <>'' and user_id is not null 
                ) a 
                left join ods_zhw.zhw_shanghu_type_log b 
                on a.user_id = b.userid and b.sh_type>0
                where b.userid is null
                group by 1 
        )
        select                         
            {2} as fk_target_id,
            1 as fk_module_type,
            t2.id as fk_uid,
            t1.userid,
            t1.ip as userip,
            t1.gameid as gameid,
            t1.id as did,
            0 as user_mark,
            0 as push_no,
            cast (now() as timestamp)  as push_time,
            0 as is_inc,
            0 as deal_flag 
        from 
        (
            select userid,id,ip,add_from,gameid,row_number()over(partition by userid order by id desc) as rn 
            from ods_zhw.zhw_dingdan 
            where true 
            and part_day between to_char(date('{0}') - interval '1 days','yyyy-mm-dd') and '{1}'
            and add_time >= (current_timestamp - interval '25 minutes')
            and gameid in (443,446,683)
            and userid in (
                    select user_id from sm_userid
            )
            and id not in (
            select did from ods_zhw.zhw_app_oaid  
            where part_day between to_char(date('{0}') - interval '1 days','yyyy-mm-dd') and '{1}'
            and oaid_sh in ('0000000000000000|', '0000000000000000|null', '00000000-0000-0000-0000-000000000000',
             '00000000000000000000000000000000', '00000000000000000000000000000000',  '0000000000000000|'
            )
            )
        ) t1 
        left join 
        (
            select cast(item_value as int) as key,item_name 
            from ods_zhw.zhw_dict_item 
            where dict_id = 51
        ) e
        on t1.add_from = e.key
        left join
        (
            select id ,jkx_userid from ods_zhw.zhw_user
        ) t2
        on t1.userid=t2.jkx_userid
        left join
        (
            select userid from public.zhw_shanghu_type_all group by 1
        ) t3
        on t1.userid=t3.userid
        left join
        (
            select did from ods_zhw.safe_center_lock_target_activity_user where fk_target_id = {2} group by 1
        ) t4
        on t1.id=t4.did
        left join
        (
            select userid from ods_zhw.zhw_fx_sublet_kf where status=1
        ) t5
        on t1.userid=t5.userid
        where t3.userid is null
        and e.item_name not in ('分销合伙人','新转租平台','分销普通版','分销尊享版','分销高级版')
        and t4.did is null
        and t5.userid is null 
    """.format(day_last_1, now_day, group_id)

    report = pd.read_sql(sql, con=holo_cnx)

    print(report)
    report.columns = ['fk_target_id', 'fk_module_type', 'fk_uid', 'userid', 'userip', 'gameid', 'did',
                      'user_mark', 'push_no', 'push_time', 'is_inc', 'deal_flag']
    # report.to_sql(name='safe_center_lock_target_activity_user', con=cnx, if_exists='append', index=False)

if __name__ == '__main__':

    """2023-04-07，针对数美root设备|是否hook设备|PC模拟器命中用户且普通租客订单 封禁上号OAID
    group_name 手游查询封号用户群(关闭数美风险规则用户上号oaid)
    """
    try:
        sm_root_hook_emulator_for_renter_group(now_day, now_day, 48)
    except Exception as e:
        print('sm_root_hook_emulator_for_renter_group 异常信息:\n', e)
    pass