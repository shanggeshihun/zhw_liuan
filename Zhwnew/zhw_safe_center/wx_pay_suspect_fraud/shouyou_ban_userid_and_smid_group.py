# _*_coding:utf-8 _*_

# @Time      : 2023/4/14  17:30
# @Author    : An
# @File      : shouyou_ban_userid_and_smid_group.py
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
end_day = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
start_day = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')  # t-1


def shouyou_ban_userid_and_smid_group(start_day, end_day, group_id):
    '''
    :param start_day:
    :param end_day:
    :param group_id:
    :return:手游导致封号SM用户且近1月封号普通用户-关闭封杀
    '''
    sql = """    	
        -- 近一个月内存在封号的用户
        with tmp_ban as (
            select add_time,
                   '客服添加封号' as data_soure, --数据来源
                   act_id,
                   act_zh,
                   case when lock_start <= add_time then lock_start else add_time end as start_time,
                   lock_end as end_time,
                   lock_sec/60/60/24 as lock_days,
                   gid as game_id,
                   game as game_name,
                   case when gid = 581 then '端游' else '手游' end game_type,
                   case when bz ~ '([6|7|8]{{1}}[0-9]{{8}})' and bz !~ '([0-9]{{10,}})' then substring(bz from '([6|7|8]{{1}}[0-9]{{8}})') end::int8 as order_id
            from ods_zhw.zhw_hao_lock
            where part_day between '{0}' and '{1}'
            and is_related=1 
            and gid in (443,446,683)
        
            union all 
        
            select a.create_time as add_time,
                   '安防查询封号' as data_soure,
                   b.hid,
                   b.game_account,
                   a.start_stmp_time,
                   a.end_stmp_time,
                   a.duration/60/60/24 as lock_days,
                   a.game_id,
                   case when a.game_name = '穿越火线-枪战王者' then '枪战王者' else a.game_name end,
                   case when a.game_id in (11,17,24,581) then '端游' else '手游' end,
                   case when c.order_id ~ '([6|7|8]{{1}}[0-9]{{8}})' and c.order_id !~ '([0-9]{{10,}})' then substring(c.order_id from '([6|7|8]{{1}}[0-9]{{8}})') end::int8 as order_id
            from ods_zhw.game_cheat_account_record a
            left join ods_zhw.game_cheat_account_info b on a.game_account_id=b.id
            left join ods_zhw.game_cheat_account_record_verify c on  a.id = c.record_id
            where type like '%%封号%%'
            and a.game_id in  (443,446,683)
            and to_char(a.create_time,'yyyy-mm-dd') between '{0}' and '{1}'
            and (a.fpt = a.pt or a.pt = -1) and a.game_id = b.game_id 
        )
            
        select
            distinct 
            {2} as fk_target_id,
            1 as fk_module_type,
            t2.id as fk_uid,
            t1.user_id as userid,
            '' as userip,
            b.gameid as gameid,
            '' as did,
            0 as user_mark,
            0 as push_no,
            cast (now() as timestamp)  as push_time,
            0 as is_inc,
            0 as deal_flag 
        from 
        (	-- 近2小时内数美
            select user_id 
            from ods_zhw.zhw_safe_defense_detail
            where true 
            and part_day between to_char(current_date - interval '1 day','yyyy-mm-dd') and to_char(current_date - interval '0 day','yyyy-mm-dd')
            and create_time >=(current_timestamp  - interval '43200 minutes')
            and user_id is not null and user_id <>''
            group by 1 
        ) t1 
        join 
        (
            -- 近一个月内存在封号的用户
            select a.userid,b.order_id,a.gameid,a.ip,
            case when c.yx = 'android' then 'android' when c.yx = 'ios' then 'ios' else 'other' end as os
            from 
            (
                select id,userid,hid,gameid,ip
                from ods_zhw.zhw_dingdan 
                where part_day between '{0}' and '{1}'
            ) a 
            join 
            (
                select order_id
                from tmp_ban 
                where order_id > 0
                group by 1 
            ) b 
            on a.id = b.order_id
            left join ods_zhw.zhw_hao c 
            on a.hid = c.id 
            where true 
        ) b 
        on t1.user_id = b.userid 
        left join
        (
            select id ,jkx_userid from ods_zhw.zhw_user
        ) t2
        on t1.user_id=t2.jkx_userid
        left join
        (
            select userid from public.zhw_shanghu_type_all group by 1
        ) t3
        on t1.user_id=t3.userid
        left join
        (
            select userid from ods_zhw.safe_center_lock_target_activity_user where fk_target_id = {2} group by 1
        ) t4
        on t1.user_id=t4.userid
        left join
        (
            select userid from ods_zhw.zhw_fx_sublet_kf where status=1
        ) t5
        on t1.user_id=t5.userid
        where true 
        and (b.gameid = 443 or (b.gameid in (446,683) and b.os = 'ios') )
        and t3.userid is null
        and t4.userid is null
        and t5.userid is null   	
    """.format(start_day, end_day, group_id)

    print(sql)
    report = pd.read_sql(sql, con=holo_cnx)
    print(report)
    # report.columns = ['fk_target_id', 'fk_module_type', 'fk_uid', 'userid', 'userip', 'gameid', 'did','user_mark', 'push_no', 'push_time', 'is_inc', 'deal_flag']
    # report.to_sql(name='safe_center_lock_target_activity_user', con=cnx, if_exists='append', index=False)

if __name__ == '__main__':
    """
    手游导致封号SM用户且近1月封号普通用户-关闭封杀
    """
    now = (datetime.datetime.now()).strftime('%Y%m%d')  # 今日日期
    end_day = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
    start_day = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')  # t-1

    try:
        shouyou_ban_userid_and_smid_group(start_day, end_day, 51)
    except Exception as e:
        print('shouyou_ban_userid_and_smid_group 异常信息:\n', e)
    pass