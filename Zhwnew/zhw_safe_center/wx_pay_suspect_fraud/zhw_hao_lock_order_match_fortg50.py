# _*_coding:utf-8 _*_

# @Time      : 2023/4/14  14:40
# @Author    : An
# @File      : zhw_hao_lock_order_match_fortg99999999.py
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


def zhw_hao_lock_order_match(day_last_1, now_day, group_id):
    sql = """    
        with tmp_hao_lock_details as (
            select (case when hao_ext::json->>'zh' is not null then hao_ext::json->>'zh' else hao_ext::json->>'act_zh' end) as act_zh,start_time,gid as game_id,event_time as add_time
            from dw.t_dwd_user_event
            where event_name ='account_banned'
            and gid in (443,446,683)
            and invertal_seconds>7*24*3600
            and part_day>= to_char(date('{0}') - interval '30 days', 'yyyy-mm-dd')
            -- 按照查询添加时间对封号进行策略处理
            and part_day = '{0}'
        ),
        zc_tmp as (
            select a.act_zh,a.start_time,a.game_id,a.add_time,
            c.id as m_order,c.hid as m_hid,c.ip as m_ip,c.gameid as m_gameid,c.userid as m_userid,c.huserid as m_huserid,c.stimer as m_stimer,c.etimer as m_etimer,
            c.item_name as addfrom_name,c.zh as m_zh,
            c.os as m_os,
            g.title as game_name_m,
            1 as pn
            from tmp_hao_lock_details a 
            join 
            (
                select c.id,c.hid,c.ip,c.gameid,case when f.order_id is null then c.userid else f.username end as userid,c.huserid,c.stimer,c.etimer,c.add_from,d.zh,d.os,
                e.item_name
                from 
                (
                    select id,hid,ip,gameid,userid,huserid,stimer,etimer,part_day,add_from
                    from ods_zhw.zhw_dingdan 
                    where true 
                    and gameid in (443,446,683)
                    and part_day between to_char(date('{0}') - interval '10 days','yyyy-mm-dd') 
                    and '{1}'
                    and zt = 2 
                ) c
                join 
                (
                    select cast(item_value as int) as key,item_name 
                    from ods_zhw.zhw_dict_item 
                    where dict_id = 51
                ) e
                on c.add_from = e.key
                join
                (
                    select id,save_date,zh,case when yx = 'android' then 'android' when yx = 'ios' then 'ios' else 'other' end as os
                    from ods_zhw.zhw_hao_archive_day
                    where true 
                    and save_date between to_char(date('{0}') - interval '10 days','yyyy-mm-dd') 
                    and '{1}'
                    and gid in (443,446,683)
                    
                    union all
                    
                    select id,to_char(current_date,'yyyy-mm-dd') as save_date,zh,case when yx = 'android' then 'android' when yx = 'ios' then 'ios' else 'other' end as os
                    from ods_zhw.zhw_hao
                    where true
                    and gid in (443,446,683)
                ) d
                on c.hid = d.id and c.part_day = d.save_date
                left join 
                (
                    select order_id,username
                    from ods_zhw.zhw_fx_order 
                    where true 
                    and part_day between to_char(date('{0}') - interval '10 days','yyyy-mm-dd') 
                    and '{1}'
                ) f 
                on c.id = f.order_id
            ) c
            on c.zh = a.act_zh and a.start_time between c.stimer and c.etimer
            left join ods_zhw.zhw_game_info g 
            on a.game_id = g.id 
            where true 
            -- and to_char(a.start_time,'yyyy-mm-dd') between '{0}' and '{1}' -- 其实封号时间
        ) ,
        cd_tmp as (
            select a.act_zh,a.start_time,a.game_id,a.add_time,
            c.id as m_order,c.hid as m_hid,c.ip as m_ip,c.gameid as m_gameid,c.userid as m_userid,c.huserid as m_huserid,c.stimer as m_stimer,c.etimer as m_etimer,
            c.item_name as addfrom_name,c.zh as m_zh,
            c.os as m_os,
            g.title as game_name_m,
            2 as pn
            from tmp_hao_lock_details a 
            join 
            (
                select c.id,c.hid,c.ip,c.gameid,case when f.order_id is null then c.userid else f.username end as userid,c.huserid,c.stimer,c.etimer,c.add_from,d.zh,d.os,
                b.t,e.item_name
                from 
                (
                    select id,hid,ip,gameid,userid,huserid,stimer,etimer,part_day,add_from
                    from ods_zhw.zhw_dingdan 
                    where true 
                    and gameid in (443,446,683)
                    and part_day between to_char(date('{0}') - interval '10 days','yyyy-mm-dd') 
                    and '{1}'
                    and zt = 3
                ) c
                join
                (
                    select cast(item_value as int) as key,item_name 
                    from ods_zhw.zhw_dict_item 
                    where dict_id = 51
                ) e
                on c.add_from = e.key
                join 
                (
        
                    select did,t
                    from ods_zhw.zhw_ts
                    where true 
                    and part_day between to_char(date('{0}') - interval '10 days','yyyy-mm-dd') 
                    and '{1}'
                    and gameid in (443,446,683)
                ) b
                on c.id = b.did
                join
                (
                    select id,save_date,zh,case when yx = 'android' then 'android' when yx = 'ios' then 'ios' else 'other' end as os
                    from ods_zhw.zhw_hao_archive_day
                    where true 
                    and save_date between to_char(date('{0}') - interval '10 days','yyyy-mm-dd') 
                    and '{1}'
                    and gid in (443,446,683)
                    
                    union all
                    
                    select id,to_char(current_date,'yyyy-mm-dd') as save_date,zh,case when yx = 'android' then 'android' when yx = 'ios' then 'ios' else 'other' end as os
                    from ods_zhw.zhw_hao
                    where true
                    and gid in (443,446,683)
                ) d
                on c.hid = d.id and c.part_day = d.save_date
                left join 
                (
                    select order_id,username
                    from ods_zhw.zhw_fx_order 
                    where true 
                    and part_day between to_char(date('{0}') - interval '10 days','yyyy-mm-dd') 
                    and '{1}'
                ) f 
                on c.id = f.order_id
            ) c
            on c.zh = a.act_zh and a.start_time between c.stimer and c.t
            left join ods_zhw.zhw_game_info g 
            on a.game_id = g.id 
            where true 
            and true 
            -- and to_char(a.start_time,'yyyy-mm-dd') between '{0}' and '{1}'
        )
        
        
        select 
            act_zh,start_time,game_id,add_time,
            m_order,m_hid,m_ip,m_gameid,m_userid,m_huserid,m_stimer,m_etimer,
            addfrom_name,m_zh,m_os,game_name_m,
            '' as info,'' as remark,
            {2} as target_fk_id,
            cast (now() as timestamp)  as push_time
        from 
        (
            select *,row_number()over(partition by add_time,act_zh order by start_time asc,pn asc ) as rn 
            from 
            (
                select *
                from zc_tmp
                union all 
                select *
                from cd_tmp
            ) aa 
        ) tt
        where rn = 1 
    """.format(day_last_1, now_day, group_id)

    report = pd.read_sql(sql, con=holo_cnx)

    print(report)

    report.columns = [
        'act_zh', 'start_time', 'game_id', 'add_time', 'm_order', 'm_hid', 'm_ip', 'm_gameid', 'm_userid', 'm_huserid',
        'm_stimer', 'm_etimer', 'addfrom_name', 'm_zh', 'm_os', 'game_name_m', 'info', 'remark', 'target_fk_id', 'push_time'
    ]
    # report.to_sql(name='zhw_hao_lock_order_match', con=cnx, if_exists='append', index=False)


if __name__ == '__main__':
    now_day = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
    day_last_1 = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')  # t-1
    zhw_hao_lock_order_match(now_day,now_day,50)

