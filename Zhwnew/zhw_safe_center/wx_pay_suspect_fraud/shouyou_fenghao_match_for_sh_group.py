# _*_coding:utf-8 _*_

# @Time      : 2023/3/24  17:30
# @Author    : An
# @File      : shouyou_fenghao_match_for_sh_group.py
# @Software  : PyCharm


"""
20230516 订单匹配逻辑依赖的数据源仅从新封号查询获取
"""


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


def shouyou_fenghao_match_for_sh_group(start_day, end_day, group_id):
    '''
    :param start_day:
    :param end_day:
    :param group_id:
    :return:针对手游查询到的封号匹配出对应封号订单，针对商户订单 封禁上号OAID
    '''
    sql = """    	
		with tmp_hao_lock_details as (
            select 
               b.game_account as act_zh,
               a.start_stmp_time as start_time,
               a.game_id,
               a.create_time as add_time
            from ods_zhw.game_cheat_account_record a
            left join ods_zhw.game_cheat_account_info b on a.game_account_id=b.id
            where a.type like '%封号%'
            and a.duration/60/60/24>7
            and a.game_id in (443,446,683)
            and (a.fpt = a.pt or a.pt = -1) and a.game_id = b.game_id
            and to_char(a.create_time,'YYYY-MM-DD') >= to_char(date('{0}') - interval '30 days', 'yyyy-mm-dd')
            and to_char(a.create_time,'YYYY-MM-DD') = '{0}' 
            group by 1,2,3,4
		),
		zc_tmp as (
			select a.*,
			c.id as m_order,c.hid as m_hid,c.ip as m_ip,c.gameid as m_gameid,c.userid as m_userid,c.huserid as m_huserid,c.stimer as m_stimer,c.etimer as m_etimer,
			c.item_name as addfrom_name,c.zh as m_zh,
			g.title as game_name_m,
			1 as pn
			from tmp_hao_lock_details a 
			join 
			(
				select c.id,c.hid,c.ip,c.gameid,case when f.order_id is null then c.userid else f.username end as userid,c.huserid,c.stimer,c.etimer,c.add_from,d.zh,
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
					select id,save_date,zh
					from ods_zhw.zhw_hao_archive_day
					where true 
					and save_date between to_char(date('{0}') - interval '10 days','yyyy-mm-dd') 
					and '{1}'
					and gid in (443,446,683)
					union all
					select id,to_char(current_date,'yyyy-mm-dd') as save_date,zh
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
			select a.*,
			c.id as m_order,c.hid as m_hid,c.ip as m_ip,c.gameid as m_gameid,c.userid as m_userid,c.huserid as m_huserid,c.stimer as m_stimer,c.etimer as m_etimer,
			c.item_name as addfrom_name,c.zh as m_zh,
			g.title as game_name_m,
			2 as pn
			from tmp_hao_lock_details a 
			join 
			(
				select c.id,c.hid,c.ip,c.gameid,case when f.order_id is null then c.userid else f.username end as userid,c.huserid,c.stimer,c.etimer,c.add_from,d.zh,
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
					select id,save_date,zh
					from ods_zhw.zhw_hao_archive_day
					where true 
					and save_date between to_char(date('{0}') - interval '10 days','yyyy-mm-dd') 
					and '{1}'
					and gid in (443,446,683)
					union all
					select id,to_char(current_date,'yyyy-mm-dd') as save_date,zh
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
			-- and to_char(a.start_time,'yyyy-mm-dd') between '{0}' and '{1}'
		),
		zc_cd_tmp as (
			select *
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
		)


		select
			{2} as fk_target_id,
			1 as fk_module_type,
			t2.id as fk_uid,
			t1.userid,
			t1.userip as userip,
			t1.gameid as gameid,
			t1.did as did,
			0 as user_mark,
			0 as push_no,
			cast (now() as timestamp)  as push_time,
			0 as is_inc,
			0 as deal_flag 
        from
		(
			select m_order as did,m_userid as userid ,m_ip as userip,m_gameid as gameid
			from zc_cd_tmp
			where true 
			and m_order not in (select did from ods_zhw.zhw_app_oaid where oaid_sh='0000000000000000|' or oaid_xd ='0000000000000000|')
		) t1 
		left join
		(
			select id ,jkx_userid from ods_zhw.zhw_user
		) t2
		on t1.userid=t2.jkx_userid
        left join
        (
            select userid from public.zhw_shanghu_type_all  where save_date = to_char(current_date-1,'yyyy-mm-dd') group by 1
        ) t3
        on t1.userid=t3.userid
        left join
        (
            select did from ods_zhw.safe_center_lock_target_activity_user where fk_target_id = {2} group by 1
        ) t4
        on t1.did=t4.did
        -- left join
        -- (
        --     select userid from ods_zhw.zhw_fx_sublet_kf where status=1
        -- ) t5
        -- on t1.userid=t5.userid
        where t3.userid is not null -- 商户身份
        and t4.did is null -- 已推送的订单
        -- and t5.userid is null 
    """.format(start_day, end_day, group_id)
    print(sql)
    sys.exit()
    report = pd.read_sql(sql, con=holo_cnx)

    print(report)
    report.columns = ['fk_target_id', 'fk_module_type', 'fk_uid', 'userid', 'userip', 'gameid', 'did',
                      'user_mark', 'push_no', 'push_time', 'is_inc', 'deal_flag']
    # report.to_sql(name='safe_center_lock_target_activity_user', con=cnx, if_exists='append', index=False)

if __name__ == '__main__':

    """
    2023-03-24，针对手游查询到的封号匹配出对应封号订单，针对商户订单 封禁上号OAID
    group_name 手游查询封号用户群(商户单-关闭上号oaid)
    """
    now = (datetime.datetime.now()).strftime('%Y%m%d')  # 今日日期
    end_day = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
    start_day = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')  # t-1

    start_day, end_day = '2023-04-26','2023-04-26'
    shouyou_fenghao_match_for_sh_group(end_day, end_day, 47)