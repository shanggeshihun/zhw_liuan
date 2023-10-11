# _*_coding:utf-8 _*_

# @Time      : 2023/4/14  14:40
# @Author    : An
# @File      : shouyou_ban_userid_with_smid_group_fortg99999999.py
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


def shouyou_ban_userid_with_smid_group(day_last_1, now_day, group_id):
    sql = """    
		select
		    distinct
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
			from 
			(
				-- 近30分钟添加封号数据的封号订单
				select *
				from public.zhw_hao_lock_order_match
				where target_fk_id = {2}
				and push_time>=to_char(current_timestamp  - interval '40 minutes','yyyy-mm-dd hh24:mi:ss')
				and addfrom_name not in('分销合伙人','新转租平台','分销普通版','分销尊享版','分销高级版')
				and m_order not in (select did from ods_zhw.zhw_app_oaid where oaid_sh in ('0000000000000000|','0000000000000000|','00000000000000000000000000000000'))
				and (game_id in (443) or (game_id in (446,683) and m_os = 'ios'))
			) a 
			where true 			
			
			union all 
			select 0 as did,c.user_id as userid,'' as userip,0 as gameid
			from 
			(	-- 近30分钟添加封号数据的封号订单的用户
				select m_userid
				from public.zhw_hao_lock_order_match
				where target_fk_id = {2}
				and push_time>=to_char(current_timestamp  - interval '40 minutes','yyyy-mm-dd hh24:mi:ss')
				and addfrom_name not in('分销合伙人','新转租平台','分销普通版','分销尊享版','分销高级版')
				and m_order not in (select did from ods_zhw.zhw_app_oaid where oaid_sh in ('0000000000000000|','0000000000000000|','00000000000000000000000000000000'))
				and (game_id in (443) or (game_id in (446,683) and m_os = 'ios'))
				group by 1 
			) a
			join 
			(	-- 近2天的数美请求
				select smid,user_id 
				from ods_zhw.zhw_safe_defense_detail
				where true 
				and part_day between '{0}' and '{1}'
				group by 1,2
			) b 
			on a.m_userid = b.user_id 
			join 
			(	-- 近2天的数美请求
				select smid,user_id 
				from ods_zhw.zhw_safe_defense_detail
				where true 
				and part_day between '{0}' and '{1}'
				group by 1,2
			) c 
			on b.smid = c.smid
			group by 1,2,3,4
		) t1 
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
            select userid from ods_zhw.safe_center_lock_target_activity_user where fk_target_id = {2} group by 1
        ) t4
        on t1.userid=t4.userid
        left join
        (
            select userid from ods_zhw.zhw_fx_sublet_kf where status=1
        ) t5
        on t1.userid=t5.userid
        where t3.userid is null
        and t4.userid is null
        and t5.userid is null 
    """.format(day_last_1, now_day, group_id)
    print(sql)
    report = pd.read_sql(sql, con=holo_cnx)

    print(report)

    report.columns = [
        'fk_target_id', 'fk_module_type', 'fk_uid', 'userid', 'userip', 'gameid', 'did',
        'user_mark', 'push_no', 'push_time', 'is_inc', 'deal_flag'
    ]
    # report.to_sql(name='shouyou_ban_userid_with_smid_group', con=cnx, if_exists='append', index=False)


if __name__ == '__main__':
    """
    手游导致封号普通用户及SMID关联用户-关闭封杀
    """
    s_time = time.time()
    now_day = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
    day_last_1 = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')  # t-1
    shouyou_ban_userid_with_smid_group(day_last_1,now_day,50)
    e_time = time.time()
    print('脚本执行耗时：',e_time - s_time)

