# _*_coding:utf-8 _*_

# @Time      : 2023/7/6  15:34
# @Author    : An
# @File      : huser_bestdeal_warn_huser_dim_hid_group.py
# @Software  : 号主刷红包-警告号主-货架逻辑

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

wj_host = cf.get("Mysql-sjwj", "host")
wj_user = cf.get("Mysql-sjwj", "user")
wj_password = cf.get("Mysql-sjwj", "password")
wj_DB = cf.get("Mysql-sjwj", "DB")
wj_port = cf.get("Mysql-sjwj", "port")
cnx = create_engine("mysql+pymysql://" + wj_user + ":" + wj_password + "@" + wj_host + ":" + wj_port + "/" + wj_DB,
                    echo=False)

test_host = cf.get("mysql-localhost", "host")
test_user = cf.get("mysql-localhost", "user")
test_password = cf.get("mysql-localhost", "password")
test_DB = cf.get("mysql-localhost", "DB")
test_port = cf.get("mysql-localhost", "port")
test_cnx = create_engine("mysql+pymysql://" + test_user + ":" + test_password + "@" + test_host + ":" + test_port + "/" + test_DB,
                    echo=False)

def huser_bestdeal_warn_huser_dim_hid_group(start_day, end_day, group_id):
    '''
    :param start_day:
    :param end_day:
    :param group_id:
    :return:
    '''
    # 初始化满足规则的结果集
    final_report = pd.DataFrame()

    # 近3天订单量[3,5)，撤单量0，红包使用金额占订单金额比例80%:警告号主。
    holo_sql = """
        select hid,userid,uid,orders,pm,cd_orders,cd_pm,hb_orders,hb_use_money
        from  
        (
            select hid,userid,uid,orders,pm,cd_orders,cd_pm,hb_orders,hb_use_money,row_number()over(partition by userid order by cd_orders desc) as rn 
            from  
            (	
                select 
                    a.hid,a.huserid as userid,
                    c.id as uid,
                    count(a.id) as orders,
                    sum(a.pm) as pm,
                    count(case when a.zt = 3 then a.id end) as cd_orders,
                    sum(case when a.zt = 3 then a.pm end) as cd_pm,
                    count(b.order_id) as hb_orders,
                    sum(b.use_money) as hb_use_money
                from ods_zhw.zhw_dingdan a 
                left join ods_zhw.zhw_hongbao_order b 
                on a.id = b.order_id and b.part_day between '{0}' and '{1}'
                left join ods_zhw.zhw_user c 
                on a.userid = c.jkx_userid 
                where a.part_day between '{0}' and '{1}'
                group by 1,2,3
                having count(a.id) between 3 and 4 
                and count(case when a.zt = 3 then a.id end) = 0
                and sum(b.use_money)*1.00/sum(a.pm)>=0.8
            ) a
        ) b 
        where b.rn = 1 
    """.format(start_day, end_day, group_id)
    holo_report = pd.read_sql(holo_sql, con=holo_cnx)
    # 无满足刷红包的则退出
    if not len(holo_report):
        return final_report

    # 近6小时已命中的刷红包号主
    mysql_sql = """    	
        -- 近6小时写入数据
        select 
            userid
        from datawj.safe_center_lock_target_activity_user_info
        where fk_target_id in (56,59) -- 号主刷红包-警告号主
        and substring(push_time,1,19) between date_format(date_sub(now(),interval 6 hour),'%%Y-%%m-%%d %%H:%%i:%%S') 
        and date_format(now(),'%%Y-%%m-%%d %%H:%%i:%%S')
        group by 1
    """.format(group_id)
    mysql_report = pd.read_sql(mysql_sql, con=cnx)

    # 1）近6小时已命中的刷红包 为空，则直接将新命中的作为结果数据
    if not len(mysql_report):
        final_report = holo_report.copy()
    # 2）近6小时已命中的刷红包 不为空，从将新命中的结果中剔除已命中的作为结果数据
    final_report = holo_report[~holo_report.userid.isin(mysql_report.userid)]

    return final_report

def push_risk_huser(url,punish_type,risk_object):
    data = {
        'punish_type':punish_type,
        'uid':risk_object
    }
    json_data = json.dumps(data)
    r = requests.post(url = url,data = json_data)
    return r.status_code


if __name__ == '__main__':

    """2023-07-13，
    group_name 号主刷红包-警告号主-货架逻辑 59
    """
    group_id = 59
    group_id = 999

    push_time = (datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
    print(push_time,' 号主刷红包-警告号主-货架逻辑 数据推送情况')

    now = (datetime.datetime.now()).strftime('%Y%m%d')  # 今日日期
    end_day = (datetime.datetime.now()).strftime('%Y-%m-%d')
    start_day = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')

    df_final_report = huser_bestdeal_warn_huser_dim_hid_group(start_day, end_day, group_id)

    if not len(df_final_report):
        print('\t无满足规则条件的结果集')
        sys.exit()
    print('\t1)满足规则条件的结果集{}条'.format(len(df_final_report)))

    uid_set = set(df_final_report.uid)
    risk_object = ','.join([str(u) for u in uid_set])
    punish_type = 'warn_user'
    url = 'http://39.98.193.35:9518/report-risk-user'
    push_status = push_risk_huser(url,punish_type,risk_object)
    print('\t2)对满足规则条件的结果集完成推送')

    if push_status==200:
        report = df_final_report.copy()

        report.rename(columns={'uid': 'fk_uid'}, inplace=True)
        report['push_time'] = push_time

        report['fk_target_id'] = group_id
        report['fk_module_type'] = 1
        report['userip'] = '0'
        report['gameid'] = 0
        report['did'] = 0
        report['user_mark'] = 0
        report['push_no'] = 0
        report['deal_flag'] = 0
        report['is_inc'] = 0
        report['deal_flag'] = 2

        report['info'] = report.apply(
            lambda x:
            '{"orders":' + str(x['orders']) +
            ',"pm":' + str(x['pm']) +
            ',"cd_orders":' + str(x['cd_orders']) +
            ',"cd_pm":' + str(x['cd_pm']) +
            ',"hb_orders":' + str(x['hb_orders']) +
            ',"hb_use_money":' + str(x['hb_use_money']) +
            '}',
            axis=1
        )
        result_report = report[[
            'fk_target_id', 'fk_module_type', 'fk_uid', 'userid', 'userip', 'gameid', 'did',
                              'user_mark', 'push_no', 'push_time', 'is_inc', 'deal_flag', 'info'
        ]]
        result_report.to_sql(name='safe_center_lock_target_activity_user_info', con=cnx, if_exists='append', index=False)

        print('\t3)将推送数据写入数据表')