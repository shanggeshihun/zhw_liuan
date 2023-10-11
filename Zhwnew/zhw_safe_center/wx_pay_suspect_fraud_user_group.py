# _*_coding:utf-8 _*_

# @Time      : 2023/3/22  14:40
# @Author    : An
# @File      : wx_pay_suspect_fraud_user_group.py
# @Software  : PyCharm



import time, datetime ,warnings
import pandas as pd
import requests
import redis
import json
from pyhive import presto
import psycopg2
from sqlalchemy import create_engine
import configparser
from dateutil.relativedelta import relativedelta
from collections import defaultdict


warnings.filterwarnings("ignore")
#------------------------参数变量区----------------------------
# 配置运营数据库地址
cf = configparser.ConfigParser()
if cf.read("D:/code/python/config.ini", encoding='utf-8') == []:
    """服务器模式"""
    cf.read("/usr/model/zhw_product/config/config.ini", encoding='utf-8')
else:
    """本地模式"""
    cf.read("D:/code/python/config.ini", encoding='utf-8')

bigdata_redis_host = cf.get("bigdata_redis","host")
bigdata_redis_port = cf.get("bigdata_redis","port")
bigdata_redis_password = cf.get("bigdata_redis","password")
bigdata_redis_db = cf.get("bigdata_redis","user_db")

host = cf.get("hive_presto","host")
username = cf.get("hive_presto","username")
port = cf.get("hive_presto","port")
schema = cf.get("hive_presto","schema")
catalog = cf.get("hive_presto","catalog")
presto_db = presto.connect(host=host, port=port,  username=username, schema=schema,catalog=catalog)

holo_host = cf.get("Hologres","host")
holo_user = cf.get("Hologres","user")
holo_password = cf.get("Hologres","password")
holo_DB = cf.get("Hologres","db")
holo_port = cf.get("Hologres","port")
holo_cnx = create_engine("postgresql+psycopg2://"+holo_user+":"+holo_password+"@"+holo_host+":"+holo_port+"/"+holo_DB)

wj_host = cf.get("Mysql-sjwj","host")
wj_user = cf.get("Mysql-sjwj","user")
wj_password = cf.get("Mysql-sjwj","password")
wj_DB = cf.get("Mysql-sjwj","DB")
wj_port = cf.get("Mysql-sjwj","port")
cnx = create_engine("mysql+pymysql://"+wj_user+":"+wj_password+"@"+wj_host+":"+wj_port+"/"+wj_DB, echo=False)

wj_host = cf.get("Mysql-Database-test","host")
wj_user = cf.get("Mysql-Database-test","user")
wj_password = cf.get("Mysql-Database-test","password")
wj_DB = cf.get("Mysql-Database-test","DB")
#wj_port = cf.get("Mysql-Database-test","port")
test_cnx = create_engine("mysql+pymysql://"+wj_user+":"+wj_password+"@"+wj_host+":"+wj_port+"/"+wj_DB, echo=False)
#------------------------参数配置----------------------------
now = (datetime.datetime.now()).strftime('%Y%m%d') #今日日期
now_day = (datetime.datetime.now()).strftime('%Y-%m-%d') #今日日期
day_last_1 = (datetime.datetime.now() - datetime.timedelta(days = 1)).strftime('%Y-%m-%d') #t-1
day_last_2 = (datetime.datetime.now() - datetime.timedelta(days = 2)).strftime('%Y-%m-%d') #t-2
day_last_3 = (datetime.datetime.now() - datetime.timedelta(days = 3)).strftime('%Y-%m-%d') #t-3
day_last_7 = (datetime.datetime.now() - datetime.timedelta(days = 6)).strftime('%Y-%m-%d') #t-7
day_last_15 = (datetime.datetime.now() - datetime.timedelta(days = 15)).strftime('%Y-%m-%d') #t-30
day_last_30 = (datetime.datetime.now() - datetime.timedelta(days = 30)).strftime('%Y-%m-%d') #t-30
day_last_365 = (datetime.datetime.now() - datetime.timedelta(days = 365)).strftime('%Y-%m-%d') #t-30
month_last_3 = (datetime.date.today() - relativedelta(months = +3)).strftime('%Y%m%d')


day_now_H = (datetime.datetime.now()).strftime('%Y%m%d%H') #h
day_now_H_Last = (datetime.datetime.now() - datetime.timedelta(hours = 1)).strftime('%Y%m%d%H') #h-1
day_now_H_Last_2 = (datetime.datetime.now() - datetime.timedelta(hours = 2)).strftime('%Y%m%d%H') #h-2
day_now_H_Last_4 = (datetime.datetime.now() - datetime.timedelta(hours = 4)).strftime('%Y%m%d%H') #h-4
day_now_H_Last_13 = (datetime.datetime.now() - datetime.timedelta(hours = 12)).strftime('%Y%m%d%H') #h-12
day_now_H_Last_24 = (datetime.datetime.now() - datetime.timedelta(hours = 24)).strftime('%Y%m%d%H') #h-24

month = (datetime.date.today()).strftime('%Y-%m') #t-1 #当月日期
last_month = (datetime.date.today() - relativedelta(months=+1)).strftime('%Y-%m') #t-1 #上月日期
last_month_1 = (datetime.date.today() - relativedelta(months=+2)).strftime('%Y-%m') #t-2
day_Week = datetime.datetime.now().weekday()
pre_hour = (datetime.datetime.now() - datetime.timedelta(hours = 1)).hour



def wx_pay_suspect_fraud_user_group(run_day, group_id):
    '''
    :param run_day:脚本运营日期
    :param group_id:群组ID
    :return:疑似微信渠道诈骗的用户信息
    '''
    sql = """
        select 
            {1} as fk_target_id,
            t1.user_type,t1.username as userid,t1.open_num,t1.money,t1.max_money,
            case
                when t4.jkx_userstatus = 0 then '系统关闭会员'
                when t4.jkx_userstatus = 1 then '开启'
                when t4.jkx_userstatus = 2 then '商户关闭会员'
                when t4.jkx_userstatus = 3 then '注销会员'
            end as userstatus,
            t4.jkx_usermoney,t4.jkx_bz,
            t2.max_part_day,
            case when t2.max_part_day is null then 3650 else date('{0}') - 1 - date(t2.max_part_day) end as recent_order_days,
            coalesce(t2.order_days,0) as order_days ,coalesce(t2.orders,0) as orders,coalesce(t2.order_pm,0.00) as order_pm,
            coalesce(t5.order_days,0) as last2w_order_days,coalesce(t5.orders,0) as last2w_orders,coalesce(t5.order_pm,0.00) as last2w_order_pm,
            date(t4.jkx_timer) as reg_time,
            date('{0}') - 1 - date(t4.jkx_timer) as reg_days,
            t4.closetimer,t4.id as fk_uid,
            cast(now() as timestamp) as push_time
        from
        (
            select
                case when user_type = 0 then '非商户非分销' else '其他' end as user_type,
                username,open_num,money
            from
            (
                select
                    case when t2.userid is not null then 1 else 0 end user_type,username,
                    count(distinct pay_num) open_num,
                    sum(money) as money,
                    max(money) as max_money
                from ods_zhw.zhw_recharge t1
                -- join ods_zhw.zhw_user u
                -- on t1.username = u.jkx_userid and u.closetimer between cast('{0}' || ' 00:00:00' as timestamp) and cast('{0}' || ' 23:59:59' as timestamp)
                left join ods_zhw.zhw_shanghu_type_log t2
                on t1.username = t2.userid and sh_type > 0
                where t1.part_day between to_char(date('{0}') - interval '6 days','yyyy-mm-dd')  and to_char(date('{0}') - interval '0 days','yyyy-mm-dd')
                and status = 2 and viaid = 3 and pay_num <> ''
                group by 1,2
            ) a
            where open_num >=4
            and user_type = 0 -- 非商户非分销
            and money>200
        ) t1
        left join
        (
            select userid,sum(pm) order_pm,max(part_day) as max_part_day,count(distinct part_day) as order_days,count(id) as orders
            from ods_zhw.zhw_dingdan
            where part_day between to_char(date('{0}') - interval '6 days','yyyy-mm-dd')  and to_char(date('{0}') - interval '0 days','yyyy-mm-dd')
            group by 1
        ) t2
        on t1.username = t2.userid
        left join ods_zhw.zhw_user t4
        on t1.username = t4.jkx_userid
        left join
        (
            select userid,sum(pm) order_pm,max(part_day) as max_part_day,count(distinct part_day) as order_days,count(id) as orders
            from ods_zhw.zhw_dingdan
            where part_day between to_char(date('{0}') - interval '13 days','yyyy-mm-dd')  and to_char(date('{0}') - interval '7 days','yyyy-mm-dd')
            group by 1
        ) t5
        on t1.username = t5.userid
        left join 
        (
            select userid 
            from ods_zhw.safe_center_lock_target_activity_user 
            where true 
            and fk_target_id = 46
            group by 1
        ) f 
        on t1.username = f.userid 
        where f.userid is null 
        order by 1,3 desc,4 desc
    """
    sql = sql.format(run_day, group_id)

    report = pd.read_sql(sql, con=holo_cnx)

    if not len(report):
        return pd.DataFrame()

    report['fk_module_type'] = 1
    report['userip'] = '0'
    report['gameid'] = 0
    report['did'] = 0
    report['user_mark'] = 0
    report['push_no'] = 0
    report['deal_flag'] = 0
    report['is_inc'] = 0
    report['deal_flag'] = 0

    print('初筛用户集合:\n', report)

    # 初始化最后封号用户df
    ban_report = pd.DataFrame()

    # 规则1 当命中用户数量超过10个时随机获取百分之10；当命中用户数量不足百分之10时取充值金额高于300且充值TOP1
    rule1_report = report[report.open_num == 4]
    rule1_report.reset_index(inplace=True, drop=True)

    len_rule1_report = len(rule1_report)
    if len_rule1_report >= 9:
        chose_num = int(len_rule1_report * 0.1)
        rule1_report_ban = rule1_report.head(chose_num)
    elif len_rule1_report == 0:
        rule1_report_ban = pd.DataFrame()
    else:
        max_rule1_report = rule1_report[rule1_report.money >= 300]
        if len(max_rule1_report):
            max_rule1_report.sort_values(by='money', ascending=False, inplace=True)
            rule1_report_ban = max_rule1_report.head(1)
        else:
            rule1_report_ban = pd.DataFrame()

    # 规则2
    rule2_report = report[report.open_num >= 5]
    rule2_report.reset_index(inplace=True, drop=True)
    if len(rule2_report):
        rule2_report_ban = rule2_report.copy()
    else:
        rule2_report_ban = pd.DataFrame()

    # 规则1 + 规则 2 数据集合
    ban_report = ban_report.append(rule1_report_ban)
    ban_report = ban_report.append(rule2_report_ban)

    if not len(ban_report):
        print('无满足条件的待封账号')
        return pd.DataFrame()


    report_part = ban_report[['fk_target_id', 'fk_module_type', 'fk_uid', 'userid', 'userip', 'gameid', 'did',
                              'user_mark', 'push_no', 'push_time', 'is_inc', 'deal_flag']]
    print('封号用户集合:\n', report_part)

    report_part.to_sql(name='safe_center_lock_target_activity_user', con=cnx, if_exists='append', index=False)

    ban_report2 = ban_report.copy()
    ban_report2['info'] = ban_report2.apply(
        lambda x:
        '{"open_num":' + str(x['open_num']) +
        ',"money":' + str(x['money']) +
        ',"max_money":' +str(x['max_money']) +
        ',"order_days":' + str(x['order_days']) +
        ',"orders":' + str(x['orders']) +
        ',"order_pm":' + str(x['order_pm']) +
        ',"reg_time":\"' + str(x['reg_time']) + '\"'+
        '}',
        axis = 1
    )
    report_part2 = ban_report2[['fk_target_id', 'fk_module_type', 'fk_uid', 'userid', 'userip', 'gameid', 'did',
                              'user_mark', 'push_no', 'push_time', 'is_inc', 'deal_flag', 'info']]
    print('封号用户集合-信息:\n', report_part2)
    report_part2.to_sql(name='safe_center_lock_target_activity_user_info', con=cnx, if_exists='append', index=False)
    return ban_report

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

def work_wxrobot_content(df):
    '''
    :param df:DataFrame 数据集
    :return:微信播报内容
    '''
    df.sort_values(by='open_num',ascending=False,inplace=True)
    df.reset_index(inplace=True, drop=True)

    title = '微信支付渠道诈骗 风险用户信息'

    cur_ts = datetime.datetime.now()
    cur_ts.strftime('%Y-%m-%d %H:%M:%S')
    rule_desc = '<font color="info">' + '**用户近7日更换openid数量及充值金额(元)**' + ' (' + cur_ts.strftime('%Y-%m-%d %H:%M:%S') + ')' + '</font>' +'\n'
    for idx,item in df.iterrows():
        userid = item['userid']
        open_num = item['open_num']
        money = item['money']

        rule_desc = rule_desc + \
                    '\t' + str(idx+1) + ') ' + \
                    str(userid) + \
                    ' openid数量 ' + '<font color="warning">' + str(open_num) + '</font>' + '，' + \
                    ' 充值总金额 ' + '<font color="warning">' + str(money) + '</font>' + '\n'

    return '## ' + title + '\n' + '>' + rule_desc


if __name__ == '__main__':
    now_day = (datetime.datetime.now()).strftime('%Y-%m-%d')
    risk_df = wx_pay_suspect_fraud_user_group(now_day,46)

    if len(risk_df):
        work_wxrobot_content = work_wxrobot_content(risk_df)
        test_webhook = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=231ae38a-3d31-4635-80d2-800029963832'
        webhook = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=ec7df926-331e-4976-bc4e-be94b2ea54b1'
        work_wxrobot(work_wxrobot_content, webhook)