# _*_coding:utf-8 _*_

# @Time      : 2023/3/30  14:40
# @Author    : An
# @File      : sm_risk_feedback_by_scence.py
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


def sm_risk_feedback_by_scence(run_day, group_id):
    '''
    :param run_day:脚本运营日期
    :param group_id:群组ID
    :return:疑似微信渠道诈骗的用户信息
    '''
    sql = """
        select 
            trigg_opera_type 
            ,count(*) as return_times
            ,count(distinct sm_id) as return_smids
            ,count(case when rule_name<>'' then sm_id end) as rule_times
            ,count(distinct case when rule_name<>'' then sm_id end) as rule_smids
            ,count(case when act_penalty<>'' then sm_id end) as action_times
            ,count(distinct case when act_penalty<>'' then sm_id end) as action_smids
        from ods_zhw.zhw_safe_defense_detail_v1 
        where true 
        and part_day between to_char(current_date - interval '1 days','yyyy-mm-dd') and to_char(current_date,'yyyy-mm-dd')
        and add_time between current_timestamp - interval '30 minutes' and current_timestamp  
        and trigg_opera_type<>'' and trigg_opera_type is not null 
        group by 1
        order by 2 desc 
    """
    sql = sql.format(run_day, group_id)

    report = pd.read_sql(sql, con=holo_cnx)

    if not len(report):
        return pd.DataFrame()

    report.columns = [
        'trigg_opera_type','return_times','return_smids','rule_times','rule_smids','action_times','action_smids'
    ]
    print(report)

    return report

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

    title = '风险反馈记录-数据统计'

    cur_ts = datetime.datetime.now()
    cur_ts.strftime('%Y-%m-%d %H:%M:%S')
    rule_desc = '<font color="info">' + '**风险反馈记录（近30分钟）**' + ' (' + cur_ts.strftime('%Y-%m-%d %H:%M:%S') + ')' + '</font>' +'\n'
    for idx,item in df.iterrows():
        trigg_opera_type = item['trigg_opera_type']
        return_times = item['return_times']
        return_smids = item['return_smids']
        rule_times = item['rule_times']
        rule_smids = item['rule_smids']
        action_times = item['action_times']
        action_smids = item['action_smids']

        rule_desc = rule_desc + '**' + str(idx+1) + ') ' + '{0:<8}'.format(trigg_opera_type) + '**' + '\n'+ \
                    '\t' + ' 反馈次数 ' + '<font color="warning">' + str(return_times) + '</font>' + '\n' + \
                    '\t' + ' 反馈设备数 ' + '<font color="warning">' + str(return_smids) + '</font>' + '\n' + \
                    '\t' + ' 匹配规则次数 ' + '<font color="warning">' + str(rule_times) + '</font>' + '\n' + \
                    '\t' + ' 匹配规则设备数 ' + '<font color="warning">' + str(rule_smids) + '</font>' + '\n' + \
                    '\t' + ' 触发行为处罚次数 ' + '<font color="warning">' + str(action_times) + '</font>' + '\n' + \
                    '\t' + ' 触发行为处罚设备数 ' + '<font color="warning">' + str(action_smids) + '</font>'  + '\n'

    return '## ' + title + '\n' + '>' + rule_desc


if __name__ == '__main__':
    date_range = pd.date_range(start='2023-03-28', end='2023-03-28')
    a = [d.strftime('%Y-%m-%d') for d in date_range.tolist()]
    for part_day in a:
        costtime_df = sm_risk_feedback_by_scence(part_day,46)

        if len(costtime_df):
            work_wxrobot_content = work_wxrobot_content(costtime_df)
            test_webhook = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=231ae38a-3d31-4635-80d2-800029963832'
            work_wxrobot(work_wxrobot_content, test_webhook)