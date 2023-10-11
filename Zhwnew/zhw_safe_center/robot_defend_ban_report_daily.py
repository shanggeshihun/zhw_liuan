# _*_coding:utf-8 _*_

# @Time      : 2023/7/25  10:46
# @Author    : An
# @File      : robot_defend_ban_report_daily.py
# @Software  : PyCharm


import time, datetime
import pandas as pd
import requests,sys
import redis
import json
from pyhive import presto
import psycopg2
from sqlalchemy import create_engine
import configparser
from dateutil.relativedelta import relativedelta
from collections import defaultdict

# ------------------------参数变量区----------------------------
# 配置运营数据库地址
cf = configparser.ConfigParser()
if cf.read("D:/code/python/config.ini", encoding='utf-8') == []:
    """服务器模式"""
    cf.read("/usr/model/zhw_product/config/config.ini", encoding='utf-8')
else:
    """本地模式"""
    cf.read("D:/code/python/config.ini", encoding='utf-8')

bigdata_redis_host = cf.get("bigdata_redis", "host")
bigdata_redis_port = cf.get("bigdata_redis", "port")
bigdata_redis_password = cf.get("bigdata_redis", "password")
bigdata_redis_db = cf.get("bigdata_redis", "user_db")

host = cf.get("hive_presto", "host")
username = cf.get("hive_presto", "username")
port = cf.get("hive_presto", "port")
schema = cf.get("hive_presto", "schema")
catalog = cf.get("hive_presto", "catalog")
presto_db = presto.connect(host=host, port=port, username=username, schema=schema, catalog=catalog)

holo_host = cf.get("Hologres_defend_r", "host")
holo_user = cf.get("Hologres_defend_r", "user")
holo_password = cf.get("Hologres_defend_r", "password")
holo_DB = cf.get("Hologres_defend_r", "db")
holo_port = cf.get("Hologres_defend_r", "port")
holo_cnx = create_engine(
    "postgresql+psycopg2://" + holo_user + ":" + holo_password + "@" + holo_host + ":" + holo_port + "/" + holo_DB)

wj_host = cf.get("Mysql-sjwj", "host")
wj_user = cf.get("Mysql-sjwj", "user")
wj_password = cf.get("Mysql-sjwj", "password")
wj_DB = cf.get("Mysql-sjwj", "DB")
wj_port = cf.get("Mysql-sjwj", "port")
cnx = create_engine("mysql+pymysql://" + wj_user + ":" + wj_password + "@" + wj_host + ":" + wj_port + "/" + wj_DB,
                    echo=False)

'''
******** 基础函数定义 ********
'''
def calc_tb_hb(type='ratio',numeric=0,compare_numeric=0):
    '''
    :param type: 计算同环比时，对比数据的类型，数值or比例
    :param numeric:当前数据
    :param compare_numeric:对比期数据
    :return:同环比结果
    '''
    if type == 'value':
        if compare_numeric == 0:
            if numeric>0:
                return 1
            else:
                return 0
        else:
            return numeric/compare_numeric - 1
    else:
        if compare_numeric == 0:
            return numeric
        else:
            return numeric - compare_numeric

def float_to_percent_nosign(float_numeric):
    return [str(round(float_numeric * 100.000, 2)) if float_numeric > 0 else str(round(float_numeric * 100.000 * (-1), 2))][0] + "%"

def mapping_fontcolor(numeric):
    """
    :param numeric: 数值
    :return: 大于0 绿色，小于0 橙色，等于0 灰色
    """
    if numeric > 0:
        return "warning"
    elif numeric < 0:
        return "info"
    else:
        return "comment"

def mapping_updown(numeric):
    """
    :param values: 数值
    :return: 大于0 上升，小于0 下降，等于0 持平
    """
    if numeric > 0:
        return "上升"
    elif numeric < 0:
        return "下降"
    else:
        return "持平"

def null_series_default(pd_series):
    if len(pd_series) == 0:
        return pd.Series([0,0])
    else:
        return pd_series

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

_now = (datetime.datetime.now() - datetime.timedelta(days=0)).strftime('%Y-%m-%d %H:%M')
_current_day = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%m%d')

current_time = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
current_day = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')


# 数据窗口日期
start_day = (datetime.datetime.now() - datetime.timedelta(days=15)).strftime('%Y-%m-%d')
end_day = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

part_day_h = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')
part_day_t = (datetime.datetime.now() - datetime.timedelta(days=8)).strftime('%Y-%m-%d')


# 封号率 按 日期、游戏、核实类型
ban_dim_day_game = '''
    select 
        t1.part_day,
        t1.game_type,
        '包含无关封号' as verify_type,
        t1.title as game_name,
        max(t1.ord_num) as orders,
        max(t1.ord_h_num) as rent_hjs,
        coalesce(sum(t2.hao_lock),0) as bans
    from
    (
        select 
            a.part_day,
            (case b.categoryid when 1 then '手游' when 2 then '端游' end) game_type,
            b.title,
            a.gameid,
            count(distinct hid) ord_h_num,
            count(a.id) ord_num
        from ods_zhw.zhw_dingdan a
        left join ods_zhw.zhw_game_info b on a.gameid = b.id
        where a.part_day between '{0}' and '{1}'
        and gameid in (11,17,24,443,446,581,683)
        group by 1,2,3,4
    ) t1
    left join
    (
        select 
            to_char(start_time,'yyyy-mm-dd') as part_day,
            game_id,
            count(distinct act_zh) as hao_lock
        from ods_zhw.zhw_hao_lock_details
        where extract(epoch from end_time) - extract(epoch from start_time)>7*24*3600
        and to_char(start_time,'yyyy-mm-dd') between '{0}' and '{1}'
        group by 1,2    
    ) t2 
    on t1.part_day = t2.part_day and t1.gameid=t2.game_id
    group by 1,2,3,4
    
    union all 
    select 
        t1.part_day,
        t1.game_type,
        '剔除无关封号' as verify_type,
        t1.title as game_name,
        max(t1.ord_num) as orders,
        max(t1.ord_h_num) as rent_hjs,
        coalesce(sum(t2.hao_lock),0) as bans
    from
    (
        select 
            a.part_day,
            (case b.categoryid when 1 then '手游' when 2 then '端游' end) game_type,
            b.title,
            a.gameid,
            count(distinct hid) ord_h_num,
            count(a.id) ord_num
        from ods_zhw.zhw_dingdan a
        left join ods_zhw.zhw_game_info b on a.gameid = b.id
        where a.part_day between '{0}' and '{1}' 
        and gameid in (11,17,24,443,446,581,683)
        group by 1,2,3,4
    ) t1
    left join
    (
        select 
            to_char(start_time,'yyyy-mm-dd') as part_day,
            game_id,
            count(distinct act_zh) as hao_lock
        from 
        (
            select start_time,game_id,act_zh,
            case    
                when c.dict_label ~ '@0$' then '无关'
                when c.dict_label ~ '@1$' then '外挂'
                when c.dict_label ~ '@2$' then '怀疑'
                when c.dict_label ~ '@3$' then '盗号'
                when c.dict_label ~ '@4$' then '手游'
                else '其他'
            end as verify_type
            from 
            (
                select a.start_time,a.game_id,a.act_zh,
                d.dict_label,
                row_number()over(partition by a.record_id order by a.verify_id desc) as rn
                from ods_zhw.zhw_hao_lock_details a 
                left join ods_zhw.sys_dict_data d 
                on a.banned_type = d.dict_value and d.dict_type ='sys_fh_type' and d.dict_label like '%%@%%'
                where to_char(a.start_time,'yyyy-mm-dd') between '{0}' and '{1}'
                and extract(epoch from a.end_time) - extract(epoch from a.start_time)>7*24*3600
            ) c 
            where c.rn = 1 
        ) a 
        where a.verify_type<>'无关' and a.verify_type<>'盗号'
        group by 1,2
    ) t2 
    on t1.part_day = t2.part_day and t1.gameid=t2.game_id
    group by 1,2,3,4
    order by 1 desc 
'''.format(start_day,end_day)
tmp_df = pd.read_sql(ban_dim_day_game, con=holo_cnx)

title = '# ** {}手游&端游封号率统计 **({})'.format(_current_day,_now)
title = '# ** {}手游封号率统计 **({})'.format(_current_day,_now)

part1_title = '## **一、手游整体**'
part1_subtitle1 = '#### ① 包含无关封号'
game_type, verify_type = '手游', '包含无关封号'
tmp_bans = null_series_default(tmp_df[(tmp_df.game_type == game_type) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].bans).sum()
tmp_bans_h = null_series_default(tmp_df[(tmp_df.game_type == game_type) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].bans).sum()
tmp_bans_t = null_series_default(tmp_df[(tmp_df.game_type == game_type) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].bans).sum()

tmp_orders = null_series_default(tmp_df[(tmp_df.game_type == game_type) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].orders).sum()
tmp_orders_h = null_series_default(tmp_df[(tmp_df.game_type == game_type) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].orders).sum()
tmp_orders_t = null_series_default(tmp_df[(tmp_df.game_type == game_type) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].orders).sum()

tmp_ban_ratio = tmp_bans / tmp_orders
tmp_bans_h_ratio = tmp_bans_h / tmp_orders_h
tmp_bans_t_ratio = tmp_bans_t / tmp_orders_t

tmp_bans_hb, tmp_bans_tb = calc_tb_hb('value', tmp_bans, tmp_bans_h), calc_tb_hb('value', tmp_bans, tmp_bans_t)
tmp_bans_ratio_hb, tmp_bans_ratio_tb = calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_h_ratio), calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_t_ratio)

part1_subcontent1 ='\t封号量**{}**、环比<font color="{}">{}**{}**%</font>、同比<font color="{}">{}**{}%**</font>\n' \
'\t封号率**{}%%**、环比<font color="{}">{}**{}**%%</font>、同比<font color="{}">{}**{}%%**</font>'.format(
    tmp_bans,
    mapping_fontcolor(tmp_bans_hb), mapping_updown(tmp_bans_hb), round(abs(tmp_bans_hb) * 100, 1),
    mapping_fontcolor(tmp_bans_tb), mapping_updown(tmp_bans_tb), round(abs(tmp_bans_tb) * 100, 1),
    round(tmp_ban_ratio*10000,1),
    mapping_fontcolor(tmp_bans_ratio_hb), mapping_updown(tmp_bans_ratio_hb), round(abs(tmp_bans_ratio_hb) * 10000, 1),
    mapping_fontcolor(tmp_bans_ratio_tb), mapping_updown(tmp_bans_ratio_tb), round(abs(tmp_bans_ratio_tb) * 10000, 1)
)

part1_subtitle2 = '#### ② 剔除无关封号'
game_type, verify_type = '手游', '剔除无关封号'
tmp_bans = null_series_default(tmp_df[(tmp_df.game_type == game_type) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].bans).sum()
tmp_bans_h = null_series_default(tmp_df[(tmp_df.game_type == game_type) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].bans).sum()
tmp_bans_t = null_series_default(tmp_df[(tmp_df.game_type == game_type) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].bans).sum()

tmp_orders = null_series_default(tmp_df[(tmp_df.game_type == game_type) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].orders).sum()
tmp_orders_h = null_series_default(tmp_df[(tmp_df.game_type == game_type) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].orders).sum()
tmp_orders_t = null_series_default(tmp_df[(tmp_df.game_type == game_type) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].orders).sum()

tmp_ban_ratio = tmp_bans / tmp_orders
tmp_bans_h_ratio = tmp_bans_h / tmp_orders_h
tmp_bans_t_ratio = tmp_bans_t / tmp_orders_t

tmp_bans_hb, tmp_bans_tb = calc_tb_hb('value', tmp_bans, tmp_bans_h), calc_tb_hb('value', tmp_bans, tmp_bans_t)
tmp_bans_ratio_hb, tmp_bans_ratio_tb = calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_h_ratio), calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_t_ratio)

part1_subcontent2 ='\t封号量**{}**、环比<font color="{}">{}**{}**%</font>、同比<font color="{}">{}**{}%**</font>\n' \
'\t封号率**{}%%**、环比<font color="{}">{}**{}**%%</font>、同比<font color="{}">{}**{}%%**</font>'.format(
    tmp_bans,
    mapping_fontcolor(tmp_bans_hb), mapping_updown(tmp_bans_hb), round(abs(tmp_bans_hb) * 100, 1),
    mapping_fontcolor(tmp_bans_tb), mapping_updown(tmp_bans_tb), round(abs(tmp_bans_tb) * 100, 1),
    round(tmp_ban_ratio*10000,1),
    mapping_fontcolor(tmp_bans_ratio_hb), mapping_updown(tmp_bans_ratio_hb), round(abs(tmp_bans_ratio_hb) * 10000, 1),
    mapping_fontcolor(tmp_bans_ratio_tb), mapping_updown(tmp_bans_ratio_tb), round(abs(tmp_bans_ratio_tb) * 10000, 1)
)


part2_title = '## **二、王者荣耀**'
part2_subtitle1 = '#### ① 包含无关封号'
game_name, verify_type = '王者荣耀', '包含无关封号'
tmp_bans = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].bans).sum()
tmp_bans_h = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].bans).sum()
tmp_bans_t = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].bans).sum()

tmp_orders = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].orders).sum()
tmp_orders_h = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].orders).sum()
tmp_orders_t = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].orders).sum()

tmp_ban_ratio = tmp_bans / tmp_orders
tmp_bans_h_ratio = tmp_bans_h / tmp_orders_h
tmp_bans_t_ratio = tmp_bans_t / tmp_orders_t

tmp_bans_hb, tmp_bans_tb = calc_tb_hb('value', tmp_bans, tmp_bans_h), calc_tb_hb('value', tmp_bans, tmp_bans_t)
tmp_bans_ratio_hb, tmp_bans_ratio_tb = calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_h_ratio), calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_t_ratio)

part2_subcontent1 ='\t封号量**{}**、环比<font color="{}">{}**{}**%</font>、同比<font color="{}">{}**{}%**</font>\n' \
'\t封号率**{}%%**、环比<font color="{}">{}**{}**%%</font>、同比<font color="{}">{}**{}%%**</font>'.format(
    tmp_bans,
    mapping_fontcolor(tmp_bans_hb), mapping_updown(tmp_bans_hb), round(abs(tmp_bans_hb) * 100, 1),
    mapping_fontcolor(tmp_bans_tb), mapping_updown(tmp_bans_tb), round(abs(tmp_bans_tb) * 100, 1),
    round(tmp_ban_ratio*10000,1),
    mapping_fontcolor(tmp_bans_ratio_hb), mapping_updown(tmp_bans_ratio_hb), round(abs(tmp_bans_ratio_hb) * 10000, 1),
    mapping_fontcolor(tmp_bans_ratio_tb), mapping_updown(tmp_bans_ratio_tb), round(abs(tmp_bans_ratio_tb) * 10000, 1)
)

part2_subtitle2 = '#### ② 剔除无关封号'
game_name, verify_type = '王者荣耀', '剔除无关封号'
tmp_bans = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].bans).sum()
tmp_bans_h = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].bans).sum()
tmp_bans_t = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].bans).sum()

tmp_orders = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].orders).sum()
tmp_orders_h = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].orders).sum()
tmp_orders_t = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].orders).sum()

tmp_ban_ratio = tmp_bans / tmp_orders
tmp_bans_h_ratio = tmp_bans_h / tmp_orders_h
tmp_bans_t_ratio = tmp_bans_t / tmp_orders_t

tmp_bans_hb, tmp_bans_tb = calc_tb_hb('value', tmp_bans, tmp_bans_h), calc_tb_hb('value', tmp_bans, tmp_bans_t)
tmp_bans_ratio_hb, tmp_bans_ratio_tb = calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_h_ratio), calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_t_ratio)

part2_subcontent2 ='\t封号量**{}**、环比<font color="{}">{}**{}**%</font>、同比<font color="{}">{}**{}%**</font>\n' \
'\t封号率**{}%%**、环比<font color="{}">{}**{}**%%</font>、同比<font color="{}">{}**{}%%**</font>'.format(
    tmp_bans,
    mapping_fontcolor(tmp_bans_hb), mapping_updown(tmp_bans_hb), round(abs(tmp_bans_hb) * 100, 1),
    mapping_fontcolor(tmp_bans_tb), mapping_updown(tmp_bans_tb), round(abs(tmp_bans_tb) * 100, 1),
    round(tmp_ban_ratio*10000,1),
    mapping_fontcolor(tmp_bans_ratio_hb), mapping_updown(tmp_bans_ratio_hb), round(abs(tmp_bans_ratio_hb) * 10000, 1),
    mapping_fontcolor(tmp_bans_ratio_tb), mapping_updown(tmp_bans_ratio_tb), round(abs(tmp_bans_ratio_tb) * 10000, 1)
)


part3_title = '## **三、枪战王者**'
part3_subtitle1 = '#### ① 包含无关封号'
game_name, verify_type = '枪战王者', '包含无关封号'
tmp_bans = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].bans).sum()
tmp_bans_h = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].bans).sum()
tmp_bans_t = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].bans).sum()

tmp_orders = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].orders).sum()
tmp_orders_h = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].orders).sum()
tmp_orders_t = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].orders).sum()

tmp_ban_ratio = tmp_bans / tmp_orders
tmp_bans_h_ratio = tmp_bans_h / tmp_orders_h
tmp_bans_t_ratio = tmp_bans_t / tmp_orders_t

tmp_bans_hb, tmp_bans_tb = calc_tb_hb('value', tmp_bans, tmp_bans_h), calc_tb_hb('value', tmp_bans, tmp_bans_t)
tmp_bans_ratio_hb, tmp_bans_ratio_tb = calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_h_ratio), calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_t_ratio)

part3_subcontent1 ='\t封号量**{}**、环比<font color="{}">{}**{}**%</font>、同比<font color="{}">{}**{}%**</font>\n' \
'\t封号率**{}%%**、环比<font color="{}">{}**{}**%%</font>、同比<font color="{}">{}**{}%%**</font>'.format(
    tmp_bans,
    mapping_fontcolor(tmp_bans_hb), mapping_updown(tmp_bans_hb), round(abs(tmp_bans_hb) * 100, 1),
    mapping_fontcolor(tmp_bans_tb), mapping_updown(tmp_bans_tb), round(abs(tmp_bans_tb) * 100, 1),
    round(tmp_ban_ratio*10000,1),
    mapping_fontcolor(tmp_bans_ratio_hb), mapping_updown(tmp_bans_ratio_hb), round(abs(tmp_bans_ratio_hb) * 10000, 1),
    mapping_fontcolor(tmp_bans_ratio_tb), mapping_updown(tmp_bans_ratio_tb), round(abs(tmp_bans_ratio_tb) * 10000, 1)
)

part3_subtitle2 = '#### ② 剔除无关封号'
game_name, verify_type = '枪战王者', '剔除无关封号'
tmp_bans = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].bans).sum()
tmp_bans_h = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].bans).sum()
tmp_bans_t = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].bans).sum()

tmp_orders = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].orders).sum()
tmp_orders_h = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].orders).sum()
tmp_orders_t = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].orders).sum()

tmp_ban_ratio = tmp_bans / tmp_orders
tmp_bans_h_ratio = tmp_bans_h / tmp_orders_h
tmp_bans_t_ratio = tmp_bans_t / tmp_orders_t

tmp_bans_hb, tmp_bans_tb = calc_tb_hb('value', tmp_bans, tmp_bans_h), calc_tb_hb('value', tmp_bans, tmp_bans_t)
tmp_bans_ratio_hb, tmp_bans_ratio_tb = calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_h_ratio), calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_t_ratio)

part3_subcontent2 ='\t封号量**{}**、环比<font color="{}">{}**{}**%</font>、同比<font color="{}">{}**{}%**</font>\n' \
'\t封号率**{}%%**、环比<font color="{}">{}**{}**%%</font>、同比<font color="{}">{}**{}%%**</font>'.format(
    tmp_bans,
    mapping_fontcolor(tmp_bans_hb), mapping_updown(tmp_bans_hb), round(abs(tmp_bans_hb) * 100, 1),
    mapping_fontcolor(tmp_bans_tb), mapping_updown(tmp_bans_tb), round(abs(tmp_bans_tb) * 100, 1),
    round(tmp_ban_ratio*10000,1),
    mapping_fontcolor(tmp_bans_ratio_hb), mapping_updown(tmp_bans_ratio_hb), round(abs(tmp_bans_ratio_hb) * 10000, 1),
    mapping_fontcolor(tmp_bans_ratio_tb), mapping_updown(tmp_bans_ratio_tb), round(abs(tmp_bans_ratio_tb) * 10000, 1)
)



part4_title = '## **四、和平精英**'
part4_subtitle1 = '#### ① 包含无关封号'
game_name, verify_type = '和平精英', '包含无关封号'
tmp_bans = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].bans).sum()
tmp_bans_h = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].bans).sum()
tmp_bans_t = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].bans).sum()

tmp_orders = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].orders).sum()
tmp_orders_h = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].orders).sum()
tmp_orders_t = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].orders).sum()

tmp_ban_ratio = tmp_bans / tmp_orders
tmp_bans_h_ratio = tmp_bans_h / tmp_orders_h
tmp_bans_t_ratio = tmp_bans_t / tmp_orders_t

tmp_bans_hb, tmp_bans_tb = calc_tb_hb('value', tmp_bans, tmp_bans_h), calc_tb_hb('value', tmp_bans, tmp_bans_t)
tmp_bans_ratio_hb, tmp_bans_ratio_tb = calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_h_ratio), calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_t_ratio)

part4_subcontent1 ='\t封号量**{}**、环比<font color="{}">{}**{}**%</font>、同比<font color="{}">{}**{}%**</font>\n' \
'\t封号率**{}%%**、环比<font color="{}">{}**{}**%%</font>、同比<font color="{}">{}**{}%%**</font>'.format(
    tmp_bans,
    mapping_fontcolor(tmp_bans_hb), mapping_updown(tmp_bans_hb), round(abs(tmp_bans_hb) * 100, 1),
    mapping_fontcolor(tmp_bans_tb), mapping_updown(tmp_bans_tb), round(abs(tmp_bans_tb) * 100, 1),
    round(tmp_ban_ratio*10000,1),
    mapping_fontcolor(tmp_bans_ratio_hb), mapping_updown(tmp_bans_ratio_hb), round(abs(tmp_bans_ratio_hb) * 10000, 1),
    mapping_fontcolor(tmp_bans_ratio_tb), mapping_updown(tmp_bans_ratio_tb), round(abs(tmp_bans_ratio_tb) * 10000, 1)
)


part4_subtitle2 = '#### ② 剔除无关封号'
game_name, verify_type = '和平精英', '剔除无关封号'
tmp_bans = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].bans).sum()
tmp_bans_h = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].bans).sum()
tmp_bans_t = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].bans).sum()

tmp_orders = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].orders).sum()
tmp_orders_h = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].orders).sum()
tmp_orders_t = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].orders).sum()

tmp_ban_ratio = tmp_bans / tmp_orders
tmp_bans_h_ratio = tmp_bans_h / tmp_orders_h
tmp_bans_t_ratio = tmp_bans_t / tmp_orders_t

tmp_bans_hb, tmp_bans_tb = calc_tb_hb('value', tmp_bans, tmp_bans_h), calc_tb_hb('value', tmp_bans, tmp_bans_t)
tmp_bans_ratio_hb, tmp_bans_ratio_tb = calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_h_ratio), calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_t_ratio)

part4_subcontent2 ='\t封号量**{}**、环比<font color="{}">{}**{}**%</font>、同比<font color="{}">{}**{}%**</font>\n' \
'\t封号率**{}%%**、环比<font color="{}">{}**{}**%%</font>、同比<font color="{}">{}**{}%%**</font>'.format(
    tmp_bans,
    mapping_fontcolor(tmp_bans_hb), mapping_updown(tmp_bans_hb), round(abs(tmp_bans_hb) * 100, 1),
    mapping_fontcolor(tmp_bans_tb), mapping_updown(tmp_bans_tb), round(abs(tmp_bans_tb) * 100, 1),
    round(tmp_ban_ratio*10000,1),
    mapping_fontcolor(tmp_bans_ratio_hb), mapping_updown(tmp_bans_ratio_hb), round(abs(tmp_bans_ratio_hb) * 10000, 1),
    mapping_fontcolor(tmp_bans_ratio_tb), mapping_updown(tmp_bans_ratio_tb), round(abs(tmp_bans_ratio_tb) * 10000, 1)
)



part5_title = '## **五、端游**'
part5_subtitle1 = '#### ① 包含无关封号'
game_name, verify_type = '端游', '包含无关封号'
tmp_bans = null_series_default(tmp_df[(tmp_df.game_type == game_type) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].bans).sum()
tmp_bans_h = null_series_default(tmp_df[(tmp_df.game_type == game_type) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].bans).sum()
tmp_bans_t = null_series_default(tmp_df[(tmp_df.game_type == game_type) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].bans).sum()

tmp_orders = null_series_default(tmp_df[(tmp_df.game_type == game_type) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].orders).sum()
tmp_orders_h = null_series_default(tmp_df[(tmp_df.game_type == game_type) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].orders).sum()
tmp_orders_t = null_series_default(tmp_df[(tmp_df.game_type == game_type) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].orders).sum()

tmp_ban_ratio = tmp_bans / tmp_orders
tmp_bans_h_ratio = tmp_bans_h / tmp_orders_h
tmp_bans_t_ratio = tmp_bans_t / tmp_orders_t

tmp_bans_hb, tmp_bans_tb = calc_tb_hb('value', tmp_bans, tmp_bans_h), calc_tb_hb('value', tmp_bans, tmp_bans_t)
tmp_bans_ratio_hb, tmp_bans_ratio_tb = calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_h_ratio), calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_t_ratio)

part5_subcontent1 ='\t封号量**{}**、环比<font color="{}">{}**{}**%</font>、同比<font color="{}">{}**{}%**</font>\n' \
'\t封号率**{}%%**、环比<font color="{}">{}**{}**%%</font>、同比<font color="{}">{}**{}%%**</font>'.format(
    tmp_bans,
    mapping_fontcolor(tmp_bans_hb), mapping_updown(tmp_bans_hb), round(abs(tmp_bans_hb) * 100, 1),
    mapping_fontcolor(tmp_bans_tb), mapping_updown(tmp_bans_tb), round(abs(tmp_bans_tb) * 100, 1),
    round(tmp_ban_ratio*10000,1),
    mapping_fontcolor(tmp_bans_ratio_hb), mapping_updown(tmp_bans_ratio_hb), round(abs(tmp_bans_ratio_hb) * 10000, 1),
    mapping_fontcolor(tmp_bans_ratio_tb), mapping_updown(tmp_bans_ratio_tb), round(abs(tmp_bans_ratio_tb) * 10000, 1)
)

part5_subtitle2 = '#### ② 剔除无关封号'
game_name, verify_type = '端游', '剔除无关封号'
tmp_bans = null_series_default(tmp_df[(tmp_df.game_type == game_type) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].bans).sum()
tmp_bans_h = null_series_default(tmp_df[(tmp_df.game_type == game_type) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].bans).sum()
tmp_bans_t = null_series_default(tmp_df[(tmp_df.game_type == game_type) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].bans).sum()

tmp_orders = null_series_default(tmp_df[(tmp_df.game_type == game_type) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].orders).sum()
tmp_orders_h = null_series_default(tmp_df[(tmp_df.game_type == game_type) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].orders).sum()
tmp_orders_t = null_series_default(tmp_df[(tmp_df.game_type == game_type) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].orders).sum()

tmp_ban_ratio = tmp_bans / tmp_orders
tmp_bans_h_ratio = tmp_bans_h / tmp_orders_h
tmp_bans_t_ratio = tmp_bans_t / tmp_orders_t

tmp_bans_hb, tmp_bans_tb = calc_tb_hb('value', tmp_bans, tmp_bans_h), calc_tb_hb('value', tmp_bans, tmp_bans_t)
tmp_bans_ratio_hb, tmp_bans_ratio_tb = calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_h_ratio), calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_t_ratio)

part5_subcontent2 ='\t封号量**{}**、环比<font color="{}">{}**{}**%</font>、同比<font color="{}">{}**{}%**</font>\n' \
'\t封号率**{}%%**、环比<font color="{}">{}**{}**%%</font>、同比<font color="{}">{}**{}%%**</font>'.format(
    tmp_bans,
    mapping_fontcolor(tmp_bans_hb), mapping_updown(tmp_bans_hb), round(abs(tmp_bans_hb) * 100, 1),
    mapping_fontcolor(tmp_bans_tb), mapping_updown(tmp_bans_tb), round(abs(tmp_bans_tb) * 100, 1),
    round(tmp_ban_ratio*10000,1),
    mapping_fontcolor(tmp_bans_ratio_hb), mapping_updown(tmp_bans_ratio_hb), round(abs(tmp_bans_ratio_hb) * 10000, 1),
    mapping_fontcolor(tmp_bans_ratio_tb), mapping_updown(tmp_bans_ratio_tb), round(abs(tmp_bans_ratio_tb) * 10000, 1)
)



part6_title = '## **六、穿越火线**'
part6_subtitle1 = '#### ① 包含无关封号'
game_name, verify_type = '穿越火线', '包含无关封号'
tmp_bans = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].bans).sum()
tmp_bans_h = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].bans).sum()
tmp_bans_t = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].bans).sum()

tmp_orders = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].orders).sum()
tmp_orders_h = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].orders).sum()
tmp_orders_t = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].orders).sum()

tmp_ban_ratio = tmp_bans / tmp_orders
tmp_bans_h_ratio = tmp_bans_h / tmp_orders_h
tmp_bans_t_ratio = tmp_bans_t / tmp_orders_t

tmp_bans_hb, tmp_bans_tb = calc_tb_hb('value', tmp_bans, tmp_bans_h), calc_tb_hb('value', tmp_bans, tmp_bans_t)
tmp_bans_ratio_hb, tmp_bans_ratio_tb = calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_h_ratio), calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_t_ratio)

part6_subcontent1 ='\t封号量**{}**、环比<font color="{}">{}**{}**%</font>、同比<font color="{}">{}**{}%**</font>\n' \
'\t封号率**{}%%**、环比<font color="{}">{}**{}**%%</font>、同比<font color="{}">{}**{}%%**</font>'.format(
    tmp_bans,
    mapping_fontcolor(tmp_bans_hb), mapping_updown(tmp_bans_hb), round(abs(tmp_bans_hb) * 100, 1),
    mapping_fontcolor(tmp_bans_tb), mapping_updown(tmp_bans_tb), round(abs(tmp_bans_tb) * 100, 1),
    round(tmp_ban_ratio*10000,1),
    mapping_fontcolor(tmp_bans_ratio_hb), mapping_updown(tmp_bans_ratio_hb), round(abs(tmp_bans_ratio_hb) * 10000, 1),
    mapping_fontcolor(tmp_bans_ratio_tb), mapping_updown(tmp_bans_ratio_tb), round(abs(tmp_bans_ratio_tb) * 10000, 1)
)



part6_subtitle2 = '#### ② 剔除无关封号'
game_name, verify_type = '穿越火线', '剔除无关封号'
tmp_bans = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].bans).sum()
tmp_bans_h = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].bans).sum()
tmp_bans_t = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].bans).sum()

tmp_orders = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].orders).sum()
tmp_orders_h = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].orders).sum()
tmp_orders_t = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].orders).sum()

tmp_ban_ratio = tmp_bans / tmp_orders
tmp_bans_h_ratio = tmp_bans_h / tmp_orders_h
tmp_bans_t_ratio = tmp_bans_t / tmp_orders_t

tmp_bans_hb, tmp_bans_tb = calc_tb_hb('value', tmp_bans, tmp_bans_h), calc_tb_hb('value', tmp_bans, tmp_bans_t)
tmp_bans_ratio_hb, tmp_bans_ratio_tb = calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_h_ratio), calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_t_ratio)

part6_subcontent2 ='\t封号量**{}**、环比<font color="{}">{}**{}**%</font>、同比<font color="{}">{}**{}%**</font>\n' \
'\t封号率**{}%%**、环比<font color="{}">{}**{}**%%</font>、同比<font color="{}">{}**{}%%**</font>'.format(
    tmp_bans,
    mapping_fontcolor(tmp_bans_hb), mapping_updown(tmp_bans_hb), round(abs(tmp_bans_hb) * 100, 1),
    mapping_fontcolor(tmp_bans_tb), mapping_updown(tmp_bans_tb), round(abs(tmp_bans_tb) * 100, 1),
    round(tmp_ban_ratio*10000,1),
    mapping_fontcolor(tmp_bans_ratio_hb), mapping_updown(tmp_bans_ratio_hb), round(abs(tmp_bans_ratio_hb) * 10000, 1),
    mapping_fontcolor(tmp_bans_ratio_tb), mapping_updown(tmp_bans_ratio_tb), round(abs(tmp_bans_ratio_tb) * 10000, 1)
)



part7_title = '## **七、英雄联盟**'
part7_subtitle1 = '#### ① 包含无关封号'
game_name, verify_type = '英雄联盟', '包含无关封号'
tmp_bans = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].bans).sum()
tmp_bans_h = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].bans).sum()
tmp_bans_t = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].bans).sum()

tmp_orders = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].orders).sum()
tmp_orders_h = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].orders).sum()
tmp_orders_t = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].orders).sum()

tmp_ban_ratio = tmp_bans / tmp_orders
tmp_bans_h_ratio = tmp_bans_h / tmp_orders_h
tmp_bans_t_ratio = tmp_bans_t / tmp_orders_t

tmp_bans_hb, tmp_bans_tb = calc_tb_hb('value', tmp_bans, tmp_bans_h), calc_tb_hb('value', tmp_bans, tmp_bans_t)
tmp_bans_ratio_hb, tmp_bans_ratio_tb = calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_h_ratio), calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_t_ratio)

part7_subcontent1 ='\t封号量**{}**、环比<font color="{}">{}**{}**%</font>、同比<font color="{}">{}**{}%**</font>\n' \
'\t封号率**{}%%**、环比<font color="{}">{}**{}**%%</font>、同比<font color="{}">{}**{}%%**</font>'.format(
    tmp_bans,
    mapping_fontcolor(tmp_bans_hb), mapping_updown(tmp_bans_hb), round(abs(tmp_bans_hb) * 100, 1),
    mapping_fontcolor(tmp_bans_tb), mapping_updown(tmp_bans_tb), round(abs(tmp_bans_tb) * 100, 1),
    round(tmp_ban_ratio*10000,1),
    mapping_fontcolor(tmp_bans_ratio_hb), mapping_updown(tmp_bans_ratio_hb), round(abs(tmp_bans_ratio_hb) * 10000, 1),
    mapping_fontcolor(tmp_bans_ratio_tb), mapping_updown(tmp_bans_ratio_tb), round(abs(tmp_bans_ratio_tb) * 10000, 1)
)



part7_subtitle2 = '#### ② 剔除无关封号'
game_name, verify_type = '英雄联盟', '剔除无关封号'
tmp_bans = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].bans).sum()
tmp_bans_h = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].bans).sum()
tmp_bans_t = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].bans).sum()

tmp_orders = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].orders).sum()
tmp_orders_h = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].orders).sum()
tmp_orders_t = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].orders).sum()

tmp_ban_ratio = tmp_bans / tmp_orders
tmp_bans_h_ratio = tmp_bans_h / tmp_orders_h
tmp_bans_t_ratio = tmp_bans_t / tmp_orders_t

tmp_bans_hb, tmp_bans_tb = calc_tb_hb('value', tmp_bans, tmp_bans_h), calc_tb_hb('value', tmp_bans, tmp_bans_t)
tmp_bans_ratio_hb, tmp_bans_ratio_tb = calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_h_ratio), calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_t_ratio)

part7_subcontent2 ='\t封号量**{}**、环比<font color="{}">{}**{}**%</font>、同比<font color="{}">{}**{}%**</font>\n' \
'\t封号率**{}%%**、环比<font color="{}">{}**{}**%%</font>、同比<font color="{}">{}**{}%%**</font>'.format(
    tmp_bans,
    mapping_fontcolor(tmp_bans_hb), mapping_updown(tmp_bans_hb), round(abs(tmp_bans_hb) * 100, 1),
    mapping_fontcolor(tmp_bans_tb), mapping_updown(tmp_bans_tb), round(abs(tmp_bans_tb) * 100, 1),
    round(tmp_ban_ratio*10000,1),
    mapping_fontcolor(tmp_bans_ratio_hb), mapping_updown(tmp_bans_ratio_hb), round(abs(tmp_bans_ratio_hb) * 10000, 1),
    mapping_fontcolor(tmp_bans_ratio_tb), mapping_updown(tmp_bans_ratio_tb), round(abs(tmp_bans_ratio_tb) * 10000, 1)
)




part8_title = '## **八、逆战**'
part8_subtitle1 = '#### ① 包含无关封号'
game_name, verify_type = '逆战', '包含无关封号'
tmp_bans = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].bans).sum()
tmp_bans_h = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].bans).sum()
tmp_bans_t = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].bans).sum()

tmp_orders = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].orders).sum()
tmp_orders_h = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].orders).sum()
tmp_orders_t = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].orders).sum()

tmp_ban_ratio = tmp_bans / tmp_orders
tmp_bans_h_ratio = tmp_bans_h / tmp_orders_h
tmp_bans_t_ratio = tmp_bans_t / tmp_orders_t

tmp_bans_hb, tmp_bans_tb = calc_tb_hb('value', tmp_bans, tmp_bans_h), calc_tb_hb('value', tmp_bans, tmp_bans_t)
tmp_bans_ratio_hb, tmp_bans_ratio_tb = calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_h_ratio), calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_t_ratio)

part8_subcontent1 ='\t封号量**{}**、环比<font color="{}">{}**{}**%</font>、同比<font color="{}">{}**{}%**</font>\n' \
'\t封号率**{}%%**、环比<font color="{}">{}**{}**%%</font>、同比<font color="{}">{}**{}%%**</font>'.format(
    tmp_bans,
    mapping_fontcolor(tmp_bans_hb), mapping_updown(tmp_bans_hb), round(abs(tmp_bans_hb) * 100, 1),
    mapping_fontcolor(tmp_bans_tb), mapping_updown(tmp_bans_tb), round(abs(tmp_bans_tb) * 100, 1),
    round(tmp_ban_ratio*10000,1),
    mapping_fontcolor(tmp_bans_ratio_hb), mapping_updown(tmp_bans_ratio_hb), round(abs(tmp_bans_ratio_hb) * 10000, 1),
    mapping_fontcolor(tmp_bans_ratio_tb), mapping_updown(tmp_bans_ratio_tb), round(abs(tmp_bans_ratio_tb) * 10000, 1)
)

part8_subtitle2 = '#### ② 剔除无关封号'
game_name, verify_type = '逆战', '剔除无关封号'
tmp_bans = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].bans).sum()
tmp_bans_h = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].bans).sum()
tmp_bans_t = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].bans).sum()

tmp_orders = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].orders).sum()
tmp_orders_h = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].orders).sum()
tmp_orders_t = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].orders).sum()

tmp_ban_ratio = tmp_bans / tmp_orders
tmp_bans_h_ratio = tmp_bans_h / tmp_orders_h
tmp_bans_t_ratio = tmp_bans_t / tmp_orders_t

tmp_bans_hb, tmp_bans_tb = calc_tb_hb('value', tmp_bans, tmp_bans_h), calc_tb_hb('value', tmp_bans, tmp_bans_t)
tmp_bans_ratio_hb, tmp_bans_ratio_tb = calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_h_ratio), calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_t_ratio)

part8_subcontent2 ='\t封号量**{}**、环比<font color="{}">{}**{}**%</font>、同比<font color="{}">{}**{}%**</font>\n' \
'\t封号率**{}%%**、环比<font color="{}">{}**{}**%%</font>、同比<font color="{}">{}**{}%%**</font>'.format(
    tmp_bans,
    mapping_fontcolor(tmp_bans_hb), mapping_updown(tmp_bans_hb), round(abs(tmp_bans_hb) * 100, 1),
    mapping_fontcolor(tmp_bans_tb), mapping_updown(tmp_bans_tb), round(abs(tmp_bans_tb) * 100, 1),
    round(tmp_ban_ratio*10000,1),
    mapping_fontcolor(tmp_bans_ratio_hb), mapping_updown(tmp_bans_ratio_hb), round(abs(tmp_bans_ratio_hb) * 10000, 1),
    mapping_fontcolor(tmp_bans_ratio_tb), mapping_updown(tmp_bans_ratio_tb), round(abs(tmp_bans_ratio_tb) * 10000, 1)
)



part9_title = '## **九、绝地求生**'
part9_subtitle1 = '#### ① 包含无关封号'
game_name, verify_type = '绝地求生', '包含无关封号'
tmp_bans = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].bans).sum()
tmp_bans_h = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].bans).sum()
tmp_bans_t = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].bans).sum()

tmp_orders = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].orders).sum()
tmp_orders_h = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].orders).sum()
tmp_orders_t = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].orders).sum()

tmp_ban_ratio = tmp_bans / tmp_orders
tmp_bans_h_ratio = tmp_bans_h / tmp_orders_h
tmp_bans_t_ratio = tmp_bans_t / tmp_orders_t

tmp_bans_hb, tmp_bans_tb = calc_tb_hb('value', tmp_bans, tmp_bans_h), calc_tb_hb('value', tmp_bans, tmp_bans_t)
tmp_bans_ratio_hb, tmp_bans_ratio_tb = calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_h_ratio), calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_t_ratio)

part9_subcontent1 ='\t封号量**{}**、环比<font color="{}">{}**{}**%</font>、同比<font color="{}">{}**{}%**</font>\n' \
'\t封号率**{}%%**、环比<font color="{}">{}**{}**%%</font>、同比<font color="{}">{}**{}%%**</font>'.format(
    tmp_bans,
    mapping_fontcolor(tmp_bans_hb), mapping_updown(tmp_bans_hb), round(abs(tmp_bans_hb) * 100, 1),
    mapping_fontcolor(tmp_bans_tb), mapping_updown(tmp_bans_tb), round(abs(tmp_bans_tb) * 100, 1),
    round(tmp_ban_ratio*10000,1),
    mapping_fontcolor(tmp_bans_ratio_hb), mapping_updown(tmp_bans_ratio_hb), round(abs(tmp_bans_ratio_hb) * 10000, 1),
    mapping_fontcolor(tmp_bans_ratio_tb), mapping_updown(tmp_bans_ratio_tb), round(abs(tmp_bans_ratio_tb) * 10000, 1)
)

part9_subtitle2 = '#### ② 剔除无关封号'
game_name, verify_type = '绝地求生', '剔除无关封号'
tmp_bans = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].bans).sum()
tmp_bans_h = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].bans).sum()
tmp_bans_t = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].bans).sum()

tmp_orders = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == current_day)].orders).sum()
tmp_orders_h = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_h)].orders).sum()
tmp_orders_t = null_series_default(tmp_df[(tmp_df.game_name == game_name) & (tmp_df.verify_type == verify_type) & (tmp_df.part_day == part_day_t)].orders).sum()

tmp_ban_ratio = tmp_bans / tmp_orders
tmp_bans_h_ratio = tmp_bans_h / tmp_orders_h
tmp_bans_t_ratio = tmp_bans_t / tmp_orders_t

tmp_bans_hb, tmp_bans_tb = calc_tb_hb('value', tmp_bans, tmp_bans_h), calc_tb_hb('value', tmp_bans, tmp_bans_t)
tmp_bans_ratio_hb, tmp_bans_ratio_tb = calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_h_ratio), calc_tb_hb('ratio', tmp_ban_ratio, tmp_bans_t_ratio)

part9_subcontent2 ='\t封号量**{}**、环比<font color="{}">{}**{}**%</font>、同比<font color="{}">{}**{}%**</font>\n' \
'\t封号率**{}%%**、环比<font color="{}">{}**{}**%%</font>、同比<font color="{}">{}**{}%%**</font>'.format(
    tmp_bans,
    mapping_fontcolor(tmp_bans_hb), mapping_updown(tmp_bans_hb), round(abs(tmp_bans_hb) * 100, 1),
    mapping_fontcolor(tmp_bans_tb), mapping_updown(tmp_bans_tb), round(abs(tmp_bans_tb) * 100, 1),
    round(tmp_ban_ratio*10000,1),
    mapping_fontcolor(tmp_bans_ratio_hb), mapping_updown(tmp_bans_ratio_hb), round(abs(tmp_bans_ratio_hb) * 10000, 1),
    mapping_fontcolor(tmp_bans_ratio_tb), mapping_updown(tmp_bans_ratio_tb), round(abs(tmp_bans_ratio_tb) * 10000, 1)
)

test_webhook = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=231ae38a-3d31-4635-80d2-800029963832"
prod_webhook = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=81dbf4f3-ac3b-427e-a477-6d525e545fb0"
result_content1 = title + '\n' + '>' \
                 + part1_title + '\n' + part1_subtitle1 + '\n' + part1_subcontent1 + '\n' + part1_subtitle2 + '\n' + part1_subcontent2 + '\n'\
                 + part2_title + '\n' + part2_subtitle1 + '\n' + part2_subcontent1 + '\n' + part2_subtitle2 + '\n' + part2_subcontent2 + '\n' \
                 + part3_title + '\n' + part3_subtitle1 + '\n' + part3_subcontent1 + '\n' + part3_subtitle2 + '\n' + part3_subcontent2 + '\n' \
                 + part4_title + '\n' + part4_subtitle1 + '\n' + part4_subcontent1 + '\n' + part4_subtitle2 + '\n' + part4_subcontent2 + '\n'
result_content2 = title + '\n' + '>' \
                 + part5_title + '\n' + part5_subtitle1 + '\n' + part5_subcontent1 + '\n' + part5_subtitle2 + '\n' + part5_subcontent2 + '\n' \
                 + part6_title + '\n' + part6_subtitle1 + '\n' + part6_subcontent1 + '\n' + part6_subtitle2 + '\n' + part6_subcontent2 + '\n' \
                 + part7_title + '\n' + part7_subtitle1 + '\n' + part7_subcontent1 + '\n' + part7_subtitle2 + '\n' + part7_subcontent2 + '\n' \
                 + part8_title + '\n' + part8_subtitle1 + '\n' + part8_subcontent1 + '\n' + part8_subtitle2 + '\n' + part8_subcontent2 + '\n' \
                 + part9_title + '\n' + part9_subtitle1 + '\n' + part9_subcontent1 + '\n' + part9_subtitle2 + '\n' + part9_subcontent2 + '\n'

work_wxrobot(result_content1,prod_webhook)
# work_wxrobot(result_content2,prod_webhook)