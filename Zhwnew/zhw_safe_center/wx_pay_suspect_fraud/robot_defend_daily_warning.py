# _*_coding:utf-8 _*_

# @Time      : 2023/7/6  15:34
# @Author    : An
# @File      : gg_fenghao_orders_group.py
# @Software  : 日常指标预警

import time, datetime, configparser, warnings, math, platform , psycopg2
import sys
import pandas as pd
import pymysql
from pyhive import presto
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

# presto数据库
host = cf.get("hive_presto_hive", "host")
username = cf.get("hive_presto_hive", "username")
port = cf.get("hive_presto_hive", "port")
schema = cf.get("hive_presto_hive", "schema")
catalog = cf.get("hive_presto_hive", "catalog")
presto_db = presto.connect(host=host, port=port, username=username, schema=schema, catalog=catalog)

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
        return "info"
    elif numeric < 0:
        return "warning"
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

current_time = (datetime.datetime.now() - datetime.timedelta(days=0)).strftime('%Y-%m-%d %H:%M:%S')
current_day = (datetime.datetime.now() - datetime.timedelta(days=0)).strftime('%Y-%m-%d')

# 数据窗口日期
start_day = (datetime.datetime.now() - datetime.timedelta(days=14)).strftime('%Y-%m-%d')
end_day = (datetime.datetime.now() - datetime.timedelta(days=0)).strftime('%Y-%m-%d')

part_day_h = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
part_day_t = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')


# 预警模块数量
warning_modules = 0

_current_day = (datetime.datetime.now() - datetime.timedelta(days=0)).strftime('%m%d')
_now = (datetime.datetime.now() - datetime.timedelta(days=0)).strftime('%H:%M')
title = '# ** {} 当日安防-异常数据 实时预警 **({})'.format(_current_day,  _now)

# p1 封号率 按 日期、游戏、类型
banratio_title = '#### 1 游戏封号率'
banratio_content = ''

banratio_dim_day_game = '''
    select 
        coalesce(t1.part_day,t2.part_day) as part_day,
        coalesce(t1.title,t2.title) as game_name,
        coalesce(t1.category,t2.category) as category,
        max(t1.orders) as orders,
        coalesce(max(t2.bans),0) as bans
    from
    (
        select 
            a.part_day,
            b.title,
            case when b.categoryid = 1 then '手游' when b.categoryid = 2 then '端游' else '其他' end category,
            count(distinct hid) as hids,
            count(a.id) as orders
        from ods_zhw.zhw_dingdan a
        left join ods_zhw.zhw_game_info b 
        on a.gameid = b.id
        where a.part_day between '{0}' and '{1}' 
        and a.gameid in (443,446,683,11,17,24,581)
        group by 1,2,3
    ) t1
    full join
    (
        select 
            to_char(a.start_time,'yyyy-mm-dd') as part_day,
            b.title,
            case when b.categoryid = 1 then '手游' when b.categoryid = 2 then '端游' else '其他' end category,
            count(distinct act_zh) as bans
        from ods_zhw.zhw_hao_lock_details a 
        left join ods_zhw.zhw_game_info b 
        on a.game_id = b.id
        where extract(epoch from a.end_time) - extract(epoch from a.start_time)>7*24*3600
        and to_char(a.start_time,'yyyy-mm-dd') between '{0}' and '{1}' 
        and a.game_id in (443,446,683,11,17,24,581)
        group by 1,2,3 
    ) t2 
    on t1.part_day = t2.part_day and t1.title=t2.title and t1.category = t2.category
    group by 1,2,3
'''.format(start_day,end_day)
tmp_df = pd.read_sql(banratio_dim_day_game, con=holo_cnx) # p1 封号率 按 日期、游戏、类型
if not len(tmp_df):
    banratio_content = '\t无异常\n'
else:
    data_types = {
        'part_day': str, 'game_name': str, 'category': str, 'orders': int, 'bans': int
    }
    tmp_df = tmp_df.astype(data_types)

    dim_type_list = []
    bans_list,orders_list = [], []
    bans_h_list,orders_h_list = [], []
    bans_t_list,orders_t_list = [], []

    dim_type = ['手游','端游']
    for _type in dim_type:
        dim_type_list.append(_type)
        bans_list.append(null_series_default(tmp_df[(tmp_df.category== _type) & (tmp_df.part_day == current_day)].bans).sum())
        orders_list.append(null_series_default(tmp_df[(tmp_df.category== _type) & (tmp_df.part_day == current_day)].orders).sum())
        bans_h_list.append(null_series_default(tmp_df[(tmp_df.category== _type) & (tmp_df.part_day == part_day_h)].bans).sum())
        orders_h_list.append(null_series_default(tmp_df[(tmp_df.category== _type) & (tmp_df.part_day == part_day_h)].orders).sum())
        bans_t_list.append(null_series_default(tmp_df[(tmp_df.category== _type) & (tmp_df.part_day == part_day_t)].bans).sum())
        orders_t_list.append(null_series_default(tmp_df[(tmp_df.category== _type) & (tmp_df.part_day == part_day_t)].orders).sum())

    game_name = ['王者荣耀','和平精英','枪战王者','穿越火线','英雄联盟','逆战','绝地求生']
    for _game in game_name:
        dim_type_list.append(_game)
        bans_list.append(null_series_default(tmp_df[(tmp_df.game_name== _game) & (tmp_df.part_day == current_day)].bans).sum())
        orders_list.append(null_series_default(tmp_df[(tmp_df.game_name== _game) & (tmp_df.part_day == current_day)].orders).sum())
        bans_h_list.append(null_series_default(tmp_df[(tmp_df.game_name== _game) & (tmp_df.part_day == part_day_h)].bans).sum())
        orders_h_list.append(null_series_default(tmp_df[(tmp_df.game_name== _game) & (tmp_df.part_day == part_day_h)].orders).sum())
        bans_t_list.append(null_series_default(tmp_df[(tmp_df.game_name== _game) & (tmp_df.part_day == part_day_t)].bans).sum())
        orders_t_list.append(null_series_default(tmp_df[(tmp_df.game_name== _game) & (tmp_df.part_day == part_day_t)].orders).sum())

    for idx,item in enumerate(dim_type_list):
        tmp_bans, tmp_h_bans, tmp_t_bans=bans_list[idx], bans_h_list[idx], bans_t_list[idx]
        tmp_bans_hb, tmp_bans_tb = calc_tb_hb('value',bans_list[idx],bans_h_list[idx]), calc_tb_hb('value',bans_list[idx],bans_t_list[idx])

        tmp_bans_ratio = bans_list[idx]/orders_list[idx]
        tmp_bans_h_ratio = bans_h_list[idx]/orders_h_list[idx]
        tmp_bans_t_ratio = bans_t_list[idx]/orders_t_list[idx]

        tmp_bans_ratio_hb, tmp_bans_ratio_tb = calc_tb_hb('ratio', tmp_bans_ratio, tmp_bans_h_ratio), calc_tb_hb('ratio', tmp_bans_ratio, tmp_bans_t_ratio)

        if ((tmp_bans >= 10 and tmp_bans_hb >= 2) or (tmp_bans >= 10 and tmp_bans_tb >= 2) or \
                (tmp_bans >= 5 and tmp_bans_ratio > tmp_bans_h_ratio * 1.5) or (tmp_bans >= 5 and tmp_bans_ratio > tmp_bans_t_ratio * 1.5)):

            warning_modules += 1

            banratio_content = '{}**\<{}\>**：\n' \
                               '\t封号率 {}%%、环比<font color="{}">{}{}%%</font>、同比<font color="{}">{}{}%%</font>\n' \
                               '\t封号数 {}、环比<font color="{}">{}{}%</font>、同比<font color="{}">{}{}%</font>\n'.format(
                banratio_content, item,
                round(tmp_bans_ratio * 10000, 1),
                mapping_fontcolor(tmp_bans_ratio_hb), mapping_updown(tmp_bans_ratio_hb), str(round(abs(tmp_bans_ratio_hb) * 10000, 1)),
                mapping_fontcolor(tmp_bans_ratio_tb), mapping_updown(tmp_bans_ratio_tb), str(round(abs(tmp_bans_ratio_tb) * 10000, 1)),
                tmp_bans,
                mapping_fontcolor(tmp_bans_hb), mapping_updown(tmp_bans_hb), str(round(abs(tmp_bans_hb) * 100, 1)),
                mapping_fontcolor(tmp_bans_tb), mapping_updown(tmp_bans_tb), str(round(abs(tmp_bans_tb) * 100, 1))
            )

    if banratio_content == '':
        banratio_content = '\t无异常\n'


# p2 封号数 按 日期、游戏、类型
interval_bans_title = '#### 2 游戏封号数(3日，7日以上)'
interval_bans_content = ''

bans_dim_day_game = '''
	select 
		to_char(a.start_time,'yyyy-mm-dd') as part_day,
		b.title as game_name,
		case when b.categoryid = 1 then '手游' when b.categoryid = 2 then '端游' else '其他' end category,
		coalesce(count(distinct case when a.lock_days=3 then act_zh end),0) as eq_3d_bans,
		coalesce(count(distinct case when a.lock_days>3 then act_zh end),0) as gt_7d_bans
	from ods_zhw.zhw_hao_lock_details a 
	left join ods_zhw.zhw_game_info b 
	on a.game_id = b.id
	where true 
	and (lock_days=3 or lock_days>7)
	and to_char(a.start_time,'yyyy-mm-dd') between '{0}' and '{1}' 
	and a.game_id in (443,446,683,11,17,24,581)
	group by 1,2,3
'''.format(start_day,end_day)
tmp_df = pd.read_sql(bans_dim_day_game, con=holo_cnx) # p1 封号率 按 日期、游戏、类型
if not len(tmp_df):
    interval_bans_content = '\t无异常\n'
else:
    data_types = {
        'part_day': str, 'game_name': str, 'category': str, 'eq_3d_bans': int, 'gt_7d_bans': int
    }
    tmp_df = tmp_df.astype(data_types)

    dim_type_list = []
    gt_7d_bans_list,eq_3d_bans_list = [], []
    gt_7d_bans_h_list,eq_3d_bans_h_list = [], []
    gt_7d_bans_t_list,eq_3d_bans_t_list = [], []

    dim_type = ['手游','端游']
    for _type in dim_type:
        dim_type_list.append(_type)
        gt_7d_bans_list.append(null_series_default(tmp_df[(tmp_df.category== _type) & (tmp_df.part_day == current_day)].gt_7d_bans).sum())
        eq_3d_bans_list.append(null_series_default(tmp_df[(tmp_df.category== _type) & (tmp_df.part_day == current_day)].eq_3d_bans).sum())
        gt_7d_bans_h_list.append(null_series_default(tmp_df[(tmp_df.category== _type) & (tmp_df.part_day == part_day_h)].gt_7d_bans).sum())
        eq_3d_bans_h_list.append(null_series_default(tmp_df[(tmp_df.category== _type) & (tmp_df.part_day == part_day_h)].eq_3d_bans).sum())
        gt_7d_bans_t_list.append(null_series_default(tmp_df[(tmp_df.category== _type) & (tmp_df.part_day == part_day_t)].gt_7d_bans).sum())
        eq_3d_bans_t_list.append(null_series_default(tmp_df[(tmp_df.category== _type) & (tmp_df.part_day == part_day_t)].eq_3d_bans).sum())

    game_name = ['王者荣耀','和平精英','枪战王者','穿越火线','英雄联盟','逆战','绝地求生']
    for _game in game_name:
        dim_type_list.append(_game)
        gt_7d_bans_list.append(null_series_default(tmp_df[(tmp_df.game_name== _game) & (tmp_df.part_day == current_day)].gt_7d_bans).sum())
        eq_3d_bans_list.append(null_series_default(tmp_df[(tmp_df.game_name== _game) & (tmp_df.part_day == current_day)].eq_3d_bans).sum())
        gt_7d_bans_h_list.append(null_series_default(tmp_df[(tmp_df.game_name== _game) & (tmp_df.part_day == part_day_h)].gt_7d_bans).sum())
        eq_3d_bans_h_list.append(null_series_default(tmp_df[(tmp_df.game_name== _game) & (tmp_df.part_day == part_day_h)].eq_3d_bans).sum())
        gt_7d_bans_t_list.append(null_series_default(tmp_df[(tmp_df.game_name== _game) & (tmp_df.part_day == part_day_t)].gt_7d_bans).sum())
        eq_3d_bans_t_list.append(null_series_default(tmp_df[(tmp_df.game_name== _game) & (tmp_df.part_day == part_day_t)].eq_3d_bans).sum())

    for idx,item in enumerate(dim_type_list):
        tmp_eq_3d_bans, tmp_gt_7d_bans=eq_3d_bans_list[idx], gt_7d_bans_list[idx]
        tmp_eq_3d_bans_hb, tmp_eq_3d_bans_tb = calc_tb_hb('value',eq_3d_bans_list[idx],eq_3d_bans_h_list[idx]), calc_tb_hb('value',eq_3d_bans_list[idx],eq_3d_bans_t_list[idx])
        tmp_gt_7d_bans_hb, tmp_gt_7d_bans_tb = calc_tb_hb('value', gt_7d_bans_list[idx], gt_7d_bans_h_list[idx]), calc_tb_hb('value', gt_7d_bans_list[idx], gt_7d_bans_t_list[idx])

        if ((tmp_eq_3d_bans >= 10 and tmp_eq_3d_bans_hb >= 0.8) or (tmp_eq_3d_bans >= 10 and tmp_eq_3d_bans_tb >= 0.8)):

            warning_modules += 1

            interval_bans_content = '{}**\<{}\>**:\n' \
                                    '\t3日封号数 {}、环比<font color ="{}">{}{}%</font>、同比<font color ="{}">{}{}%</font>\n'.format(
                interval_bans_content, item,
                tmp_eq_3d_bans,
                mapping_fontcolor(tmp_eq_3d_bans_hb), mapping_updown(tmp_eq_3d_bans_hb),
                round(abs(tmp_eq_3d_bans_hb) * 100, 1),
                mapping_fontcolor(tmp_eq_3d_bans_tb), mapping_updown(tmp_eq_3d_bans_tb), round(abs(tmp_eq_3d_bans_tb) * 100, 1)
            )

        if ((tmp_gt_7d_bans >= 10 and tmp_gt_7d_bans_hb >= 0.8) or (tmp_gt_7d_bans >= 10 and tmp_gt_7d_bans_tb >= 0.8)):
            interval_bans_content = '{}**\<{}\>**:\n' \
                                    '\t7日封号数 {}、环比<font color ="{}">{}{}%</font>、同比<font color ="{}">{}{}%</font>\n'.format(
                interval_bans_content, item,
                tmp_gt_7d_bans,
                mapping_fontcolor(tmp_gt_7d_bans_hb), mapping_updown(tmp_gt_7d_bans_hb),
                round(abs(tmp_gt_7d_bans_hb) * 100, 1),
                mapping_fontcolor(tmp_gt_7d_bans_tb), mapping_updown(tmp_gt_7d_bans_tb), round(abs(tmp_gt_7d_bans_tb) * 100, 1)
            )

    if interval_bans_content == '':
        interval_bans_content = '\t无异常\n'

# p3 封号数 按 日期、游戏、原因
bans_reason_title = '#### 3 游戏封号原因(3日，7日以上)'
bans_reason_content = ''

bans_dim_day_game_reason = '''
    select 
        to_char(a.create_time,'yyyy-mm-dd') as part_day,
        b.title as game_name,
        case when b.categoryid = 1 then '手游' when b.categoryid = 2 then '端游' else '其他' end category,
        a.reason,
        count(distinct case when a.duration/60/60/24 = 3 then c.game_account end) as eq_3d_bans,
        count(distinct case when a.duration/60/60/24 >7 then c.game_account end) as gt_7d_bans
    from ods_zhw.game_cheat_account_record a 
    left join ods_zhw.zhw_game_info b 
    on a.game_id = b.id 
    join ods_zhw.game_cheat_account_info c 
    on a.game_account_id=c.id and a.game_id = c.game_id
    where true 
    and (a.fpt = a.pt or a.pt = -1)
    and a.game_id in (443,446,683,11,17,24,581)
    and a.type like '%%封号%%'
    and (a.duration/60/60/24 = 3 or a.duration/60/60/24>7)
    and a.create_time between cast('{0} 00:00:00' as timestamp) and cast('{1} 23:59:59' as timestamp)
    group by 1,2,3,4
'''.format(start_day,end_day)
tmp_df1 = pd.read_sql(bans_dim_day_game_reason, con=holo_cnx) # p1 封号率 按 日期、游戏、类型
if not len(tmp_df1):
    bans_reason_content = '\t无异常\n'
else:
    data_types = {
        'part_day': str, 'game_name': str, 'reason': str, 'eq_3d_bans': int, 'gt_7d_bans': int
    }
    tmp_df1 = tmp_df1.astype(data_types)
    tmp_df2 = tmp_df1.copy()
    tmp_df2.set_index(['part_day', 'game_name', 'category', 'reason'], inplace=True)
    tmp_df2['eq_3d_bans_sum'] = tmp_df2.eq_3d_bans.sum(level=['part_day', 'game_name', 'category'])
    tmp_df2['gt_7d_bans_sum'] = tmp_df2.gt_7d_bans.sum(level=['part_day', 'game_name', 'category'])
    tmp_df2.sort_values(by = ['part_day','game_name'],inplace=True)

    tmp_df2['eq_3d_bans_rate'] = tmp_df2.eq_3d_bans / tmp_df2.eq_3d_bans_sum
    tmp_df2['gt_7d_bans_rate'] = tmp_df2.gt_7d_bans / tmp_df2.gt_7d_bans_sum
    tmp_df2.fillna(0,inplace=True)

    tmp_df = tmp_df2.reset_index(drop=False)

    dim_type_list = []
    reason_list = []
    gt_7d_bans_rate_list,eq_3d_bans_rate_list = [], []
    gt_7d_bans_rate_h_list,eq_3d_bans_rate_h_list = [], []
    gt_7d_bans_rate_t_list,eq_3d_bans_rate_t_list = [], []

    game_name = ['王者荣耀','和平精英','枪战王者','穿越火线','英雄联盟','逆战','绝地求生']
    game_name = ['王者荣耀','和平精英','枪战王者','穿越火线']

    for _game in game_name:
        game_reason_set = set(tmp_df[(tmp_df.game_name == _game) & (tmp_df.part_day == current_day)].reason)
        for _reason in game_reason_set:
            dim_type_list.append(_game)
            reason_list.append(_reason)
            gt_7d_bans_rate_list.append(null_series_default(tmp_df[(tmp_df.game_name == _game) & (tmp_df.part_day == current_day) & (tmp_df.reason == _reason)].gt_7d_bans_rate).sum())
            eq_3d_bans_rate_list.append(null_series_default(tmp_df[(tmp_df.game_name == _game) & (tmp_df.part_day == current_day) & (tmp_df.reason == _reason)].eq_3d_bans_rate).sum())
            gt_7d_bans_rate_h_list.append(null_series_default(tmp_df[(tmp_df.game_name == _game) & (tmp_df.part_day == part_day_h) & (tmp_df.reason == _reason)].gt_7d_bans_rate).sum())
            eq_3d_bans_rate_h_list.append(null_series_default(tmp_df[(tmp_df.game_name == _game) & (tmp_df.part_day == part_day_h) & (tmp_df.reason == _reason)].eq_3d_bans_rate).sum())
            gt_7d_bans_rate_t_list.append(null_series_default(tmp_df[(tmp_df.game_name == _game) & (tmp_df.part_day == part_day_t) & (tmp_df.reason == _reason)].gt_7d_bans_rate).sum())
            eq_3d_bans_rate_t_list.append(null_series_default(tmp_df[(tmp_df.game_name == _game) & (tmp_df.part_day == part_day_t) & (tmp_df.reason == _reason)].eq_3d_bans_rate).sum())

    for idx,item in enumerate(dim_type_list):
        tmp_reason = reason_list[idx]

        tmp_eq_3d_bans_rate, tmp_gt_7d_bans_rate=eq_3d_bans_rate_list[idx], gt_7d_bans_rate_list[idx]

        tmp_eq_3d_bans_rate, tmp_eq_3d_bans_rate_h, tmp_eq_3d_bans_rate_t = eq_3d_bans_rate_list[idx], eq_3d_bans_rate_h_list[idx], eq_3d_bans_rate_t_list[idx]
        tmp_gt_7d_bans_rate, tmp_gt_7d_bans_rate_h, tmp_gt_7d_bans_rate_t = gt_7d_bans_rate_list[idx], gt_7d_bans_rate_h_list[idx], gt_7d_bans_rate_t_list[idx]

        tmp_eq_3d_bans_rate_hb, tmp_eq_3d_bans_rate_tb = calc_tb_hb('ratio',eq_3d_bans_rate_list[idx],eq_3d_bans_rate_h_list[idx]), calc_tb_hb('ratio',eq_3d_bans_rate_list[idx],eq_3d_bans_rate_t_list[idx])
        tmp_gt_7d_bans_rate_hb, tmp_gt_7d_bans_rate_tb = calc_tb_hb('ratio',gt_7d_bans_rate_list[idx],gt_7d_bans_rate_h_list[idx]), calc_tb_hb('ratio',gt_7d_bans_rate_list[idx],gt_7d_bans_rate_t_list[idx])

        if ((tmp_eq_3d_bans_rate >= 0.2 and  tmp_eq_3d_bans_rate > 1.5*tmp_eq_3d_bans_rate_h) or (tmp_eq_3d_bans_rate >= 0.2 and  tmp_eq_3d_bans_rate > 1.5*tmp_eq_3d_bans_rate_t)):

            warning_modules += 1

            bans_reason_content = '{}**\<{}\>**：\n' \
                                  ' \t[{}]\n' \
                                  '\t  3日封号占比 {}%、环比<font color="{}">{}{}%%</font>、同比<font color="{}">{}{}%%</font>\n'.format(
                bans_reason_content, item, tmp_reason, str(round(tmp_eq_3d_bans_rate*100,1)),
                mapping_fontcolor(tmp_eq_3d_bans_rate_hb), mapping_updown(tmp_eq_3d_bans_rate_hb), round(abs(tmp_eq_3d_bans_rate_hb) * 100, 1),
                mapping_fontcolor(tmp_eq_3d_bans_rate_tb), mapping_updown(tmp_eq_3d_bans_rate_tb), round(abs(tmp_eq_3d_bans_rate_tb) * 100, 1)
            )

        if ((tmp_gt_7d_bans_rate >= 0.2 and tmp_gt_7d_bans_rate > 1.5 * tmp_gt_7d_bans_rate_h) or (tmp_gt_7d_bans_rate >= 0.2 and tmp_gt_7d_bans_rate > 1.5 * tmp_gt_7d_bans_rate_t)):

            warning_modules += 1

            bans_reason_content = '{}**\<{}\>**：\n' \
                                  ' \t[{}]\n' \
                                  '\t  7日封号占比 {}%、环比<font color="{}">{}{}%%</font>、同比<font color="{}">{}{}%%</font>\n'.format(
                bans_reason_content, item, tmp_reason, str(round(tmp_gt_7d_bans_rate*100,1)),
                mapping_fontcolor(tmp_gt_7d_bans_rate_hb), mapping_updown(tmp_gt_7d_bans_rate_hb), round(abs(tmp_gt_7d_bans_rate_hb) * 100, 1),
                mapping_fontcolor(tmp_gt_7d_bans_rate_tb), mapping_updown(tmp_gt_7d_bans_rate_tb), round(abs(tmp_gt_7d_bans_rate_tb) * 100, 1)
            )
    if bans_reason_content == '':
        bans_reason_content = '\t无异常\n'


# p4 新封号查询成功率 按 日期、游戏
succ_query_rate_title = '#### 4 新封号查询成功率'
succ_query_rate_content = ''

succrate_dim_day_game = '''
    select  
        a.part_day,
        b.title as game_name,
        case when b.categoryid = 1 then '手游' when b.categoryid = 2 then '端游' else '其他' end category,
        count(1) as times,
        coalesce(sum(case when a.task_status=30 then 1 end),0) as succ_times,
        coalesce(sum(case when a.task_status=30 then 1 end),0)*1.00/count(1) as succ_rate
    from kudu.safe_center.game_cheat_query_task a
    left join kudu.zhwdb.zhw_game_info b 
    on a.game_id=b.id
    where a.task_type=0 /*任务类型=封号查询*/
    and a.source=28     /*来源=新封号*/
    and a.part_day between '{0}' and '{1}' 
    group by 1,2,3
'''.format(start_day,end_day)
tmp_df = pd.read_sql(succrate_dim_day_game, con=presto_db) # p1 封号率 按 日期、游戏、类型
if not len(tmp_df):
    succ_query_rate_content = '\t无异常\n'
else:
    data_types = {
        'part_day': str, 'game_name': str, 'category': str, 'times': int, 'succ_times': int, 'succ_rate': float
    }
    tmp_df = tmp_df.astype(data_types)
    dim_type_list = []
    times_list, times_h_list, times_t_list = [], [], []
    succ_rate_list, succ_rate_h_list,succ_rate_t_list = [], [], []

    game_name = ['王者荣耀','和平精英','枪战王者','穿越火线','英雄联盟','逆战','绝地求生']

    for _game in game_name:
        dim_type_list.append(_game)

        times_list.append(null_series_default(tmp_df[(tmp_df.game_name == _game) & (tmp_df.part_day == current_day)].times).sum())
        times_h_list.append(null_series_default(tmp_df[(tmp_df.game_name == _game) & (tmp_df.part_day == part_day_h)].times).sum())
        times_t_list.append(null_series_default(tmp_df[(tmp_df.game_name == _game) & (tmp_df.part_day == part_day_t)].times).sum())

        succ_rate_list.append(null_series_default(tmp_df[(tmp_df.game_name == _game) & (tmp_df.part_day == current_day)].succ_rate).sum())
        succ_rate_h_list.append(null_series_default(tmp_df[(tmp_df.game_name == _game) & (tmp_df.part_day == part_day_h)].succ_rate).sum())
        succ_rate_t_list.append(null_series_default(tmp_df[(tmp_df.game_name == _game) & (tmp_df.part_day == part_day_t)].succ_rate).sum())

    for idx,item in enumerate(dim_type_list):
        tmp_times = times_list[idx]
        tmp_times_hb, tmp_times_tb = calc_tb_hb('value',times_list[idx],times_h_list[idx]), calc_tb_hb('value',times_list[idx],times_t_list[idx])

        tmp_succ_rate = succ_rate_list[idx]
        tmp_succ_rate_hb, tmp_succ_rate_tb = calc_tb_hb('ratio', succ_rate_list[idx], succ_rate_h_list[idx]), calc_tb_hb('ratio', succ_rate_list[idx],succ_rate_t_list[idx])

        # (新封号查询成功次数>500 且 成功率>10% 且 成功率同比或环比下降15%)
        if (tmp_times>500 and tmp_succ_rate>=0.1 and (tmp_succ_rate_hb < -0.15 or tmp_succ_rate_tb<-0.15)):

            warning_modules += 1

            succ_query_rate_content = '{}**\<{}\> **：\n' \
                                      '\t成功率 {}%、环比<font color="{}">{}{}%</font>、同比<font color="{}">{}{}%</font>\n'.format(
                succ_query_rate_content, item,
                str(round(tmp_succ_rate*100,1)),
                mapping_fontcolor(tmp_succ_rate_hb), mapping_updown(tmp_succ_rate_hb), round(abs(tmp_succ_rate_hb) * 100, 1),
                mapping_fontcolor(tmp_succ_rate_tb),  mapping_updown(tmp_succ_rate_tb), round(abs(tmp_succ_rate_tb) * 100, 1)
            )

    if succ_query_rate_content == '':
        succ_query_rate_content = '\t无异常\n'


# p5 新封号查询失败占比 按 日期、游戏、原因
fail_query_rate_title = '#### 5 新封号查询失败占比'
fail_query_rate_content = ''

failtimes_dim_day_game_reason = '''
    select  
        a.part_day,
        b.title as game_name,
        case when b.categoryid = 1 then '手游' when b.categoryid = 2 then '端游' else '其他' end category,
        coalesce(a.task_msg,'其他') as reason,
        count(1) as times
    from kudu.safe_center.game_cheat_query_task a
    left join kudu.zhwdb.zhw_game_info b 
    on a.game_id=b.id
    where a.task_type=0 --任务类型=封号查询
    and a.source=28     --来源=新封号
    and a.part_day between '{0}' and '{1}'
    and a.task_status !=30 --查询失败
    group by 1,2,3,4
'''.format(start_day,end_day)
tmp_df1 = pd.read_sql(failtimes_dim_day_game_reason, con=presto_db)
if not len(tmp_df1):
    fail_query_rate_content = '\t无异常\n'
else:
    data_types = {
        'part_day': str, 'game_name': str, 'category': str, 'reason': str, 'times': int
    }
    tmp_df1 = tmp_df1.astype(data_types)
    tmp_df2 = tmp_df1.copy()
    tmp_df2.set_index(['part_day', 'game_name', 'category', 'reason'], inplace=True)
    tmp_df2['times_sum'] = tmp_df2.times.sum(level=['part_day', 'game_name', 'category'])
    tmp_df2.sort_values(by = ['part_day','game_name'],inplace=True)

    tmp_df2['times_rate'] = tmp_df2.times / tmp_df2.times_sum
    tmp_df2.fillna(0,inplace=True)

    tmp_df = tmp_df2.reset_index(drop=False)

    dim_type_list = []
    reason_list = []
    times_list, times_h_list, times_t_list = [], [], []
    times_rate_list, times_rate_h_list, times_rate_t_list = [], [], []

    game_name = ['王者荣耀','和平精英','枪战王者','穿越火线']

    for _game in game_name:
        game_reason_set = set(tmp_df[(tmp_df.game_name == _game) & (tmp_df.part_day == current_day)].reason)
        for _reason in game_reason_set:
            dim_type_list.append(_game)
            reason_list.append(_reason)
            times_list.append(null_series_default(tmp_df[(tmp_df.game_name == _game) & (tmp_df.part_day == current_day) & (tmp_df.reason == _reason)].times).sum())
            times_h_list.append(null_series_default(tmp_df[(tmp_df.game_name == _game) & (tmp_df.part_day == part_day_h) & (tmp_df.reason == _reason)].times).sum())
            times_t_list.append(null_series_default(tmp_df[(tmp_df.game_name == _game) & (tmp_df.part_day == part_day_t) & (tmp_df.reason == _reason)].times).sum())

            times_rate_list.append(null_series_default(tmp_df[(tmp_df.game_name == _game) & (tmp_df.part_day == current_day) & (tmp_df.reason == _reason)].times_rate).sum())
            times_rate_h_list.append(null_series_default(tmp_df[(tmp_df.game_name == _game) & (tmp_df.part_day == part_day_h) & (tmp_df.reason == _reason)].times_rate).sum())
            times_rate_t_list.append(null_series_default(tmp_df[(tmp_df.game_name == _game) & (tmp_df.part_day == part_day_t) & (tmp_df.reason == _reason)].times_rate).sum())

    for idx,item in enumerate(dim_type_list):
        tmp_reason = reason_list[idx]
        tmp_times = times_list[idx]
        tmp_times_hb, tmp_times_tb = calc_tb_hb('value', times_list[idx], times_h_list[idx]), calc_tb_hb('value', times_list[idx], times_t_list[idx])

        tmp_times_rate = times_rate_list[idx]
        tmp_times_rate_hb, tmp_times_rate_tb = calc_tb_hb('ratio',times_rate_list[idx],times_rate_h_list[idx]), calc_tb_hb('ratio',times_rate_list[idx],times_rate_t_list[idx])

        # （失败次数>1000 且 失败次数同比or环比增长50%） 或 （失败次数>1000 且 占比>=0.2 且 占比同比or环比增长15%）
        if ((tmp_times >1000 and (tmp_times_hb > 0.50 or tmp_times_tb > 0.50)) or (tmp_times >1000 and tmp_times_rate>=0.2 and (tmp_times_rate_hb>0.15 or tmp_times_rate_hb>0.15))):

            warning_modules += 1

            fail_query_rate_content = '{}**\<{}\> :**\n' \
                                      '\t[{}] \n' \
                                      '\t  失败次数 {}、环比<font color="{}">{}{}%</font>、同比<font color="{}">{}{}%</font>\n' \
                                      '\t  失败次数占比 {}%、环比<font color="{}">{}{}%</font>、同比<font color="{}">{}{}%</font>\n'.format(
                fail_query_rate_content, item, tmp_reason,
                tmp_times,
                mapping_fontcolor(tmp_times_hb) ,mapping_updown(tmp_times_hb), round(abs(tmp_times_hb)*100,1),
                mapping_fontcolor(tmp_times_tb) ,mapping_updown(tmp_times_tb), round(abs(tmp_times_tb)*100,1),
                round(tmp_times_rate*100,1),
                mapping_fontcolor(tmp_times_rate_hb), mapping_updown(tmp_times_rate_hb), round(abs(tmp_times_rate_hb) * 100, 1),
                mapping_fontcolor(tmp_times_rate_tb), mapping_updown(tmp_times_rate_tb), round(abs(tmp_times_rate_tb) * 100, 1)
            )

    if fail_query_rate_content == '':
        fail_query_rate_content = '\t无异常\n'


# p6 封号查询待核实 按 日期、游戏
ban_unverifies_title = '#### 6 封号查询-封号核实进度'
ban_unverifies_content = ''

unverifies_dim_day_game = '''
	select 
		to_char(a.start_time,'yyyy-mm-dd') as part_day,
		b.title as game_name,
		case when b.categoryid = 1 then '手游' when b.categoryid = 2 then '端游' else '其他' end category,
		count(distinct a.act_zh) as bans,
		coalesce(count(distinct case when a.verify_id is not null then a.act_zh end),0) as verified_bans
	from ods_zhw.zhw_hao_lock_details a 
	left join ods_zhw.zhw_game_info b
	on a.game_id = b.id
	where true 
	and lock_days>7
	and to_char(a.start_time,'yyyy-mm-dd') between '{0}' and '{1}' 
	and a.game_id in (443,446,683,11,17,24,581)
	group by 1,2,3
'''.format(start_day,end_day)
tmp_df = pd.read_sql(unverifies_dim_day_game, con=holo_cnx)
if not len(tmp_df):
    ban_unverifies_content = '\t无异常\n'
else:
    data_types = {
        'part_day': str, 'game_name': str, 'category': str, 'bans': int, 'verified_bans': int
    }
    tmp_df = tmp_df.astype(data_types)
    tmp_df['verified_bans_rate'] = tmp_df.verified_bans/tmp_df.bans
    tmp_df['part_day_d'] = pd.to_datetime(tmp_df.part_day)
    tmp_df['gap_hours'] = (datetime.datetime.now() - tmp_df.part_day_d)/pd.Timedelta(1, 'H')
    # 目标进度：当日完成70%的封号核实量
    tmp_df['target_verified_bans'] = tmp_df.apply(lambda x: x['bans'] if x['gap_hours']/24*0.7>=1 else x['gap_hours']/24*0.7*x['bans'], axis = 1)
    tmp_df = tmp_df[tmp_df.part_day.isin([current_day, part_day_h])]
    tmp_df.reset_index(inplace=True)

    for idx,item in tmp_df.iterrows():
        tmp_game_name = item['game_name']
        tmp_part_day = item['part_day']
        tmp_bans = item['bans']
        tmp_verified_bans = item['verified_bans']
        tmp_verified_bans_rate = item['verified_bans_rate']
        tmp_target_verified_bans = item['target_verified_bans']

        # （已核实数量<目标进度数量)
        if tmp_verified_bans < tmp_target_verified_bans:

            warning_modules += 1

            ban_unverifies_content = '{}**\<{}\>：**\n' \
                                     '\t{} 封号数 {}、已核实{}、核实进度{}%\n'.format(
                ban_unverifies_content, tmp_game_name, tmp_part_day, tmp_bans, tmp_verified_bans, round(tmp_verified_bans_rate*100,1)
            )

    if ban_unverifies_content == '':
        ban_unverifies_content = '\t无异常\n'

if warning_modules == 0 :
    sys.exit()
else:
    content = title + '\n' + '>' \
              + banratio_title + '\n' + banratio_content \
              + interval_bans_title + '\n' + interval_bans_content \
              + bans_reason_title + '\n' + bans_reason_content \
              + succ_query_rate_title + '\n' + succ_query_rate_content \
              + fail_query_rate_title + '\n' + fail_query_rate_content \
              + ban_unverifies_title + '\n' + ban_unverifies_content \
              + '\n预警逻辑链接：https://wl28tzag2r.feishu.cn/docx/JPR0d4P0wofbsuxvLJ0cq9mznMh'

    test_webhook = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=231ae38a-3d31-4635-80d2-800029963832"
    prod_webhook = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=231ae38a-3d31-4635-80d2-800029963832"
    work_wxrobot(content, test_webhook)