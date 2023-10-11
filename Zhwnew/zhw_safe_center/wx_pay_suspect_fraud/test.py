# _*_coding:utf-8 _*_

# @Time      : 2023/7/6  15:34
# @Author    : An
# @File      : gg_fenghao_orders_group.py
# @Software  : 日常指标预警

import time, datetime, configparser, warnings, math, platform , psycopg2
import sys
import pandas as pd
import pymysql, presto
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
        elif numeric == 0:
            return 0
        else:
            return numeric/compare_numeric - 1
    else:
        if compare_numeric == 0:
            return numeric
        elif numeric == 0:
            return 0
        else:
            return numeric - compare_numeric

current_time = (datetime.datetime.now() - datetime.timedelta(days=0)).strftime('%Y-%m-%d %H:%M:%S')
current_day = (datetime.datetime.now() - datetime.timedelta(days=0)).strftime('%Y-%m-%d')

# 数据窗口日期
start_day = (datetime.datetime.now() - datetime.timedelta(days=14)).strftime('%Y-%m-%d')
end_day = (datetime.datetime.now() - datetime.timedelta(days=0)).strftime('%Y-%m-%d')

part_day_h = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
part_day_t = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')

# p1 封号率 按 日期、游戏、类型
banratio_dim_day_game = '''
    select 
        coalesce(t1.part_day,t2.part_day) as part_day,
        coalesce(t1.title,t2.title) as game_name,
        coalesce(t1.category,t2.category) as category,
        max(t1.orders) as orders,
        coalesce(max(t2.gt_7d_bans),0) as gt_7d_bans
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
            count(distinct act_zh) as gt_7d_bans
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
tmp1 = pd.read_sql(banratio_dim_day_game, con=holo_cnx) # p1 封号率 按 日期、游戏、类型

# 手游封号率
mobile_gt_7d_bans = tmp1[(tmp1.category=='手游') & (tmp1.part_day == current_day)].gt_7d_bans.sum()
mobile_orders = tmp1[(tmp1.category=='手游') & (tmp1.part_day == current_day)].orders.sum()
mobile_gt_7d_bans_ratio = mobile_gt_7d_bans / mobile_orders

mobile_gt_7d_bans_h = tmp1[(tmp1.category=='手游') & (tmp1.part_day == part_day_h)].gt_7d_bans.sum()
mobile_orders_h = tmp1[(tmp1.category=='手游') & (tmp1.part_day == part_day_h)].orders.sum()
mobile_gt_7d_bans_ratio_h = mobile_gt_7d_bans_h / mobile_orders_h

mobile_gt_7d_bans_t = tmp1[(tmp1.category=='手游') & (tmp1.part_day == part_day_t)].gt_7d_bans.sum()
mobile_orders_t = tmp1[(tmp1.category=='手游') & (tmp1.part_day == part_day_t)].orders.sum()
mobile_gt_7d_bans_ratio_t = mobile_gt_7d_bans_t / mobile_orders_t

# 手游封号率-王者
wangzhe_gt_7d_bans = tmp1[(tmp1.game_name=='王者荣耀') & (tmp1.part_day == current_day)].gt_7d_bans.sum()
wangzhe_orders = tmp1[(tmp1.game_name=='王者荣耀') & (tmp1.part_day == current_day)].orders.sum()
wangzhe_gt_7d_bans_ratio = wangzhe_gt_7d_bans / wangzhe_orders

wangzhe_gt_7d_bans_h = tmp1[(tmp1.game_name=='王者荣耀') & (tmp1.part_day == part_day_h)].gt_7d_bans.sum()
wangzhe_orders_h = tmp1[(tmp1.game_name=='王者荣耀') & (tmp1.part_day == part_day_h)].orders.sum()
wangzhe_gt_7d_bans_ratio_h = wangzhe_gt_7d_bans_h / wangzhe_orders_h

wangzhe_gt_7d_bans_t = tmp1[(tmp1.game_name=='王者荣耀') & (tmp1.part_day == part_day_t)].gt_7d_bans.sum()
wangzhe_orders_t = tmp1[(tmp1.game_name=='王者荣耀') & (tmp1.part_day == part_day_t)].orders.sum()
wangzhe_gt_7d_bans_ratio_t = wangzhe_gt_7d_bans_t / wangzhe_orders_t

# 手游封号率-和平
heping_gt_7d_bans = tmp1[(tmp1.game_name=='和平精英') & (tmp1.part_day == current_day)].gt_7d_bans.sum()
heping_orders = tmp1[(tmp1.game_name=='和平精英') & (tmp1.part_day == current_day)].orders.sum()
heping_gt_7d_bans_ratio = heping_gt_7d_bans / heping_orders

heping_gt_7d_bans_h = tmp1[(tmp1.game_name=='和平精英') & (tmp1.part_day == part_day_h)].gt_7d_bans.sum()
heping_orders_h = tmp1[(tmp1.game_name=='和平精英') & (tmp1.part_day == part_day_h)].orders.sum()
heping_gt_7d_bans_ratio_h = heping_gt_7d_bans_h / heping_orders_h

heping_gt_7d_bans_t = tmp1[(tmp1.game_name=='和平精英') & (tmp1.part_day == part_day_t)].gt_7d_bans.sum()
heping_orders_t = tmp1[(tmp1.game_name=='和平精英') & (tmp1.part_day == part_day_t)].orders.sum()
heping_gt_7d_bans_ratio_t = heping_gt_7d_bans_t / heping_orders_t

# 手游封号率-枪战
qiangzhan_gt_7d_bans = tmp1[(tmp1.game_name=='枪战王者') & (tmp1.part_day == current_day)].gt_7d_bans.sum()
qiangzhan_orders = tmp1[(tmp1.game_name=='枪战王者') & (tmp1.part_day == current_day)].orders.sum()
qiangzhan_gt_7d_bans_ratio = qiangzhan_gt_7d_bans / qiangzhan_orders

qiangzhan_gt_7d_bans_h = tmp1[(tmp1.game_name=='枪战王者') & (tmp1.part_day == part_day_h)].gt_7d_bans.sum()
qiangzhan_orders_h = tmp1[(tmp1.game_name=='枪战王者') & (tmp1.part_day == part_day_h)].orders.sum()
qiangzhan_gt_7d_bans_ratio_h = qiangzhan_gt_7d_bans_h / qiangzhan_orders_h

qiangzhan_gt_7d_bans_t = tmp1[(tmp1.game_name=='枪战王者') & (tmp1.part_day == part_day_t)].gt_7d_bans.sum()
qiangzhan_orders_t = tmp1[(tmp1.game_name=='枪战王者') & (tmp1.part_day == part_day_t)].orders.sum()
qiangzhan_gt_7d_bans_ratio_t = qiangzhan_gt_7d_bans_t / qiangzhan_orders_t

# 端游封号率
client_gt_7d_bans = tmp1[(tmp1.category=='端游') & (tmp1.part_day == current_day)].gt_7d_bans.sum()
client_orders = tmp1[(tmp1.category=='端游') & (tmp1.part_day == current_day)].orders.sum()
client_gt_7d_bans_ratio = client_gt_7d_bans / client_orders

client_gt_7d_bans_h = tmp1[(tmp1.category=='端游') & (tmp1.part_day == part_day_h)].gt_7d_bans.sum()
client_orders_h = tmp1[(tmp1.category=='端游') & (tmp1.part_day == part_day_h)].orders.sum()
client_gt_7d_bans_ratio_h = client_gt_7d_bans_h / client_orders_h

client_gt_7d_bans_t = tmp1[(tmp1.category=='端游') & (tmp1.part_day == part_day_t)].gt_7d_bans.sum()
client_orders_t = tmp1[(tmp1.category=='端游') & (tmp1.part_day == part_day_t)].orders.sum()
client_gt_7d_bans_ratio_t = client_gt_7d_bans_t / client_orders_t

# 端游封号率-穿越火线
chuanyue_gt_7d_bans = tmp1[(tmp1.game_name=='穿越火线') & (tmp1.part_day == current_day)].gt_7d_bans.sum()
chuanyue_orders = tmp1[(tmp1.game_name=='穿越火线') & (tmp1.part_day == current_day)].orders.sum()
chuanyue_gt_7d_bans_ratio = chuanyue_gt_7d_bans / chuanyue_orders

chuanyue_gt_7d_bans_h = tmp1[(tmp1.game_name=='穿越火线') & (tmp1.part_day == part_day_h)].gt_7d_bans.sum()
chuanyue_orders_h = tmp1[(tmp1.game_name=='穿越火线') & (tmp1.part_day == part_day_h)].orders.sum()
chuanyue_gt_7d_bans_ratio_h = chuanyue_gt_7d_bans_h / chuanyue_orders_h

chuanyue_gt_7d_bans_t = tmp1[(tmp1.game_name=='穿越火线') & (tmp1.part_day == part_day_t)].gt_7d_bans.sum()
chuanyue_orders_t = tmp1[(tmp1.game_name=='穿越火线') & (tmp1.part_day == part_day_t)].orders.sum()
chuanyue_gt_7d_bans_ratio_t = chuanyue_gt_7d_bans_t / chuanyue_orders_t

# 端游封号率-英雄联盟
lol_gt_7d_bans = tmp1[(tmp1.game_name=='英雄联盟') & (tmp1.part_day == current_day)].gt_7d_bans.sum()
lol_orders = tmp1[(tmp1.game_name=='英雄联盟') & (tmp1.part_day == current_day)].orders.sum()
lol_gt_7d_bans_ratio = lol_gt_7d_bans / lol_orders

lol_gt_7d_bans_h = tmp1[(tmp1.game_name=='英雄联盟') & (tmp1.part_day == part_day_h)].gt_7d_bans.sum()
lol_orders_h = tmp1[(tmp1.game_name=='英雄联盟') & (tmp1.part_day == part_day_h)].orders.sum()
lol_gt_7d_bans_ratio_h = lol_gt_7d_bans_h / lol_orders_h

lol_gt_7d_bans_t = tmp1[(tmp1.game_name=='英雄联盟') & (tmp1.part_day == part_day_t)].gt_7d_bans.sum()
lol_orders_t = tmp1[(tmp1.game_name=='英雄联盟') & (tmp1.part_day == part_day_t)].orders.sum()
lol_gt_7d_bans_ratio_t = lol_gt_7d_bans_t / lol_orders_t

# 端游封号率-逆战
nizhan_gt_7d_bans = tmp1[(tmp1.game_name=='逆战') & (tmp1.part_day == current_day)].gt_7d_bans.sum()
nizhan_orders = tmp1[(tmp1.game_name=='逆战') & (tmp1.part_day == current_day)].orders.sum()
nizhan_gt_7d_bans_ratio = nizhan_gt_7d_bans / nizhan_orders

nizhan_gt_7d_bans_h = tmp1[(tmp1.game_name=='逆战') & (tmp1.part_day == part_day_h)].gt_7d_bans.sum()
nizhan_orders_h = tmp1[(tmp1.game_name=='逆战') & (tmp1.part_day == part_day_h)].orders.sum()
nizhan_gt_7d_bans_ratio_h = nizhan_gt_7d_bans_h / nizhan_orders_h

nizhan_gt_7d_bans_t = tmp1[(tmp1.game_name=='逆战') & (tmp1.part_day == part_day_t)].gt_7d_bans.sum()
nizhan_orders_t = tmp1[(tmp1.game_name=='逆战') & (tmp1.part_day == part_day_t)].orders.sum()
nizhan_gt_7d_bans_ratio_t = nizhan_gt_7d_bans_t / nizhan_orders_t

# 端游封号率-绝地求生
juedi_gt_7d_bans = tmp1[(tmp1.game_name=='绝地求生') & (tmp1.part_day == current_day)].gt_7d_bans.sum()
juedi_orders = tmp1[(tmp1.game_name=='绝地求生') & (tmp1.part_day == current_day)].orders.sum()
juedi_gt_7d_bans_ratio = juedi_gt_7d_bans / juedi_orders

juedi_gt_7d_bans_h = tmp1[(tmp1.game_name=='绝地求生') & (tmp1.part_day == part_day_h)].gt_7d_bans.sum()
juedi_orders_h = tmp1[(tmp1.game_name=='绝地求生') & (tmp1.part_day == part_day_h)].orders.sum()
juedi_gt_7d_bans_ratio_h = juedi_gt_7d_bans_h / juedi_orders_h

juedi_gt_7d_bans_t = tmp1[(tmp1.game_name=='绝地求生') & (tmp1.part_day == part_day_t)].gt_7d_bans.sum()
juedi_orders_t = tmp1[(tmp1.game_name=='绝地求生') & (tmp1.part_day == part_day_t)].orders.sum()
juedi_gt_7d_bans_ratio_t = juedi_gt_7d_bans_t / juedi_orders_t

# 封号数量是同环比期的2倍且封号量>=10 or 封号率是同环期的2倍

if not (calc_tb_hb('value',mobile_gt_7d_bans,mobile_gt_7d_bans_h)>=2 and mobile_gt_7d_bans>=10) or \
    (calc_tb_hb('value', mobile_gt_7d_bans, mobile_gt_7d_bans_t) >= 2 and mobile_gt_7d_bans >= 10) or \
    (calc_tb_hb('ratio', mobile_gt_7d_bans_ratio, mobile_gt_7d_bans_h) >= mobile_gt_7d_bans_h*2) or \
    (calc_tb_hb('ratio', mobile_gt_7d_bans_ratio, mobile_gt_7d_bans_t) >= mobile_gt_7d_bans_t*2):

    msg = '手游封号数 '+ str(mobile_gt_7d_bans) +\
          '、环比 '+ str(round(calc_tb_hb('value',mobile_gt_7d_bans,mobile_gt_7d_bans_h)*100,1)) +'%' +\
          '、同比' + str(round(calc_tb_hb('value', mobile_gt_7d_bans, mobile_gt_7d_bans_t)*100,1)) + '%' +\
          '，封号率 '+ str(round(mobile_gt_7d_bans_ratio*10000,1)) + '‱' + \
          '、环比' + str(round(calc_tb_hb('ratio', mobile_gt_7d_bans_ratio, mobile_gt_7d_bans_ratio_h)*10000,1)) + '‱' + \
          '、同比' + str(round(calc_tb_hb('ratio', mobile_gt_7d_bans_ratio, mobile_gt_7d_bans_ratio_t)*10000,1)) + '‱'
    print(msg)

if not (calc_tb_hb('value',wangzhe_gt_7d_bans,wangzhe_gt_7d_bans_h)>=2 and wangzhe_gt_7d_bans>=10) or \
    (calc_tb_hb('value', wangzhe_gt_7d_bans, wangzhe_gt_7d_bans_t) >= 2 and wangzhe_gt_7d_bans >= 10) or \
    (calc_tb_hb('ratio', wangzhe_gt_7d_bans_ratio, wangzhe_gt_7d_bans_h) >= wangzhe_gt_7d_bans_h*2) or \
    (calc_tb_hb('ratio', wangzhe_gt_7d_bans_ratio, wangzhe_gt_7d_bans_t) >= wangzhe_gt_7d_bans_t*2):

    msg = '王者荣耀 封号数 '+ str(wangzhe_gt_7d_bans) +\
          '、环比 '+ str(round(calc_tb_hb('value',wangzhe_gt_7d_bans,wangzhe_gt_7d_bans_h)*100,1)) +'%' +\
          '、同比' + str(round(calc_tb_hb('value', wangzhe_gt_7d_bans, wangzhe_gt_7d_bans_t)*100,1)) + '%' +\
          '，封号率 '+ str(round(wangzhe_gt_7d_bans_ratio*10000,1)) + '‱' + \
          '、环比' + str(round(calc_tb_hb('ratio', wangzhe_gt_7d_bans_ratio, wangzhe_gt_7d_bans_ratio_h)*10000,1)) + '‱' + \
          '、同比' + str(round(calc_tb_hb('ratio', wangzhe_gt_7d_bans_ratio, wangzhe_gt_7d_bans_ratio_t)*10000,1)) + '‱'
    print(msg)

if not (calc_tb_hb('value',heping_gt_7d_bans,heping_gt_7d_bans_h)>=2 and heping_gt_7d_bans>=10) or \
    (calc_tb_hb('value', heping_gt_7d_bans, heping_gt_7d_bans_t) >= 2 and heping_gt_7d_bans >= 10) or \
    (calc_tb_hb('ratio', heping_gt_7d_bans_ratio, heping_gt_7d_bans_h) >= heping_gt_7d_bans_h*2) or \
    (calc_tb_hb('ratio', heping_gt_7d_bans_ratio, heping_gt_7d_bans_t) >= heping_gt_7d_bans_t*2):

    msg = '和平精英 封号数 '+ str(heping_gt_7d_bans) +\
          '、环比 '+ str(round(calc_tb_hb('value',heping_gt_7d_bans,heping_gt_7d_bans_h)*100,1)) +'%' +\
          '、同比' + str(round(calc_tb_hb('value', heping_gt_7d_bans, heping_gt_7d_bans_t)*100,1)) + '%' +\
          '，封号率 '+ str(round(heping_gt_7d_bans_ratio*10000,1)) + '‱' + \
          '、环比' + str(round(calc_tb_hb('ratio', heping_gt_7d_bans_ratio, heping_gt_7d_bans_ratio_h)*10000,1)) + '‱' + \
          '、同比' + str(round(calc_tb_hb('ratio', heping_gt_7d_bans_ratio, heping_gt_7d_bans_ratio_t)*10000,1)) + '‱'
    print(msg)

if not (calc_tb_hb('value',qiangzhan_gt_7d_bans,qiangzhan_gt_7d_bans_h)>=2 and qiangzhan_gt_7d_bans>=10) or \
    (calc_tb_hb('value', qiangzhan_gt_7d_bans, qiangzhan_gt_7d_bans_t) >= 2 and qiangzhan_gt_7d_bans >= 10) or \
    (calc_tb_hb('ratio', qiangzhan_gt_7d_bans_ratio, qiangzhan_gt_7d_bans_h) >= qiangzhan_gt_7d_bans_h*2) or \
    (calc_tb_hb('ratio', qiangzhan_gt_7d_bans_ratio, qiangzhan_gt_7d_bans_t) >= qiangzhan_gt_7d_bans_t*2):

    msg = '枪战王者 封号数 '+ str(qiangzhan_gt_7d_bans) +\
          '、环比 '+ str(round(calc_tb_hb('value',qiangzhan_gt_7d_bans,qiangzhan_gt_7d_bans_h)*100,1)) +'%' +\
          '、同比' + str(round(calc_tb_hb('value', qiangzhan_gt_7d_bans, qiangzhan_gt_7d_bans_t)*100,1)) + '%' +\
          '，封号率 '+ str(round(qiangzhan_gt_7d_bans_ratio*10000,1)) + '‱' + \
          '、环比' + str(round(calc_tb_hb('ratio', qiangzhan_gt_7d_bans_ratio, qiangzhan_gt_7d_bans_ratio_h)*10000,1)) + '‱' + \
          '、同比' + str(round(calc_tb_hb('ratio', qiangzhan_gt_7d_bans_ratio, qiangzhan_gt_7d_bans_ratio_t)*10000,1)) + '‱'
    print(msg)

# 封号数量是同环比期的2倍且封号量>=10 or 封号率是同环期的2倍
if not (calc_tb_hb('value',client_gt_7d_bans,client_gt_7d_bans_h)>=2 and client_gt_7d_bans>=10) or \
    (calc_tb_hb('value', client_gt_7d_bans, client_gt_7d_bans_t) >= 2 and client_gt_7d_bans >= 10) or \
    (calc_tb_hb('ratio', client_gt_7d_bans_ratio, client_gt_7d_bans_h) >= client_gt_7d_bans_h*2) or \
    (calc_tb_hb('ratio', client_gt_7d_bans_ratio, client_gt_7d_bans_t) >= client_gt_7d_bans_t*2):

    msg = '端游封号数 '+ str(client_gt_7d_bans) +\
          '、环比 '+ str(round(calc_tb_hb('value',client_gt_7d_bans,client_gt_7d_bans_h)*100,1)) +'%' +\
          '、同比' + str(round(calc_tb_hb('value', client_gt_7d_bans, client_gt_7d_bans_t)*100,1)) + '%' +\
          '，封号率 '+ str(round(client_gt_7d_bans_ratio*10000,1)) + '‱' + \
          '、环比' + str(round(calc_tb_hb('ratio', client_gt_7d_bans_ratio, client_gt_7d_bans_ratio_h)*10000,1)) + '‱' + \
          '、同比' + str(round(calc_tb_hb('ratio', client_gt_7d_bans_ratio, client_gt_7d_bans_ratio_t)*10000,1)) + '‱'
    print(msg)

if not (calc_tb_hb('value',chuanyue_gt_7d_bans,chuanyue_gt_7d_bans_h)>=2 and chuanyue_gt_7d_bans>=10) or \
    (calc_tb_hb('value', chuanyue_gt_7d_bans, chuanyue_gt_7d_bans_t) >= 2 and chuanyue_gt_7d_bans >= 10) or \
    (calc_tb_hb('ratio', chuanyue_gt_7d_bans_ratio, chuanyue_gt_7d_bans_h) >= chuanyue_gt_7d_bans_h*2) or \
    (calc_tb_hb('ratio', chuanyue_gt_7d_bans_ratio, chuanyue_gt_7d_bans_t) >= chuanyue_gt_7d_bans_t*2):

    msg = '穿越火线 封号数 '+ str(chuanyue_gt_7d_bans) +\
          '、环比 '+ str(round(calc_tb_hb('value',chuanyue_gt_7d_bans,chuanyue_gt_7d_bans_h)*100,1)) +'%' +\
          '、同比' + str(round(calc_tb_hb('value', chuanyue_gt_7d_bans, chuanyue_gt_7d_bans_t)*100,1)) + '%' +\
          '，封号率 '+ str(round(chuanyue_gt_7d_bans_ratio*10000,1)) + '‱' + \
          '、环比' + str(round(calc_tb_hb('ratio', chuanyue_gt_7d_bans_ratio, chuanyue_gt_7d_bans_ratio_h)*10000,1)) + '‱' + \
          '、同比' + str(round(calc_tb_hb('ratio', chuanyue_gt_7d_bans_ratio, chuanyue_gt_7d_bans_ratio_t)*10000,1)) + '‱'
    print(msg)

if not (calc_tb_hb('value',lol_gt_7d_bans,lol_gt_7d_bans_h)>=2 and lol_gt_7d_bans>=10) or \
    (calc_tb_hb('value', lol_gt_7d_bans, lol_gt_7d_bans_t) >= 2 and lol_gt_7d_bans >= 10) or \
    (calc_tb_hb('ratio', lol_gt_7d_bans_ratio, lol_gt_7d_bans_h) >= lol_gt_7d_bans_h*2) or \
    (calc_tb_hb('ratio', lol_gt_7d_bans_ratio, lol_gt_7d_bans_t) >= lol_gt_7d_bans_t*2):

    msg = '英雄联盟 封号数 '+ str(lol_gt_7d_bans) +\
          '、环比 '+ str(round(calc_tb_hb('value',lol_gt_7d_bans,lol_gt_7d_bans_h)*100,1)) +'%' +\
          '、同比' + str(round(calc_tb_hb('value', lol_gt_7d_bans, lol_gt_7d_bans_t)*100,1)) + '%' +\
          '，封号率 '+ str(round(lol_gt_7d_bans_ratio*10000,1)) + '‱' + \
          '、环比' + str(round(calc_tb_hb('ratio', lol_gt_7d_bans_ratio, lol_gt_7d_bans_ratio_h)*10000,1)) + '‱' + \
          '、同比' + str(round(calc_tb_hb('ratio', lol_gt_7d_bans_ratio, lol_gt_7d_bans_ratio_t)*10000,1)) + '‱'
    print(msg)

print(calc_tb_hb('value',nizhan_gt_7d_bans,nizhan_gt_7d_bans_h))
print(calc_tb_hb('value', nizhan_gt_7d_bans, nizhan_gt_7d_bans_t))
print(calc_tb_hb('ratio', nizhan_gt_7d_bans_ratio, nizhan_gt_7d_bans_h))
print(calc_tb_hb('ratio', nizhan_gt_7d_bans_ratio, nizhan_gt_7d_bans_t) )
if not (calc_tb_hb('value',nizhan_gt_7d_bans,nizhan_gt_7d_bans_h)>=2 and nizhan_gt_7d_bans>=10) or \
    (calc_tb_hb('value', nizhan_gt_7d_bans, nizhan_gt_7d_bans_t) >= 2 and nizhan_gt_7d_bans >= 10) or \
    (calc_tb_hb('ratio', nizhan_gt_7d_bans_ratio, nizhan_gt_7d_bans_h) >= nizhan_gt_7d_bans_h*2) or \
    (calc_tb_hb('ratio', nizhan_gt_7d_bans_ratio, nizhan_gt_7d_bans_t) >= nizhan_gt_7d_bans_t*2):
    print(1)
    msg = '逆战 封号数 '+ str(nizhan_gt_7d_bans) +\
          '、环比 '+ str(round(calc_tb_hb('value',nizhan_gt_7d_bans,nizhan_gt_7d_bans_h)*100,1)) +'%' +\
          '、同比' + str(round(calc_tb_hb('value', nizhan_gt_7d_bans, nizhan_gt_7d_bans_t)*100,1)) + '%' +\
          '，封号率 '+ str(round(nizhan_gt_7d_bans_ratio*10000,1)) + '‱' + \
          '、环比' + str(round(calc_tb_hb('ratio', nizhan_gt_7d_bans_ratio, nizhan_gt_7d_bans_ratio_h)*10000,1)) + '‱' + \
          '、同比' + str(round(calc_tb_hb('ratio', nizhan_gt_7d_bans_ratio, nizhan_gt_7d_bans_ratio_t)*10000,1)) + '‱'
    print(msg)

if not (calc_tb_hb('value',juedi_gt_7d_bans,juedi_gt_7d_bans_h)>=2 and juedi_gt_7d_bans>=10) or \
    (calc_tb_hb('value', juedi_gt_7d_bans, juedi_gt_7d_bans_t) >= 2 and juedi_gt_7d_bans >= 10) or \
    (calc_tb_hb('ratio', juedi_gt_7d_bans_ratio, juedi_gt_7d_bans_h) >= juedi_gt_7d_bans_h*2) or \
    (calc_tb_hb('ratio', juedi_gt_7d_bans_ratio, juedi_gt_7d_bans_t) >= juedi_gt_7d_bans_t*2):

    msg = '绝地 封号数 '+ str(juedi_gt_7d_bans) +\
          '、环比 '+ str(round(calc_tb_hb('value',juedi_gt_7d_bans,juedi_gt_7d_bans_h)*100,1)) +'%' +\
          '、同比' + str(round(calc_tb_hb('value', juedi_gt_7d_bans, juedi_gt_7d_bans_t)*100,1)) + '%' +\
          '，封号率 '+ str(round(juedi_gt_7d_bans_ratio*10000,1)) + '‱' + \
          '、环比' + str(round(calc_tb_hb('ratio', juedi_gt_7d_bans_ratio, juedi_gt_7d_bans_ratio_h)*10000,1)) + '‱' + \
          '、同比' + str(round(calc_tb_hb('ratio', juedi_gt_7d_bans_ratio, juedi_gt_7d_bans_ratio_t)*10000,1)) + '‱'
    print(msg)