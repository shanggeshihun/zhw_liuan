# _*_coding:utf-8 _*_

# @Time      : 2022/5/18  15:37
# @Author    : An
# @File      : zhw_msg_recall_new_v2.py
# @Software  : 在 zqy_activate_msg_new 基础上于2022/5/18改版

# 目标表 zqy_duanxin_zhaohui_new @吴辰哲 zqy_duanxin_zhaohui_new'

# noinspection PyUnresolvedReferences
import pymysql,sys
from pyhive import presto
import pandas as pd
# noinspection PyUnresolvedReferences
import numpy as np
# noinspection PyUnresolvedReferences
from sqlalchemy import create_engine
# noinspection PyUnresolvedReferences
import time, datetime
from WorkWeixinRobot.work_weixin_robot import WWXRobot

# 1、连接hive数据库
# 配置项
host_hive = "172.16.13.47"
username_hive = "zhw"
port_hive = 8080
catalog_hive = "hive"
schema_hive = "zhwdb"
# 连接语句
connect_hive = presto.connect(host=host_hive, port=port_hive, username=username_hive, catalog=catalog_hive)


# 2、连接线上运营mysql数据库
# 配置项
host_yy = "rm-2zez7u673640b68x1.mysql.rds.aliyuncs.com"
user_yy = "data_yunying"
password_yy = "yTMgWouzdRfVtmgj"
DB_yy = "data_yunying"
port_yy = "3306"
# 连接语句
connect_yy = create_engine(
    "mysql+pymysql://" + user_yy + ":" + password_yy + "@" + host_yy + ":" + port_yy + "/" + DB_yy, echo=False)
# 案例：cnx = create_engine("mysql+pymysql://"+user+":"+password+"@"+host+":"+port+"/"+DB, echo=False)


# a = pd.read_csv(r'/work/project/zhw_product/rita/issue_id.txt')
a = pd.read_csv(r'E:\工作文件\在刀锋\dofun\Python\Python\zhw_liuan\zhw\zhw_ads\issue_id.txt')
b = pd.Series(a['id']).tolist()
c = tuple(b)  # 元组，用()表示

start_time = time.time()

# 红包领取 & 红包使用
sql_1 = '''
    select 
        a.part_day,
        a.type,
        a.issue,
        count(distinct a.jkx_userid)  as hb_rece_user,-- 红包领取人数
        count(a.id) as hb_rece_num, -- 红包领取数
        sum(a.money) as hb_rece_money, -- 红包领取金额
        count(distinct case when usemoney>0 then jkx_userid end ) as hb_use_user,--红包累积使用金额
        count(case when usemoney>0 then id end) as hb_use_num,
        sum(a.usemoney) as hb_use_money -- 红包累积使用金额
    from 
    (
        select part_day,type,issue,jkx_userid,id,money,usemoney 
        from zhwdb.zhw_hongbao 
        where part_month>='2022-03'
        -- 满足以下条件的数据从2022年3月开始（2022-03-05）
        and ((type=1198  and issue in{0}) or 
        (type=1246 and  issue in(1908, 1909, 1910, 1911, 2013))
        or (type=1248 and  issue in(1916, 1917))
        or (type=1235 and  issue in(1936,1937,1938,1939,1940))
        or (type=1232 and  issue in(1949,1950))
        or (type=1237 and  issue in(1855,1856,1857,1858,1859))
        or (type=1239 and  issue in(1877,1878,1879,1880))
        or (type=1241 and  issue in(1896,1897))
        or (type=1243 and  issue in(2002))
        or (type=1217 and  issue in(1598,1599,1900,1659)) or 
            -- 2022/5/19 添加
        (type=1246 and issue in(2033 ,2034 ,2035 ,2036)) or 
        (type=1256 and issue in(2085,2086)) or 
        (type=1266 and issue in(2081,2083,2139,2140)) or 
        (type=1273 and issue in(2097,2098)) or 
        (type=1258 and issue in(2089)) or 
        (type=1260 and issue in(2091)) or 
        (type=1276 and issue in(2143))
        )
    )a 
    group by 1,2,3
'''.format(c)
result_1 = pd.read_sql(sql_1, connect_hive)
print('success :result_1')


# 领取红包且活跃，领取红包有效期内活跃
sql_2 = '''
    select 
    a.part_day,
    a.type,
    a.issue,
    count(distinct b.userid) as curr_user,  -- 当日领取且活跃人数
    count(distinct c.userid) as youxiao_user  -- 当日领取且有效期内活跃人数
    from 
    (
        select part_day,type,issue,jkx_userid,recetime,outtime 
        from zhwdb.zhw_hongbao  
        where part_month>='2022-03'
        and ((type=1198  and issue in{0}) or 
        (type=1246 and  issue in(1908, 1909, 1910, 1911, 2013))
        or (type=1248 and  issue in(1916, 1917))
        or (type=1235 and  issue in(1936,1937,1938,1939,1940))
        or (type=1232 and  issue in(1949,1950))
        or (type=1237 and  issue in(1855,1856,1857,1858,1859))
        or (type=1239 and  issue in(1877,1878,1879,1880))
        or (type=1241 and  issue in(1896,1897))
        or (type=1243 and  issue in(2002))
        or (type=1217 and  issue in(1598,1599,1900,1659)) or 
            -- 2022/5/19 添加
        (type=1246 and issue in(2033 ,2034 ,2035 ,2036)) or 
        (type=1256 and issue in(2085,2086)) or 
        (type=1266 and issue in(2081,2083,2139,2140)) or 
        (type=1273 and issue in(2097,2098)) or 
        (type=1258 and issue in(2089)) or 
        (type=1260 and issue in(2091)) or 
        (type=1276 and issue in(2143))
        )
    ) a 
    left join 
    (
        select part_day,userid 
        from zhwdb.zhw_user_login_log_extend 
        where part_month>='2022-03'
        group by 1,2
    ) b ---当天的活跃情况
    on a.jkx_userid=b.userid and a.part_day=b.part_day
    left join 
    (
        select part_day,userid,usertimer 
        from zhwdb.zhw_user_login_log_extend 
        where part_month>='2022-03'
        group by 1,2,3
    ) c ---红包有效期内的活跃情况
    on a.jkx_userid=c.userid and to_unixtime(c.usertimer) between a.recetime and a.outtime
    group by 1,2,3
'''.format(c)
print(sql_2)
# result_2 = pd.read_sql(sql_2, connect_hive)
# print('success :result_2')
sys.exit()

sql_3 = '''
    select 
    a.part_day,
    a.type,
    a.issue,
    count(distinct b.userid) as curr_fufei_user,--当日领取当日下单人数
    count(b.id) as curr_fufei_num,
    sum(b.pm) as curr_fufei_money
    from 
    (
        select part_day,type,issue,jkx_userid 
        from zhwdb.zhw_hongbao 
        where part_month>='2022-03' 
        and ((type=1198  and issue in{0}) or 
        (type=1246 and  issue in(1908, 1909, 1910, 1911, 2013))
        or (type=1248 and  issue in(1916, 1917))
        or (type=1235 and  issue in(1936,1937,1938,1939,1940))
        or (type=1232 and  issue in(1949,1950))
        or (type=1237 and  issue in(1855,1856,1857,1858,1859))
        or (type=1239 and  issue in(1877,1878,1879,1880))
        or (type=1241 and  issue in(1896,1897))
        or (type=1243 and  issue in(2002))
        or (type=1217 and  issue in(1598,1599,1900,1659)) or 
            -- 2022/5/19 添加
        (type=1246 and issue in(2033 ,2034 ,2035 ,2036)) or 
        (type=1256 and issue in(2085,2086)) or 
        (type=1266 and issue in(2081,2083,2139,2140)) or 
        (type=1273 and issue in(2097,2098)) or 
        (type=1258 and issue in(2089)) or 
        (type=1260 and issue in(2091)) or 
        (type=1276 and issue in(2143))
        ) 
        group by 1,2,3,4
    )a 
    left join 
    (
        select part_day,userid,id,pm 
        from zhwdb.zhw_dingdan 
        where part_month>='2022-03'
    )b 
    on a.part_day=b.part_day and a.jkx_userid=b.userid 
    group by 1,2,3
'''.format(c)
result_3 = pd.read_sql(sql_3, connect_hive)
print('success :result_3')

# 领取红包的用户数，所在订单金额，所在订单数
sql_4 = '''
    select 
    a.part_day,
    a.type,
    a.issue,
    count(distinct c.userid)  curr_hb_did_user,-- 当日领取当日使用人数
    count(c.id) as curr_hb_did_num, -- 当日领取当日下单订单量
    sum(c.pm) as curr_hb_did_money  -- 当日领取当日下单金额
    from 
    (
        select part_day,type,issue,id 
        from zhwdb.zhw_hongbao 
        where part_month>='2022-03'
        and ((type=1198  and issue in{0}) or 
        (type=1246 and  issue in(1908, 1909, 1910, 1911, 2013))
        or (type=1248 and  issue in(1916, 1917))
        or (type=1235 and  issue in(1936,1937,1938,1939,1940))
        or (type=1232 and  issue in(1949,1950))
        or (type=1237 and  issue in(1855,1856,1857,1858,1859))
        or (type=1239 and  issue in(1877,1878,1879,1880))
        or (type=1241 and  issue in(1896,1897))
        or (type=1243 and  issue in(2002))
        or (type=1217 and  issue in(1598,1599,1900,1659)) or 
            -- 2022/5/19 添加
        (type=1246 and issue in(2033 ,2034 ,2035 ,2036)) or 
        (type=1256 and issue in(2085,2086)) or 
        (type=1266 and issue in(2081,2083,2139,2140)) or 
        (type=1273 and issue in(2097,2098)) or 
        (type=1258 and issue in(2089)) or 
        (type=1260 and issue in(2091)) or 
        (type=1276 and issue in(2143))
        )
    )a 
    left join 
    (
        select order_id,hb_id,part_day 
        from zhwdb.zhw_hongbao_order 
        where part_month>='2022-03'
    ) b 
    on a.id=b.hb_id and a.part_day=b.part_day 
    left join 
    (
        select userid,id,pm,part_day 
        from zhwdb.zhw_dingdan 
        where part_month>='2022-03'
    )c 
    on b.order_id=c.id  and b.part_day=c.part_day 
    group by 1,2,3
'''.format(c)
result_4 = pd.read_sql(sql_4, connect_hive)
print('success :result_4')

sql_5 = '''
    /*3日内活跃用户数*/
    select a.part_day,a.type,a.issue,
    count(distinct b.userid) as threeday_login_user  --当日领取且3日内活跃
    from 
    (
        select part_day,type,issue,jkx_userid,recetime as dt1 
        from zhwdb.zhw_hongbao 
        where part_month>='2022-03' 
        and ((type=1198  and issue in{0}) or 
        (type=1246 and  issue in(1908, 1909, 1910, 1911, 2013))
        or (type=1248 and  issue in(1916, 1917))
        or (type=1235 and  issue in(1936,1937,1938,1939,1940))
        or (type=1232 and  issue in(1949,1950))
        or (type=1237 and  issue in(1855,1856,1857,1858,1859))
        or (type=1239 and  issue in(1877,1878,1879,1880))
        or (type=1241 and  issue in(1896,1897))
        or (type=1243 and  issue in(2002))
        or (type=1217 and  issue in(1598,1599,1900,1659)) or 
            -- 2022/5/19 添加
        (type=1246 and issue in(2033 ,2034 ,2035 ,2036)) or 
        (type=1256 and issue in(2085,2086)) or 
        (type=1266 and issue in(2081,2083,2139,2140)) or 
        (type=1273 and issue in(2097,2098)) or 
        (type=1258 and issue in(2089)) or 
        (type=1260 and issue in(2091)) or 
        (type=1276 and issue in(2143))
        )
    group by 1,2,3,4,5
    ) a 
    left join 
    (
    select part_day,userid,to_unixtime(usertimer) as dt2 
    from zhwdb.zhw_user_login_log_extend 
    where part_month>='2022-03'
    ) b 
    on b.dt2>=a.dt1 and b.dt2<=a.dt1+259200 and a.jkx_userid=b.userid 
    group by 1,2,3
'''.format(c)
result_5 = pd.read_sql(sql_5, connect_hive)
print('success :result_5')

sql_6 = '''
    /*3日内付费用户数*/
    select a.part_day,a.type,a.issue,
    count(distinct b.userid) as threeday_fufei_user,  --当日领取且3日内下单
    count(distinct b.id) as threeday_fufei_num, --当日领取且3日内下单量
    sum(b.pm) as threeday_fufei_money, --当日领取且3日内下单金额
    count(distinct c.userid) as threeday_hb_fufei_user,  --当日领取且3日内红包下单
    count(distinct c.id) as threeday_hb_fufei_num, --当日领取且3日内红包下单量
    sum(c.pm) as threeday_hb_fufei_money --当日领取且3日内红包下单金额
    from 
    (
        select part_day,type,issue,jkx_userid,recetime as dt1 
        from zhwdb.zhw_hongbao 
        where part_month>='2022-03' 
        and ((type=1198  and issue in{0}) or 
        (type=1246 and  issue in(1908, 1909, 1910, 1911, 2013))
        or (type=1248 and  issue in(1916, 1917))
        or (type=1235 and  issue in(1936,1937,1938,1939,1940))
        or (type=1232 and  issue in(1949,1950))
        or (type=1237 and  issue in(1855,1856,1857,1858,1859))
        or (type=1239 and  issue in(1877,1878,1879,1880))
        or (type=1241 and  issue in(1896,1897))
        or (type=1243 and  issue in(2002))
        or (type=1217 and  issue in(1598,1599,1900,1659)) or 
            -- 2022/5/19 添加
        (type=1246 and issue in(2033 ,2034 ,2035 ,2036)) or 
        (type=1256 and issue in(2085,2086)) or 
        (type=1266 and issue in(2081,2083,2139,2140)) or 
        (type=1273 and issue in(2097,2098)) or 
        (type=1258 and issue in(2089)) or 
        (type=1260 and issue in(2091)) or 
        (type=1276 and issue in(2143))
        )
        group by 1,2,3,4,5
    ) a 
    left join 
    (
        select part_day,userid,to_unixtime(add_time) as dt2,id,pm
        from zhwdb.zhw_dingdan 
        where part_month>='2022-03'
    )b 
    on b.dt2>=a.dt1 and b.dt2<=a.dt1+259200 and a.jkx_userid=b.userid 
    left join 
    (
        select t2.part_day,t3.id,t3.pm,t3.userid,t3.dt3
        from
        (
            select part_day,jkx_userid as userid,order_id
            from zhwdb.zhw_hongbao_order 
            where part_month>='2022-03'
        ) t2 
        join 
        (
            select part_day,userid,to_unixtime(add_time) as dt3,id,pm
            from zhwdb.zhw_dingdan 
            where part_month>='2022-03'
        ) t3 
        on t2.order_id = t3.id
    )c 
    on c.dt3>=a.dt1 and c.dt3<=a.dt1+259200 and a.jkx_userid=c.userid 
    group by 1,2,3
'''.format(c)
result_6 = pd.read_sql(sql_6, connect_hive)
print('success :result_6')

sql_7 = '''
    /*7日留存率*/
    select 
    a.part_day,
    a.type ,
    a.issue,
    count(distinct c.userid) sevenday_login_user
    from 
    (
        select part_day,type,issue,jkx_userid 
        from zhwdb.zhw_hongbao  
        where part_month>='2022-03'
        and  ((type=1198  and issue in{0}) or 
        (type=1246 and  issue in(1908, 1909, 1910, 1911, 2013))
        or (type=1248 and  issue in(1916, 1917))
        or (type=1235 and  issue in(1936,1937,1938,1939,1940))
        or (type=1232 and  issue in(1949,1950))
        or (type=1237 and  issue in(1855,1856,1857,1858,1859))
        or (type=1239 and  issue in(1877,1878,1879,1880))
        or (type=1241 and  issue in(1896,1897))
        or (type=1243 and  issue in(2002))
        or (type=1217 and  issue in(1598,1599,1900,1659)) or 
            -- 2022/5/19 添加
        (type=1246 and issue in(2033 ,2034 ,2035 ,2036)) or 
        (type=1256 and issue in(2085,2086)) or 
        (type=1266 and issue in(2081,2083,2139,2140)) or 
        (type=1273 and issue in(2097,2098)) or 
        (type=1258 and issue in(2089)) or 
        (type=1260 and issue in(2091)) or 
        (type=1276 and issue in(2143))
        ) 
        group by 1,2,3,4
    ) a 
    left  join 
    (
        select part_day,userid 
        from zhwdb.zhw_user_login_log_extend 
        where part_month>='2022-03'
        group by 1,2
    ) b ---当天的活跃情况
    on a.jkx_userid=b.userid and a.part_day=b.part_day
    left join 
    (
        select part_day,userid 
        from zhwdb.zhw_user_login_log_extend  
        where part_month>='2022-03'
        group by 1,2
    ) c  
    on b.userid=c.userid and date(c.part_day)>date(a.part_day)
    and date(c.part_day)<date_add('day',7,date(a.part_day))
    group by 1,2,3
'''.format(c)
result_7 = pd.read_sql(sql_7, connect_hive)
print('success :result_7')

sql_8 = '''
/*15日登录留存率*/
    select 
    a.part_day,
    a.type ,
    a.issue,
    count(distinct c.userid)  fifteen_login_user
    from 
    (
        select part_day,type,issue,jkx_userid from zhwdb.zhw_hongbao  where part_month>='2022-03'
        and  ((type=1198  and issue in{0}) or 
        (type=1246 and  issue in(1908, 1909, 1910, 1911, 2013))
        or 	(type=1248 and  issue in(1916, 1917))
        or   (type=1235 and  issue in(1936,1937,1938,1939,1940))
        or   (type=1232 and  issue in(1949,1950))
        or   (type=1237 and  issue in(1855,1856,1857,1858,1859))
        or  (type=1239 and  issue in(1877,1878,1879,1880))
        or  (type=1241 and  issue in(1896,1897))
        or  (type=1243 and  issue in(2002))
        or  (type=1217 and  issue in(1598,1599,1900,1659)) or 
            -- 2022/5/19 添加
        (type=1246 and issue in(2033 ,2034 ,2035 ,2036)) or 
        (type=1256 and issue in(2085,2086)) or 
        (type=1266 and issue in(2081,2083,2139,2140)) or 
        (type=1273 and issue in(2097,2098)) or 
        (type=1258 and issue in(2089)) or 
        (type=1260 and issue in(2091)) or 
        (type=1276 and issue in(2143))
        ) 
        group by 1,2,3,4
    ) a 
    left  join 
    (
        select part_day,userid 
        from zhwdb.zhw_user_login_log_extend 
        where part_month>='2022-03'
        group by 1,2
    ) b ---当天的活跃情况
    on a.jkx_userid=b.userid and a.part_day=b.part_day
    left join 
    (
        select part_day,userid 
        from zhwdb.zhw_user_login_log_extend  
        where part_month>='2022-03'
        group by 1,2
    ) c  
    on b.userid=c.userid and date(c.part_day)>date(a.part_day)
    and date(c.part_day)<date_add('day',15,date(a.part_day))
    group by 1,2,3
'''.format(c)
result_8 = pd.read_sql(sql_8, connect_hive)
print('success :result_8')


sql_9 = '''
    /*7日付费留存率   使用红包后7天内有下单 */
    
    select 
    c.part_day,
    c.type,
    c.issue,
    count(distinct d.userid)  as sevenday_fufei_user
    from 
    (   -- 领取日期，使用红包日期
        select a.part_day,a.type,a.issue,b.jkx_userid,max(b.dt2) as mdt2 from 
        (
            select part_day,type,issue,id,jkx_userid 
            from zhwdb.zhw_hongbao  
            where part_month>='2022-03' 
            and ((type=1198  and issue in{0}) or 
            (type=1246 and  issue in(1908, 1909, 1910, 1911, 2013))
            or (type=1248 and  issue in(1916, 1917))
            or (type=1235 and  issue in(1936,1937,1938,1939,1940))
            or (type=1232 and  issue in(1949,1950))
            or (type=1237 and  issue in(1855,1856,1857,1858,1859))
            or (type=1239 and  issue in(1877,1878,1879,1880))
            or (type=1241 and  issue in(1896,1897))
            or (type=1243 and  issue in(2002))
            or (type=1217 and  issue in(1598,1599,1900,1659)) or 
                -- 2022/5/19 添加
            (type=1246 and issue in(2033 ,2034 ,2035 ,2036)) or 
            (type=1256 and issue in(2085,2086)) or 
            (type=1266 and issue in(2081,2083,2139,2140)) or 
            (type=1273 and issue in(2097,2098)) or 
            (type=1258 and issue in(2089)) or 
            (type=1260 and issue in(2091)) or 
            (type=1276 and issue in(2143))
            )
        )a  
        left join 
        (
            select jkx_userid,hb_id,date(part_day) as dt2 
            from zhwdb.zhw_hongbao_order 
            where part_month>='2022-03'
        ) b 
        on a.id=b.hb_id 
        group by 1,2,3,4
    ) c
    left join 
    (
        select date(part_day) as dt3,id,userid,pm 
        from zhwdb.zhw_dingdan 
        where part_month>='2022-03'
    ) d 
    on c.jkx_userid=d.userid and d.dt3>c.mdt2 and d.dt3<=date_add('day',7,c.mdt2)
    group by 1,2,3
'''.format(c)
result_9 = pd.read_sql(sql_9, connect_hive)
print('success :result_9')

sql_10 = '''
/*15日付费留存率*/
    select 
    c.part_day,
    c.type,
    c.issue,
    count(distinct d.userid) as fifteen_fufei_user
    from 
    (
        select a.part_day,a.type,a.issue,b.jkx_userid,max(b.dt2) as mdt2 from 
        (
            select part_day,type,issue,id,jkx_userid from zhwdb.zhw_hongbao  where part_month>='2022-03' 
            and ((type=1198  and issue in{0}) or 
            (type=1246 and  issue in(1908, 1909, 1910, 1911, 2013))
            or (type=1248 and  issue in(1916, 1917))
            or (type=1235 and  issue in(1936,1937,1938,1939,1940))
            or (type=1232 and  issue in(1949,1950))
            or (type=1237 and  issue in(1855,1856,1857,1858,1859))
            or (type=1239 and  issue in(1877,1878,1879,1880))
            or (type=1241 and  issue in(1896,1897))
            or (type=1243 and  issue in(2002))
            or (type=1217 and  issue in(1598,1599,1900,1659)) or 
                -- 2022/5/19 添加
            (type=1246 and issue in(2033 ,2034 ,2035 ,2036)) or 
            (type=1256 and issue in(2085,2086)) or 
            (type=1266 and issue in(2081,2083,2139,2140)) or 
            (type=1273 and issue in(2097,2098)) or 
            (type=1258 and issue in(2089)) or 
            (type=1260 and issue in(2091)) or 
            (type=1276 and issue in(2143))
            )
        )a  
        left join 
        (select jkx_userid,hb_id,date(part_day) as dt2 from zhwdb.zhw_hongbao_order where part_month>='2022-03')b 
        on a.id=b.hb_id 
        group by 1,2,3,4
    )c
    left  join 
    (
        select date(part_day) as dt3,id,userid,pm 
        from zhwdb.zhw_dingdan 
        where part_month>='2022-03'
    )d 
    on c.jkx_userid=d.userid and d.dt3>c.mdt2 and d.dt3<=date_add('day',15,c.mdt2)
    group by 1,2,3
'''.format(c)
result_10 = pd.read_sql(sql_10, connect_hive)
print('success :result_10')

result = pd.merge(result_1, result_2, on=['part_day', 'type', 'issue'], how='left')
result = pd.merge(result, result_3, on=['part_day', 'type', 'issue'], how='left')
result = pd.merge(result, result_4, on=['part_day', 'type', 'issue'], how='left')
result = pd.merge(result, result_5, on=['part_day', 'type', 'issue'], how='left')
result = pd.merge(result, result_6, on=['part_day', 'type', 'issue'], how='left')
result = pd.merge(result, result_7, on=['part_day', 'type', 'issue'], how='left')
result = pd.merge(result, result_8, on=['part_day', 'type', 'issue'], how='left')
result = pd.merge(result, result_9, on=['part_day', 'type', 'issue'], how='left')
result = pd.merge(result, result_10, on=['part_day', 'type', 'issue'], how='left')

result.to_sql(name='zqy_duanxin_zhaohui_new', con=connect_yy, if_exists='replace', index=False)




# 红包类型，红包id，红包任务
red_packet_type_tuple = (1198,1246, 1248, 1235, 1232, 1237, 1239, 1241, 1243, 1217, 1246, 1256, 1266, 1273, 1258, 1260, 1276)
red_packet_id_tuple = (1908, 1909, 1910, 1911, 2013,1916, 1917,1936,1937,1938,1939,1940,1949,1950,1855,1856,1857,1858,1859,1877,1878,1879,1880,1896,1897,2002,1598,1599,1900,1659,2033, 2034, 2035, 2036, 2085, 2086, 2081, 2083, 2139, 2140, 2097, 2098, 2089, 2091, 2143)
red_packet_id_tuple = red_packet_id_tuple + c
"""
                                    红包类型，红包id 发送短信数据统计
"""
sql_task_record = """
    select task_id,red_packet_ids, red_packet_type
    from hive.zhwhigh.zhw_task_record
    where red_packet_ids  is not null 
    and red_packet_type in {}
    group by 1,2,3
""".format(red_packet_type_tuple)
result_task_record = pd.read_sql(sql_task_record, connect_hive)

task_id_, red_packet_type_, red_packet_ids_ = [], [], []
for idx, item in result_task_record.iterrows():
    task_id = item['task_id']
    red_packet_type = item['red_packet_type']
    red_packet_ids = [int(a) for a in item['red_packet_ids'].split(',')]

    tmp_len = len(red_packet_ids)

    task_id_.extend([task_id] * tmp_len)
    red_packet_type_.extend([red_packet_type] * tmp_len)
    red_packet_ids_.extend(red_packet_ids)

df_task_record = pd.DataFrame(
    {'task_id_': task_id_, 'red_packet_type_': red_packet_type_, 'red_packet_ids_': red_packet_ids_})
task_id_tuple = tuple(task_id_)
df_task_record.to_excel(r"./tmp.xlsx")
df_task_record_2 = df_task_record[df_task_record.red_packet_ids_.isin(red_packet_id_tuple)]

# 红包任务下的短信发送记录
sql_send_sms = """
    select format_datetime(create_time,'yyyy-MM-dd') as create_dt,task_id,count(*) as send_sms_count
    from hive.zhwhigh.zhw_log_send_sms
    where task_id in {}
    and format_datetime(create_time,'yyyy-MM')>='2022-03'
    group by 1,2
""".format(task_id_tuple)
result_send_sms = pd.read_sql(sql_send_sms, connect_hive)

df_ = pd.merge(result_send_sms, df_task_record_2, left_on=['task_id'], right_on=['task_id_'])
df_.to_sql(name='red_packet_type_id_msg', con=connect_yy, if_exists='replace', index=False)


