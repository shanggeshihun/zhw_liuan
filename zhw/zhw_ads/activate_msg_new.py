# 目标表 zqy_activate_msg_new  BI已完成（激活召回效果评估）

# _*_coding:utf-8 _*_


# @Time      : 2022/5/18  15:37
# @Author    : An
# @File      : activate_msg_new.py
# @Software  : 2022/5/19 添加issue_id


# noinspection PyUnresolvedReferences
import pymysql
from pyhive import presto
import pandas as pd
# noinspection PyUnresolvedReferences
import numpy as np
# noinspection PyUnresolvedReferences
from sqlalchemy import create_engine
# noinspection PyUnresolvedReferences
from datetime import date
from dateutil.relativedelta import relativedelta

# 1、连接hive数据库
# 配置项
host_hive = '172.16.13.47'
username_hive = 'zhw'
port_hive = 8080
catalog_hive = 'hive'
schema_hive = 'zhwdb'
# 连接语句
connect_hive = presto.connect(host=host_hive, port=port_hive, username=username_hive, catalog=catalog_hive)

# 2、连接线上运营mysql数据库
# 配置项
host_yy = 'rm-2zez7u673640b68x1.mysql.rds.aliyuncs.com'
user_yy = 'data_yunying'
password_yy = 'yTMgWouzdRfVtmgj'
DB_yy = 'data_yunying'
port_yy = '3306'
connect_yy = create_engine(
    "mysql+pymysql://" + user_yy + ":" + password_yy + "@" + host_yy + ":" + port_yy + "/" + DB_yy, echo=False)
# 案例：cnx = create_engine("mysql+pymysql://"+user+":"+password+"@"+host+":"+port+"/"+DB, echo=False)
a = pd.read_csv(r'/work/project/zhw_product/rita/issue_id.txt')
b = pd.Series(a['id']).tolist()
c = tuple(b)  # 元组，用()表示
print(1)
# SQL语句---查找要更新的数据并更新
sql_1 = '''
select 
  t1.dt1 as "领取日期",
  t1.issue as "期数",
  t2."红包使用人数"*1.000/t1."领取人数" as "召回率",
  t3."7日复购人数"*1.000/t2."红包使用人数" as "7日复购率",
  t4."30日复购人数"*1.000/t2."红包使用人数" as "30日复购率",
  t1."领取数量",
  t1."领取人数",
  t1."领取金额",
  t2."红包使用数量",
  t2."红包使用人数",
  t2."红包订单数量",
  t2."红包使用金额",
  t2."红包订单金额",
  t3."7日订单量",
  t3."7日复购人数",
  t3."7日订单金额",
  t4."30日订单量",
  t4."30日复购人数",
  t4."30日订单金额",
  t5."3日内活跃人数",
  t5."3日内活跃人数"*1.000/t1."领取人数" as "3日内活跃率"
from 
(
    /*召回红包每一期的领取数量、领取人数、领取金额*/
    select date(from_unixtime(recetime)) as dt1,
    issue ,
    count(id) as "领取数量",
    count(distinct jkx_userid) as "领取人数",
    sum(money) as "领取金额"
    from zhwdb.zhw_hongbao 
    where  part_month>='2022-04' 
    and ((type=1198  and issue in{0}) or 
    (type=1246 and issue in(1908,1909,1910,1911,2013)) or 
    (type=1248 and issue in(1916,1917)) or 
    (type=1235 and issue in(1936,1937,1938,1939,1940)) or 
    (type=1232 and issue in(1949,1950)) or 
    (type=1237 and issue in(1855,1856,1857,1858,1859)) or 
    (type=1239 and issue in(1877,1878,1879,1880)) or 
    (type=1241 and issue in(1896,1897)) or 
    (type=1243 and issue=2002) or 
    (type=1217 and issue in(1598,1599,1900,1659)) or 
    -- 2022/5/19 添加
    (type=1246 and issue in(2033 ,2034 ,2035 ,2036)) or 
    (type=1256 and issue in(2085,2086)) or 
    (type=1266 and issue in(2081,2083,2139,2140)) or 
    (type=1273 and issue in(2097,2098)) or 
    (type=1258 and issue in(2089)) or 
    (type=1260 and issue in(2091)) or 
    (type=1276 and issue in(2143))
    )
    group by 1,2
) t1
--order by 1,2;
left join 
(
    /*召回红包每一期红包截止目前的使用数量、食用金额、订单金额数据*/
    select date(from_unixtime(a.recetime)) as dt1,
           a.issue ,
           count(distinct b.hb_id) as "红包使用数量",
           count(distinct b.jkx_userid) as "红包使用人数",
           count(distinct b.order_id) as "红包订单数量",
           sum(b.use_money) as "红包使用金额",
           sum(c.pm) as "红包订单金额"     
    from  zhwdb.zhw_hongbao a,
          zhwdb.zhw_hongbao_order b, 
          zhwdb.zhw_dingdan c 
    where  a.id=b.hb_id and b.order_id=c.id
    and   a.part_month>='2022-04' 
    and   b.part_month>='2022-03'
    and   c.part_month>='2022-03'
    and   ((a.type=1198  and a.issue in{0}) or 
    (a.type=1246 and a.issue in(1908,1909,1910,1911,2013)) or 
    (a.type=1248 and a.issue in(1916,1917)) or 
    (a.type=1235 and a.issue in(1936,1937,1938,1939,1940)) or 
    (a.type=1232 and a.issue in(1949,1950)) or 
    (a.type=1237 and a.issue in(1855,1856,1857,1858,1859)) or 
    (a.type=1239 and a.issue in(1877,1878,1879,1880)) or 
    (a.type=1241 and a.issue in(1896,1897)) or 
    (a.type=1243 and a.issue=2002) or 
    (a.type=1217 and a.issue in(1598,1599,1900,1659)) or 
    -- 2022/5/19 添加
    (a.type=1246 and a.issue in(2033 ,2034 ,2035 ,2036)) or 
    (a.type=1256 and a.issue in(2085,2086)) or 
    (a.type=1266 and a.issue in(2081,2083,2139,2140)) or 
    (a.type=1273 and a.issue in(2097,2098)) or 
    (a.type=1258 and a.issue in(2089)) or 
    (a.type=1260 and a.issue in(2091)) or 
    (a.type=1276 and a.issue in(2143))
    )
    group by 1,2
) t2
on t1.dt1 = t2.dt1 and t1.issue = t2.issue
left join 
(
    /*召回红包每个使用的用户的7日复购率*/
    select c.dt1 ,
    c.issue ,
    count(d.id) as "7日订单量",
    count(distinct d.userid) as "7日复购人数",
    sum(d.pm)  as "7日订单金额"
    from 
    (
        select a.dt1,a.issue,a.jkx_userid,max(b.dt2) as mdt2 
        from 
        (
            select date(from_unixtime(recetime)) as dt1,issue,id,money,jkx_userid 
            from zhwdb.zhw_hongbao 
            where part_month>='2022-04' 
            and ((type=1198  and issue in{0}) or 
            (type=1246 and issue in(1908,1909,1910,1911,2013,2033 ,2034 ,2035 ,2036)) or 
            (type=1248 and issue in(1916,1917)) or 
            (type=1235 and issue in(1936,1937,1938,1939,1940)) or 
            (type=1232 and issue in(1949,1950)) or 
            (type=1237 and issue in(1855,1856,1857,1858,1859)) or 
            (type=1239 and issue in(1877,1878,1879,1880)) or 
            (type=1241 and issue in(1896,1897)) or 
            (type=1243 and issue=2002) or 
            (type=1217 and issue in(1598,1599,1900,1659)) or
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
        inner join 
        (
            select jkx_userid,hb_id,date(from_unixtime(usetime)) as dt2 
            from zhwdb.zhw_hongbao_order 
            where part_month>='2022-04' 
            and ((type=1198  and issue in{0}) or 
            (type=1246 and issue in(1908,1909,1910,1911,2013)) or 
            (type=1248 and issue in(1916,1917)) or 
            (type=1235 and issue in(1936,1937,1938,1939,1940)) or 
            (type=1232 and issue in(1949,1950)) or 
            (type=1237 and issue in(1855,1856,1857,1858,1859)) or 
            (type=1239 and issue in(1877,1878,1879,1880)) or 
            (type=1241 and issue in(1896,1897)) or 
            (type=1243 and issue=2002) or 
            (type=1217 and issue in(1598,1599,1900,1659)) or 
            -- 2022/5/19 添加
            (type=1246 and issue in(2033 ,2034 ,2035 ,2036)) or 
            (type=1256 and issue in(2085,2086)) or 
            (type=1266 and issue in(2081,2083,2139,2140)) or 
            (type=1273 and issue in(2097,2098)) or 
            (type=1258 and issue in(2089)) or 
            (type=1260 and issue in(2091)) or 
            (type=1276 and issue in(2143))
            )
        )b 
        on a.id=b.hb_id 
        group by 1,2,3
    )c
    inner join 
    (
        select date(add_time) as dt3,id,userid,pm 
        from zhwdb.zhw_dingdan where part_month>='2022-03'
    )d 
    on c.jkx_userid=d.userid and d.dt3>c.mdt2 and d.dt3<=date_add('day',7,c.mdt2)
    group by 1,2
) t3
on t1.dt1 = t3.dt1 and t1.issue = t3.issue
left join 
(
    /*召回红包每个使用的用户的30日复购率*/
    select c.dt1,
    c.issue,
    count(d.id) as "30日订单量",
    count(distinct d.userid) as "30日复购人数",
    sum(d.pm)  as "30日订单金额"
    from 
    (
        select a.dt1,a.issue,b.jkx_userid,max(b.dt2) as mdt2 from 
        (
            select date(from_unixtime(recetime)) as dt1,issue,id,money,jkx_userid from zhwdb.zhw_hongbao 
            where part_month>='2022-04' 
            and ((type=1198  and issue in{0}) or 
            (type=1246 and issue in(1908,1909,1910,1911,2013)) or 
            (type=1248 and issue in(1916,1917)) or 
            (type=1235 and issue in(1936,1937,1938,1939,1940)) or 
            (type=1232 and issue in(1949,1950)) or 
            (type=1237 and issue in(1855,1856,1857,1858,1859)) or 
            (type=1239 and issue in(1877,1878,1879,1880)) or 
            (type=1241 and issue in(1896,1897)) or 
            (type=1243 and issue=2002) or 
            (type=1217 and issue in(1598,1599,1900,1659)) or 
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
        inner join 
        (
            select jkx_userid,hb_id,date(from_unixtime(usetime)) as dt2 from zhwdb.zhw_hongbao_order 
            where part_month>='2022-04' 
            and ((type=1198  and issue in{0}) or 
            (type=1246 and issue in(1908,1909,1910,1911,2013)) or 
            (type=1248 and issue in(1916,1917)) or 
            (type=1235 and issue in(1936,1937,1938,1939,1940)) or 
            (type=1232 and issue in(1949,1950)) or 
            (type=1237 and issue in(1855,1856,1857,1858,1859)) or 
            (type=1239 and issue in(1877,1878,1879,1880)) or 
            (type=1241 and issue in(1896,1897)) or 
            (type=1243 and issue=2002) or 
            (type=1217 and issue in(1598,1599,1900,1659)) or 
            -- 2022/5/19 添加
            (type=1246 and issue in(2033 ,2034 ,2035 ,2036)) or 
            (type=1256 and issue in(2085,2086)) or 
            (type=1266 and issue in(2081,2083,2139,2140)) or 
            (type=1273 and issue in(2097,2098)) or 
            (type=1258 and issue in(2089)) or 
            (type=1260 and issue in(2091)) or 
            (type=1276 and issue in(2143))
            )

        )b 
        on a.id=b.hb_id 
        group by 1,2,3
    )c
    inner join 
    (select date(add_time) as dt3,id,userid,pm from zhwdb.zhw_dingdan where part_month>='2022-03')d 
    on c.jkx_userid=d.userid and d.dt3>c.mdt2 and d.dt3<=date_add('day',30,c.mdt2)
    group by 1,2
) t4
--order by 1,2;
on t1.dt1 = t4.dt1 and t1.issue = t4.issue
left join 
/*召回红包每一期的领取红包的用户的3日内活跃*/
(
    select a.dt1,a.issue,count(distinct b.userid) as "3日内活跃人数" 
    from 
    (
        select date(from_unixtime(recetime)) as dt1,issue,jkx_userid 
        from zhwdb.zhw_hongbao 
        where  part_month>='2022-04' 
        and ((type=1198  and issue in{0}) or 
        (type=1246 and issue in(1908,1909,1910,1911,2013)) or 
        (type=1248 and issue in(1916,1917)) or 
        (type=1235 and issue in(1936,1937,1938,1939,1940)) or 
        (type=1232 and issue in(1949,1950)) or 
        (type=1237 and issue in(1855,1856,1857,1858,1859)) or 
        (type=1239 and issue in(1877,1878,1879,1880)) or 
        (type=1241 and issue in(1896,1897)) or 
        (type=1243 and issue=2002) or 
        (type=1217 and issue in(1598,1599,1900,1659)) or 
        -- 2022/5/19 添加
        (type=1246 and issue in(2033 ,2034 ,2035 ,2036)) or 
        (type=1256 and issue in(2085,2086)) or 
        (type=1266 and issue in(2081,2083,2139,2140)) or 
        (type=1273 and issue in(2097,2098)) or 
        (type=1258 and issue in(2089)) or 
        (type=1260 and issue in(2091)) or 
        (type=1276 and issue in(2143))
        )

        group by 1,2,3
)a  
inner  join 
(
    select date(usertimer) as dt2,userid from 
    zhwdb.zhw_user_login_log_extend  where part_month>='2022-03'
) b 
on a.jkx_userid=b.userid and b.dt2>=a.dt1 and b.dt2<=date_add('day',2,a.dt1)
group by 1,2)t5
on t1.dt1 = t5.dt1 and t1.issue = t5.issue
order by 1,2
'''.format(c)
# sql读取语句
result_1 = pd.read_sql(sql_1, connect_hive)
# 怎么写入线上数据库
result_1.to_sql(name='zqy_activate_msg_new', con=connect_yy, if_exists='replace', index=False)
# to_sql() 方法的 if_exists 参数用于当目标表已经存在时的处理方式，默认是 fail，即目标表存在就失败，另外两个选项是 replace 表示替代原表，即删除再创建，append 选项仅添加数据
# 案例：app_day.to_sql(name='table_name', con=cnx, if_exists = 'append', index=False)###插入指定的数据库的数据表，方式为插入，也可以改为replace替换