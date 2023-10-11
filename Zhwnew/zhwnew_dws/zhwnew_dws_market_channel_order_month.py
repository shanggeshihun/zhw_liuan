# _*_coding:utf-8 _*_

# @Time      : 2023/2/19  18:26
# @Author    : An
# @File      : zhwnew_dws_market_channel_order_month.py
# @Software  :


import time, datetime, configparser, warnings, math, platform
import sys
import pymysql, presto
# import pypresto as pypresto
from WorkWeixinRobot.work_weixin_robot import WWXRobot
from sqlalchemy import create_engine

plat = platform.system().lower()
if plat == 'windows':
    sys.path.append("E:/工作文件\在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig")
elif plat == 'linux':
    sys.path.append("/work/project/zhw_product/liuan/PublicConfig")
else:
    sys.exit()

from OperateMysqlNew import OperateMysqlNew
from OperatePresto import OperatePresto
from OperateHologresNew import OperateHologresNew


class ZhwnewDwsMarketChanelOrderMonth:
    def __init__(self):
        warnings.filterwarnings("ignore")
        # ------------------------数据库配置读取----------------------------
        cf = configparser.ConfigParser()
        if cf.read("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig/config.ini", encoding='utf-8') == []:
            """服务器模式"""
            cf.read("/home/zhwom/config/config.ini", encoding='utf-8')
        else:
            """本地模式"""
            cf.read("E:/工作文件/在刀锋/dofun/Python/Python/zhw_liuan/PublicConfig/config.ini", encoding='utf-8')

        self.final_result_log = {}

        # 运营数据库
        self.mysql_host = cf.get("Mysql-data_yunying", "host")
        self.mysql_user = cf.get("Mysql-data_yunying", "user")
        self.mysql_password = cf.get("Mysql-data_yunying", "password")
        self.mysql_db = cf.get("Mysql-data_yunying", "DB")
        self.mysql_port = cf.get("Mysql-data_yunying", "port")

        # 本地MySQL8.0
        # self.mysql_local8_host = cf.get("MySQL8.0-localhost", "host")
        # self.mysql_local8_user = cf.get("MySQL8.0-localhost", "user")
        # self.mysql_local8_password = cf.get("MySQL8.0-localhost", "password")
        # self.mysql_local8_db = cf.get("MySQL8.0-localhost", "DB")
        # self.mysql_local8_port = cf.get("MySQL8.0-localhost", "port")

        # hive数据库
        self.presto_host = cf.get("hive_presto", "host")
        self.presto_username = cf.get("hive_presto", "username")
        self.presto_port = cf.get("hive_presto", "port")
        self.presto_schema = cf.get("hive_presto", "schema")
        self.presto_catalog = cf.get("hive_presto", "catalog")

        # hologres数据库
        self.holo_host = cf.get("hologres-dofun", "host")
        self.holo_port = cf.get("hologres-dofun", "port")
        self.holo_database = cf.get("hologres-dofun", "DB")
        self.holo_user = cf.get("hologres-dofun", "user")
        self.holo_password = cf.get("hologres-dofun", "password")

    def run(self, start_day ,end_day):
        """
        :param start_day: 起始日期参数
        :param end_day: 终止日期参数
        :return: 抽奖主题：抽奖日期+用户维度  指标数据 统计数据写到MySQL
        """
        warnings.filterwarnings("ignore")

        # 实例化mysql
        operate_mysql = OperateMysqlNew(
            username=self.mysql_user,
            password=self.mysql_password,
            host_ip=self.mysql_host,
            port=int(self.mysql_port),
            database=self.mysql_db
        )

        # 实例化mysql 本地 8.0
        # operate_mysql_local8 = OperateMysqlNew(
        #     username=self.mysql_local8_user,
        #     password=self.mysql_local8_password,
        #     host_ip=self.mysql_local8_host,
        #     port=int(self.mysql_local8_port),
        #     database=self.mysql_local8_db
        # )

        # 实例化hive数据库
        operate_presto = OperatePresto(
            username=self.presto_username,
            host_ip=self.presto_host,
            port=int(self.presto_port),
            catalog=self.presto_catalog,
            schema=self.presto_schema
        )

        # 实例化Hologres，查询待更新的数据信息
        operate_hologres = OperateHologresNew(
            username=self.holo_user,
            password=self.holo_password,
            host_ip=self.holo_host,
            port=int(self.holo_port),
            database=self.holo_database
        )

        # 清理目标表数据
        mysql_sql = "delete from zhwnew_dws_market_channel_order_month where part_month between '{0}' and '{1}'".format(start_day[:7] ,end_day[:7])
        operate_mysql.delete_data(mysql_sql)

        # 原始表 数据处理
        holo_sql = """
            with market_user_tmp as (
                    select a.jkx_userid, a.jkx_timer as jkx_day,
                    b.big_channel,b.channel_type,b.channel_id,b.channel
                    from ods_zhw.zhw_bigdata_market_user a
                    left join 
                    (
                        select '网吧端' as  big_channel,name as channel_type,app_channel as channel_id,app_channel_name as channel 
                        from ods_zhw.zhw_bi_channel_config 
                        where status=1 
                        and name in ('列表页') 
                        group by 1,2,3,4
                    ) b 
                    on a.wordid=b.channel_id
                    
                    union all
                    select a.jkx_userid, a.jkx_timer as jkx_day,
                    c.big_channel,c.channel_type,c.channel_id,c.channel
                    from ods_zhw.zhw_bigdata_market_user a
                    left join 
                    (
                        select '网吧端' as  big_channel,name as channel_type,app_channel as channel_id,app_channel_name as channel 
                        from ods_zhw.zhw_bi_channel_config 
                        where status=1 
                        and name in ('落地页') 
                        group by 1,2,3,4
                    ) c
                    on cast(a.browser_id as varchar)=c.channel_id
                    
                    union all
                    select a.jkx_userid, a.jkx_timer as jkx_day,
                    d.big_channel,d.channel_type,d.channel_id,d.channel
                    from ods_zhw.zhw_bigdata_market_user a
                    left join 
                    (
                        select 'SEM端' as big_channel,
                        case when name in ('品牌专区','360品专','品牌专区') then '品牌专区' else 'SEM搜索' end as  channel_type,
                        app_channel as channel_id,app_channel_name as channel 
                        from ods_zhw.zhw_bi_channel_config
                        where status=1 
                        and name in ('品牌专区','SEM搜索','360品专','百度品专') 
                        group by 1,2,3,4
                    ) d 
                    on a.browser=d.channel_id
                    
                    union all
                    select a.jkx_userid, a.jkx_timer as jkx_day,
                    e.big_channel,e.channel_type,e.channel_id,e.channel
                    from ods_zhw.zhw_bigdata_market_user a
                    left join 
                    (
                        select '移动端' as  big_channel,name as channel_type,app_channel as channel_id,app_channel_name as channel 
                        from ods_zhw.zhw_bi_channel_config
                        where status=1 
                        and name in ('APP安卓','APPPro','AppLite','AppuLite','APPLite') 
                        group by 1,2,3,4
                    ) e 
                    on a.channelname=e.channel_id
                    
                    union all
                    select a.jkx_userid, a.jkx_timer as jkx_day,
                    f.big_channel,f.channel_type,f.channel_id,f.channel
                    from ods_zhw.zhw_bigdata_market_user a
                    left join 
                    (
                        select '网吧端' as  big_channel,name as channel_type,app_channel as channel_id,app_channel_name as channel 
                        from ods_zhw.zhw_bi_channel_config
                        where status=1 
                        and name in ('网吧渠道') 
                        group by 1,2,3,4
                    ) f
                    on a.wangba_id=f.channel_id
            ),
            gmv_tmp as (
                select substring(b.part_day,1,7) as part_month, 
                a.big_channel,
                a.channel_type,a.channel,a.channel_id, 
                count(case when b.part_day = a.jkx_day then b.id end) as reg_orders,
                count(case when b.part_day <> a.jkx_day then b.id end) as old_orders,
                sum(b.pm) as order_money,
                sum(f.sys_fee) as s_fee  , -- 系统手续费
                sum(f.channel_fee) as c_fee, -- 渠道手续费
                sum(f.add_fee) as add_fee,-- 官方加价
                sum(d.add_money) as sub_add_fee,-- 二级加价
                sum(case when b.part_day = a.jkx_day then coalesce(f.sys_fee,0.00) +coalesce(f.channel_fee) end) as reg_fee,
                sum(case when b.part_day <> a.jkx_day then coalesce(f.sys_fee,0.00) +coalesce(f.channel_fee) end) as old_fee,
                count(case when b.zt in (3) then b.id end ) as cd_orders
                from market_user_tmp a 
                inner join 
                (
                    select part_day, userid, id,zt,pm,pmoney
                    from ods_zhw.zhw_dingdan 
                    where part_day between '{0}' and '{1}'
                ) b 
                on b.userid = a.jkx_userid
                left join 
                (
                    select dingdan_id,relet_give_hour 
                    from ods_zhw.zhw_dingdan_rent_give 
                    where true 
                    and part_month between to_char(date('{0}'),'yyyy-mm') and to_char(date('{1}'),'yyyy-mm')
                    and to_char(add_time ,'yyyy-mm-dd') between '{0}' and '{1}'
                ) c
                on b.id=c.dingdan_id
                left join 
                (
                    select order_id,add_money,pay_money ejjj 
                    from ods_zhw.zhw_dingdan_channel_addmoney  
                    where part_day between '{0}' and '{1}'
                ) d  -- 二级加价
                on b.id=d.order_id
                left join 
                (
                    select order_id,sys_fee ,channel_fee ,add_fee 
                    from ods_zhw.zhw_dingdan_fee 
                    where true 
                    and part_month between to_char(date('{0}'),'yyyy-mm') and to_char(date('{1}'),'yyyy-mm')
                ) f -- 渠道手续费、系统手续费、官方加价
                on b.id = f.order_id
                group by 1,2,3,4,5
            ),
            bt_tmp as (
                select substring(s.part_day,1,7) as part_month,
                a.big_channel,a.channel_type,a.channel,a.channel_id,
                sum(s.use_money)-sum(y.back_money) as use_hb_money,
                sum(x.sys_spend) as sys_spend
                from market_user_tmp a 
                join 
                (
                    select part_day,order_id,jkx_userid,use_money 
                    from ods_zhw.zhw_hongbao_order  
                    where part_day between '{0}' and '{1}'
                ) s   
                on s.jkx_userid = a.jkx_userid
                left join 
                (
                    select part_day,order_id,sum(back_money) as back_money
                    from ods_zhw.zhw_hongbao_order_back 
                    where part_day between '{0}' and '{1}'
                    group by 1,2
                ) y
                on s.order_id=y.order_id
                left join 
                (
                    select to_char(create_time,'yyyy-mm-dd') as part_day,order_id,sum(sys_spend) as sys_spend
                    from ods_zhw.zhw_share_card_order 
                    where true 
                    and part_month between to_char(date('{0}'),'yyyy-mm') and to_char(date('{1}'),'yyyy-mm')
                    and to_char(create_time,'yyyy-mm-dd') between '{0}' and '{1}'
                    group by 1,2
                ) x 
                on s.order_id = x.order_id 
                group by 1,2,3,4,5
            )
            
            select 
                coalesce(a.part_month,b.part_month) as part_month,
                coalesce(a.big_channel,b.big_channel) as big_channel,
                coalesce(a.channel_type,b.channel_type) as channel_type,
                coalesce(a.channel,b.channel) as channel,
                coalesce(a.channel_id,b.channel_id) as channel_id,
                max(a.reg_orders) as reg_orders,
                max(a.old_orders) as old_orders,
                max(a.order_money) as order_money,
                max(a.s_fee) as s_fee,
                max(a.c_fee) as c_fee,
                max(a.add_fee) as add_fee,
                max(a.sub_add_fee) as sub_add_fee,
                max(a.reg_fee) as reg_fee,
                max(a.old_fee) as old_fee,
                max(a.cd_orders) as cd_orders,
                max(b.use_hb_money) as use_hb_money,
                max(b.sys_spend) as sys_spend
            from gmv_tmp a 
            full join bt_tmp b 
            on a.part_month = b.part_month and a.big_channel = b.big_channel
            and a.channel_type = b.channel_type and a.channel = b.channel and a.channel_id = b.channel_id 
            group by 1,2,3,4,5
        """.format(start_day ,end_day)

        holo_data_list = operate_hologres.query_data(holo_sql)

        operate_hologres_log = {}
        for k, v in operate_hologres.operate_result.items():
            if v[0] != '无':
                operate_hologres_log[k] = v

        operate_hologres.close_conn()

        # 目标表 数据写入
        columns = 17
        insert_sql = "insert into zhwnew_dws_market_channel_order_month(part_month ,big_channel ,channel_type ,channel ,channel_id ,reg_orders ,old_orders ,order_money ,s_fee ,c_fee ,add_fee ,sub_add_fee ,reg_fee ,old_fee ,cd_orders ,use_hb_money ,sys_spend) values ({0});".format(','.join(['%s'] * columns))
        operate_mysql.insert_data(insert_sql, holo_data_list)

        operate_mysql_log = {}
        for k, v in operate_mysql.operate_result.items():
            if v[0] != '无':
                operate_mysql_log[k] = v

        operate_mysql.close_conn()

        # 数据日志记录
        self.final_result_log['operate_hologres_log'] = operate_hologres_log
        self.final_result_log['operate_mysql'] = operate_mysql_log


if __name__ == '__main__':

    today = (datetime.datetime.now()).strftime('%Y-%m-%d')  # 今日日期
    today_hour = (datetime.datetime.now()).strftime('%Y%m%d%H')  # 今日日期小时
    today_last_hour = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y%m%d%H')  # h
    today_last_hour_ = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y-%m-%d-%H')  # h

    now_hour = datetime.datetime.now().hour
    now_last_hour = now_hour - 1


    start_time = time.time()
    start_day, last_day = '2022-12-01','2023-02-18'
    zhwnew_dws_market_channel_order_month = ZhwnewDwsMarketChanelOrderMonth()
    zhwnew_dws_market_channel_order_month.run(start_day ,last_day)
    end_time = time.time()
    print(last_day, '运行耗时：', end_time - start_time)
    time.sleep(3)
    
    # for i in range(1,22):
    #
    #     start_time = time.time()
    #     last_day = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y-%m-%d')  # t-1
    #     zhwnew_dws_market_channel_order_month = ZhwnewDwsMarketChanelOrderMonth()
    #     zhwnew_dws_market_channel_order_month.run(last_day ,last_day)
    #     end_time = time.time()
    #     print(last_day, '运行耗时：', end_time - start_time)
    #     time.sleep(3)