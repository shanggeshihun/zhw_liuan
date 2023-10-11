# -*- coding: utf-8 -*-
import time

import requests
from pyhive import presto


# 连接presto
conn = presto.connect(host='172.16.13.47', port='8080', username='zhw_uqp', catalog='hive', schema='zhwdb')

# 发送消息文本
def post_txt(url, txt):
    data_txt = {"msgtype": "text",
                "text": {"content": txt}
                }
    requests.post(url, json=data_txt).json()


def job():
    print('开始执行定时任务')
    cursor = conn.cursor()
    # 查询的SQL，指定表明和要查询的字段
    cursor.execute("""
    select 
        cheat_name "特征名",
        substr(cast(online_time as varchar),1,19) "上线时间",
        num "近一周上报数"
    from
        (
        select 
            cheat_name,
            online_time
        from safe_center.safe_center_backstage.wos_feature
        ) a 
    join 
        (
        select 
            inf,
            count(distinct jsm) as num
        from kudu.safe_center.safe_log_v5
        where typ='dfbox_cheatname' in 
        and part_day>=cast(date_add('day',-6,current_date) as varchar)
        group by 1
        ) b on a.cheat_name=b.inf
    order by 3 desc
        """)
    lst = ['#盒子特征近1周上报数据#']
    for result in cursor.fetchall():
        lst.append('' + str(result[0]))
        lst.append('上线时间：' + str(result[1]))
        lst.append('近一周上报数：' + str(result[2]))
    post_txt(
        # 机器人KEY
        #'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=90b62b41-a568-4951-a250-b3bf2086ffe8',
        'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=c73a13ec-e747-463a-98c4-236374d6bb79',
        # 取查询出来的内容，拼成一个字符串
        '\n'.join(lst)
    )
    print('执行定时任务完成')


if __name__ == "__main__":
    # 定时任务
    # schedule.every(1).minutes.do(job)
    # schedule.every().day.at("17:00").do(job)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)
    job()
