# _*_coding:utf-8 _*_
# @Time     :2023/11/9 17:34
# @Author   : anliu
# @File     :some_note.py
# @Theme    :PyCharm

# 每2秒钟执行任务
schedule.every(2).seconds.do(job)
# 每十分钟执行任务
# schedule.every(10).minutes.do(job)
# 每个小时执行任务
# schedule.every().hour.do(job)
# # 每天的10:30执行任务
# schedule.every().day.at("10:30").do(job)
# # 每个月执行任务
# schedule.every().monday.do(job)
# # 每个星期三的13:15分执行任务
# schedule.every().wednesday.at("13:15").do(job)
# # 每分钟的第17秒执行任务
# schedule.every().minute.at(":17").do(job)
