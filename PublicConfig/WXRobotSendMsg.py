# _*_coding:utf-8 _*_

#@Time      : 2022/1/30  0:48
#@Author    : An
#@File      : WXRobotSendMsg.py
#@Software  : PyCharm

from WorkWeixinRobot.work_weixin_robot import WWXRobot

import urllib3
urllib3.disable_warnings()

import requests,json

class WXSendMsg():
    def __init__(self,Webhook):
        self.Webhook = Webhook

    def send_markdown_msg(self):
        headers = {"Content-Type": "application/json'"}
        data = {
            "msgtype": "markdown",
            "markdown": {
                 "content": "# **提醒！实时新增用户反馈**<font color=\"warning\">**123例**</font>\n" +  # 标题 （支持1至6级标题，注意#与文字中间要有空格）
                    "#### **请相关同事注意，及时跟进！**\n" +  # 加粗：**需要加粗的字**
                    "> 类型：<font color=\"info\">用户反馈</font> \n" +  # 引用：> 需要引用的文字
                    "> 普通用户反馈：<font color=\"warning\">117例</font> \n" +  # 字体颜色(只支持3种内置颜色)
                    "> VIP用户反馈：<font color=\"warning\">6例</font>"
                }
        }
        json_data = json.dumps(data)
        r = requests.post(url=self.Webhook, data = json_data,headers = headers,verify=False)
        print(r.text)

    def send_markdown_msg_model(self,title_name,item_dict):
        # self.title_name = "一起家里蹲"
        #
        # self.item_dict = {
        #     "家里蹲一号": "睡觉",
        #     "家里蹲二号": "嗑瓜子"
        # }

        title = "# **{}**".format(title_name)

        content = ""
        content = content + title
        for k, v in item_dict.items():
            item = "> {0}:<font color=\"info\">{1}</font>".format(k, v)

            content = content + "\n" + item
        content = content + "\n@刘安"

        headers = {"Content-Type": "application/json"}

        data = {
            "msgtype": "markdown",
            "markdown": {
                "content": content,
                "mentioned_list": ["liuan@jld1141.wecom.work"]
            }
        }

        json_data = json.dumps(data)
        r = requests.post(url=self.Webhook, data=json_data, headers=headers, verify=False)
        print(r.text)

if __name__ == '__main__':
    Webhook= "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=d38dfede-dfc8-4bb1-b8d6-bcf888bf945e"
    wx_send_msg = WXSendMsg(Webhook)

    title_name = "一起家里蹲"
    
    item_dict = {
        "家里蹲一号": "睡觉",
        "家里蹲二号": "嗑瓜子"
    }
    wx_send_msg.send_markdown_msg_model(title_name,item_dict)
