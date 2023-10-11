# _*_coding:utf-8 _*_

# @Time      : 2023/1/19  13:41
# @Author    : An
# @File      : qqexmail_smtp.py
# @Software  : PyCharm

import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
import logging
import time

from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

class QqExmailSmtp():
    def __init__(self, user, password):
        # 发件人邮箱账号
        self.user = user
        # user登录邮箱的用户名，password登录邮箱的密码（授权码，即客户端密码，非网页版登录密码），但用腾讯邮箱的登录密码也能登录成功
        self.password = password

    def connect(self):
        try:
            self.smtp_host = "smtp.exmail.qq.com"
            self.smtp_port = 465
            self.smtp = smtplib.SMTP_SSL(self.smtp_host, self.smtp_port)
            self.smtp.set_debuglevel(1)
            self.smtp.login(self.user, self.password)
        except Exception:
            raise (Exception)

    def close(self):
        self.smtp.quit()

    def mail_to(self, to_addrs, subject, text_content, attachment_path):
        """
        发送邮件到指定地址（也可以是地址列表或用,拼接的字符串）
        """
        msg = MIMEMultipart()
        # 正文
        text_content = MIMEText(text_content)
        msg.attach(text_content)

        if attachment_path:  # 最开始的函数参数我默认设置了None ，想添加附件，自行更改一下就好
            doc_app = MIMEApplication(open(attachment_path, 'rb').read())
            doc_app.add_header('Content-Disposition', 'attachment', filename=attachment_path.split('\\')[-1])
            msg.attach(doc_app)

        if type(to_addrs) == str:
            msg['To'] = to_addrs
            to_addrs = to_addrs.split(',')
        elif type(to_addrs) == list:
            msg['To'] = ','.join(to_addrs)
        else:
            logging.error("邮件地址类型错误，必须为str（可以用,拼接）或list")
            return

        time.sleep(1)
        msg['From'] = formataddr(["自动发送的邮件", self.user])
        # 邮件的主题
        msg['Subject'] = subject

        try:
            ret = self.smtp.sendmail(self.user, to_addrs, msg.as_string())
            logging.info("成功发送邮件至：{} {}".format(to_addrs, ret))
        except Exception:
            logging.error("发送邮件失败: {} {} {}".format(to_addrs, ret, Exception))

if __name__ == "__main__":
    content_template = """
    您好！

    您的账号信息如下：
    用户名：{}
    密码：{}

    ----------------------------------------------------------------
    本邮件为自动发出，无需回复，谢谢！
    """
    content = content_template.format("username", "password")

    qqexmail_smtp = QqExmailSmtp('anliu@dofun.cn', 'Shui1520')
    qqexmail_smtp.connect()

    to_addrs = ['anliu@dofun.cn']
    subject = '您的账号信息'

    attachment_path= r'E:\工作文件\在刀锋\dofun\Python\Python\zhw_liuan\PublicConfig\ReadSql.py'
    qqexmail_smtp.mail_to(to_addrs,subject, content ,attachment_path)
    qqexmail_smtp.close()