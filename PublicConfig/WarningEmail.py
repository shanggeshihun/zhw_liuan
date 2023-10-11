# _*_coding:utf-8 _*_
# @Time　　 :2021/6/22/022   17:12
# @Author　 : Antipa
# @File　　 :WarningEmail.py
# @Theme    :PyCharm

import smtplib
from email.mime.text import MIMEText
from email.header import Header
def warning_email(mail_content ):
    from_addr='1569873132@qq.com'   #邮件发送账号
    qqCode='hyhnhdbfwveibacd'   #授权码（这个要填自己获取到的）
    to_addrs='getfunc@163.com'   #接收邮件账号
    smtp_server='smtp.qq.com'#固定写死
    smtp_port=465#固定端口

    #配置服务器
    stmp=smtplib.SMTP_SSL(smtp_server,smtp_port)
    stmp.login(from_addr,qqCode)

    #组装发送内容
    mail_title = "Python邮件预警系统"

    message = MIMEText(mail_content , 'plain', 'utf-8')   #发送的内容
    message['From'] = Header(mail_title, 'utf-8')   #发件人

    message['To'] = Header("管理员")   #收件人
    subject = '【预警提醒】'
    message['Subject'] = Header(subject, 'utf-8')  #邮件标题

    stmp.sendmail(from_addr, to_addrs, message.as_string())

def warning_email1(subject,mail_content,file_path=None):

    msg = MIMEMultipart()

    text = MIMEText(mail_content)
    msg.attach(text)

    docFile = r'E:\工作文件\在刀锋\dofun\Python\Python\zhw_liuan\PublicConfig\test_email.py'  # 如果需要添加附件，就给定路径
    if file_path:  # 最开始的函数参数我默认设置了None ，想添加附件，自行更改一下就好
        docFile = file_path
    docApart = MIMEApplication(open(docFile, 'rb').read())
    docApart.add_header('Content-Disposition', 'attachment', filename=docFile)
    msg.attach(docApart)

    subject = subject  # 主题
    msg_from,passwd = "1569873132@qq.com","hyhnhdbfwveibacd"
    msg_to = "getfunc@163.com"

    msg['Subject'] = subject
    msg['From'] = msg_from
    msg['To'] = msg_to

    try:
        s = smtplib.SMTP_SSL("smtp.qq.com", 465)
        s.login(msg_from, passwd)
        s.sendmail(msg_from, msg_to, msg.as_string())
        print("发送成功")
    except smtplib.SMTPException as e:
        print("发送失败")
    finally:
        s.quit()


from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
def sen_email(msg_from, passwd, msg_to, text_content, file_path=None):

    msg = MIMEMultipart()

    subject = "Test My Email"  # 主题

    text = MIMEText(text_content)
    msg.attach(text)

    docFile = r'E:\工作文件\在刀锋\dofun\Python\Python\zhw_liuan\PublicConfig\test_email.py'  # 如果需要添加附件，就给定路径
    if file_path:  # 最开始的函数参数我默认设置了None ，想添加附件，自行更改一下就好
        docFile = file_path
    docApart = MIMEApplication(open(docFile, 'rb').read())
    docApart.add_header('Content-Disposition', 'attachment', filename=docFile)
    msg.attach(docApart)

    msg['Subject'] = subject
    msg['From'] = msg_from
    msg['To'] = msg_to

    try:
        s = smtplib.SMTP_SSL("smtp.qq.com", 465)
        s.login(msg_from, passwd)
        s.sendmail(msg_from, msg_to, msg.as_string())
        print("发送成功")
    except smtplib.SMTPException as e:
        print("发送失败")
    finally:
        s.quit()

if __name__ == '__main__':
    # title = '11111111'
    # warning_email(title)

    warning_email1()

    # msg_from = '1569873132@qq.com' # 发送方邮箱
    # passwd = 'hyhnhdbfwveibacd' # 填入发送方邮箱的授权码（就是刚刚你拿到的那个授权码）
    # msg_to = 'getfunc@163.com' # 收件人邮箱
    # text_content = "你好啊，你猜这是谁发的邮件"
    #
    # sen_email(msg_from, passwd, msg_to, text_content, file_path=None)
