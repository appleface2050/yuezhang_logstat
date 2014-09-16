# -*- coding: utf-8 -*-


import smtplib
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
import sys
import datetime

sys.path.append(os.path.dirname(os.path.split(os.path.realpath(__file__))[0]))

class SendEmail(object):
    # 六版数据优于接口超时没跑成功邮件发送列表
    mailto_list=["shangzongkai@zhangyue.com"] 
    # 设置服务器名称、用户名、密码以及邮件后缀 
    mail_host = "smtp.zhangyue.com" 
    mail_user = "sjfx_no_reply@zhangyue.com" 
    mail_pass = "zhangyue123"
    mail_postfix="zhangyue.com"
    
    def __init__(self, to_list, sub, context):
        self._to_list = to_list
        self._sub = sub
        self._context = context
        #self.__content_type = """Content-Type: text/html; charset=\"GBK\""""

    def send_mail(self):
        '''
        to_list: 发送给谁
        sub: 主题
        context: 内容
        send_mail("xxx@126.com","sub","context")
        '''

        me = self.mail_user + "<"+self.mail_user+"@"+self.mail_postfix+">"
        msg = MIMEText(self._context)
        msg['Subject'] = self._sub 
        msg['From'] = me 
        msg['To'] = ";".join(self._to_list)
        try: 
            send_smtp = smtplib.SMTP() 
            send_smtp.connect(self.mail_host)
            send_smtp.login(self.mail_user, self.mail_pass) 
            send_smtp.sendmail(me, self._to_list, msg.as_string()) 
            send_smtp.close() 
            return True
        except Exception, e: 
            print e 
            return False 

    def send_mail_html(self):

        me = self.mail_user + "<"+self.mail_user+"@"+self.mail_postfix+">"
        msg = MIMEText(self._context,'html','utf-8')
        #msg = MIMEText(context)
        msg['Subject'] = self._sub
        msg['From'] = me 
        msg['To'] = ";".join(self._to_list)
        try: 
            send_smtp = smtplib.SMTP() 
            send_smtp.connect(self.mail_host)
            send_smtp.login(self.mail_user, self.mail_pass) 
            send_smtp.sendmail(me, self._to_list, msg.as_string()) 
            send_smtp.close() 
            return True
        except Exception, e: 
            print e 
            return False 


if __name__ == '__main__':
    mailto_list=["shangzongkai@zhangyue.com"]     
    subject = "ERROR!!! logstat data break down"
    html="我们 logstat data break down \n {{{(>_<)}}} \n\n\n\n\n\n\n\n\n\n\n\n\n auto email don\'t reply"  
 #   s = SendEmail()
 #   if (True == s.send_mail(mailto_list,subject,html)): 
 #       print ("发送成功")
 #   else:
 #       print ("发送失败")
    
    s = SendEmail(mailto_list, subject, html)
    if s.send_mail_html():
        print ("发送成功")
    else:
        print ("发送失败")







