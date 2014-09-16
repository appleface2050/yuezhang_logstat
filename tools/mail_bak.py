# -*- coding: utf-8 -*-

# 导入 smtplib 和 MIMEText 
import smtplib 
from email.mime.text import MIMEText 
import os
import sys
import datetime

sys.path.append(os.path.dirname(os.path.split(os.path.realpath(__file__))[0]))

from model.stat import BookStat 
from lib.utils import time_start
from service import Service
from tools.mail_config import *


# 定义发送列表 
mailto_list=["shangzongkai@zhangyue.com"] 

# 设置服务器名称、用户名、密码以及邮件后缀 
mail_host = "smtp.zhangyue.com" 
mail_user = "shangzongkai@zhangyue.com" 
mail_pass = "L?q83H" 
mail_postfix="zhangyue.com"

# 取数据
def get_book_data(cp, time):
    q = BookStat.mgr().Q(time=time).filter(scope_id=1,mode='day',time=time)
    books = q[:]
    books = Service.inst().fill_book_info(books)
    res = []
    for i in books:
        if i['cp'].encode("UTF-8") == cp:
            res.append(i)
    return res

# 发送邮件函数 
def send_mail(to_list, sub, context): 
    '''''
    to_list: 发送给谁
    sub: 主题
    context: 内容
    send_mail("xxx@126.com","sub","context")
    ''' 
    me = mail_user + "<"+mail_user+"@"+mail_postfix+">" 
    #msg = MIMEText(context) 
    msg = MIMEText(html,'html') 
    msg['Subject'] = sub 
    msg['From'] = me 
    msg['To'] = ";".join(to_list) 
    try: 
        send_smtp = smtplib.SMTP() 
        send_smtp.connect(mail_host) 
        send_smtp.login(mail_user, mail_pass) 
        send_smtp.sendmail(me, to_list, msg.as_string()) 
        send_smtp.close() 
        return True 
    except Exception, e: 
        print(str(e)) 
        return False 
        
def get_date(self, delta=0):
    tody = time_start(datetime.datetime.now()-datetime.timedelta(days=delta),'day')
    return tody

def fill_context(books):
    title = u"书ID   书名    作者    版权    类别    子类    计费类型    状态    月饼消费    付费下载数  付费下载用户数  免费下载数  免费下载用户数  简介访问数  简介访问人数\n"
    
    #title = title.decode('gbk')
    title = unicode(title)
    context = ""
    context += title
    for i in books:
        #print type(i['state'])
        context += str(i['book_id']) + '\t' + i['name'] + '\t' +  i['author'] + '\t' + i['cp'] + '\t' + i['category_0'] + '\t' + i['category_1'] + '\t'
        context += unicode(i['charge_type']) + '\t' 
        #context += i['state'] + '\t' 
        context += str(i['fee']) + '\t' + str(i['pay_down']) + '\t' + str(i['pay_user']) + '\t'
        context += str(i['free_down']) + '\t' + str(i['free_user']) + '\t' + str(i['pv']) + '\t' + str(i['uv']) 
        context += '\n'
    context = context.encode("UTF-8")  
    return context,title

if __name__ == '__main__': 
   
    #date
    yest = get_date(1) - datetime.timedelta(days=1)
    books = get_book_data(CP,yest)
    
    #qq = u"作者"
    #qq.encode('gb2312')

    #context,title = fill_context(books)
    html='<html><body>hello world</body></html>'  
    if (True == send_mail(mailto_list,"subject",html)): 
        print ("发送成功")
    else: 
        print ("发送失败")
 










