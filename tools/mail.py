# -*- coding: utf-8 -*-

# 导入 smtplib 和 MIMEText 
import smtplib 
from email.mime.text import MIMEText 
from email.mime.multipart import MIMEMultipart 
import os
import sys
import datetime
#from pyh import * 

sys.path.append(os.path.dirname(os.path.split(os.path.realpath(__file__))[0]))

from model.stat import BookStat 
from lib.utils import time_start
from service import Service
from tools.mail_config import *
from lib.excel import Excel
from model.wap import WapVisitStat

def bubblesortdesc(dict_in_list):
    for j in range(len(dict_in_list)-1,-1,-1):
        for i in range(j):
            if dict_in_list[i]['fee'] < dict_in_list[i+1]['fee']:
                dict_in_list[i],dict_in_list[i+1] = dict_in_list[i+1],dict_in_list[i]
    return dict_in_list



def excel(books,start):
    title = [('time','时间'),('book_id','书ID'),('name','书名'),('author','作者'),
            ('cp','版权'),('category_0','类别'),('category_1','子类'),('state','状态'),
            ('charge_type','计费类型'),('fee','收益'),('pay_down','付费下载数'),
            ('pay_user','付费下载用户数'),('free_down','免费下载数'),
            ('free_user','免费下载用户数'),('pv','简介访问数'),('uv','简介访问人数')] 
    books = bubblesortdesc(books)
    data = books
    start = start.strftime('%Y-%m-%d')
    filename = "%s.xls" % (start)
    f = open(filename,'wb')
    f.write(Excel().generate(title,data))
    f.close()
    return filename

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
def send_mail(to_list, sub, filename): 
    '''''
    to_list: 发送给谁
    sub: 主题
    context: 内容
    send_mail("xxx@126.com","sub","context")
    ''' 
    me = mail_user + "<"+mail_user+"@"+mail_postfix+">" 
    #msg = MIMEText(context) 
    #msg = MIMEText(html,'html','utf-8') 
    #msg['Subject'] = sub 
    #msg['From'] = me 
    #msg['To'] = ";".join(to_list)
    context = ""
    msg = MIMEText(context,'utf-8')
    msgRoot = MIMEMultipart('related') 
    msgRoot['Subject'] = unicode(sub,'utf-8')
    msgRoot['From'] = me
    msgRoot['To'] = ";".join(to_list)
    att = MIMEText(open(filename, 'rb').read(), 'base64', 'utf-8')
    att["Content-Type"] = 'application/octet-stream'  
    att["Content-Disposition"] = 'attachment; filename="%s"' % filename
    msgRoot.attach(att) 
    
    try: 
        send_smtp = smtplib.SMTP() 
        send_smtp.connect(mail_host) 
        send_smtp.login(mail_user, mail_pass) 
        send_smtp.sendmail(me, to_list, msgRoot.as_string()) 
        send_smtp.close() 
        return True 
    except Exception, e: 
        print(str(e)) 
        return False 
        
def get_date(self, delta=0):
    tody = time_start(datetime.datetime.now()-datetime.timedelta(days=delta),'day')
    return tody
'''
def make_book_data(books):
    book_data = ""
    for i in books:
        book_data += "<tr>"
        book_data += "<td>" + str(i['book_id']) + "</td>" + '\n'
        book_data += "<td>" + i['name'] + "</td>" + '\n'
        book_data += "<td>" + i['author'] + "</td>" + '\n'
        book_data += "<td>" + i['cp'] + "</td>" + '\n'
        book_data += "<td>" + i['category_0'] + "</td>" + '\n'
        book_data += "<td>" + i['category_1'] + "</td>" + '\n'
        book_data += "<td>" + i['charge_type'] + "</td>" + '\n'
        book_data += "<td>" + i['state'].decode('utf-8') + "</td>" + '\n'
        book_data += "<td>" + str(i['fee']) + "</td>" + '\n'
        book_data += "<td>" + str(i['pay_down']) + "</td>" + '\n'
        book_data += "<td>" + str(i['pay_user']) + "</td>" + '\n'
        book_data += "<td>" + str(i['free_down']) + "</td>" + '\n'
        book_data += "<td>" + str(i['free_user']) + "</td>" + '\n'
        book_data += "<td>" + str(i['pv']) + "</td>" + '\n'
        book_data += "<td>" + str(i['uv']) + "</td>" + '\n'

        book_data += "</tr>"
    book_data = book_data.encode("UTF-8")
    return book_data

def make_html(books):
    file = open('tpl.html','r')   
    html = file.read()
    book_data = make_book_data(books)
    html = html % (book_data)
    return html 

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
'''
if __name__ == '__main__': 
   
    #date
    yest = get_date(1) - datetime.timedelta(days=1)
    books = get_book_data(CP,yest)
    filename = excel(books,yest)
    #html = make_html(books)
    subject = CP + '_' + yest.strftime('%Y-%m-%d')
    if (True == send_mail(mailto_list,subject,filename)): 
        print ("发送成功")
    else: 
        print ("发送失败")
 
    





