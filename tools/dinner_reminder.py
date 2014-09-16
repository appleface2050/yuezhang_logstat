# -*- coding: utf-8 -*-  

from send_email import SendEmail

mailto_list = ['shangzongkai@zhangyue.com']
subject = 'Time to book dinner !!!!!!!'
msg = 'Time to book dinner !!!!!!!'
s = SendEmail(mailto_list, subject, msg)
if s.send_mail():
    print ("发送成功")
else:
    print ("发送失败")


