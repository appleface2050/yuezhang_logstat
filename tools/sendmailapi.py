# -*- coding: utf-8 -*-

import urllib
import tornado.ioloop
import tornado.web
from send_email import SendEmail

class SendMailHandler(tornado.web.RequestHandler):
    def get(self):
        send_to = self.get_argument("to", "")
        subject = self.get_argument("subject", "")
        context = self.get_argument("message", "")
        html = self.get_argument("html", "")
        if send_to.find(','):
            send_to_list = send_to.split(',')
        else:
            send_to_list = [send_to]
        subject = urllib.unquote(subject)
        html = urllib.unquote(html)
        
        if not html:
            s = SendEmail(send_to_list, subject, context)
            if s.send_mail():
                self.write("Succeed")
            else:
                self.write("Failed")
        else:
            if '\n' in html:
                html = html.replace('\n','<br>')
            s = SendEmail(send_to_list, subject, html)
            if s.send_mail_html():
                self.write("Succeed")
            else:
                self.write("Failed")

    def post(self):
        send_to = self.get_argument("to", "")
        subject = self.get_argument("subject", "")
        context = self.get_argument("message", "")
        html = self.get_argument("html", "")
        if send_to.find(','):
            send_to_list = send_to.split(',')
        else:
            send_to_list = [send_to]
        subject = urllib.unquote(subject)
        html = urllib.unquote(html)
        
        if not html:
            s = SendEmail(send_to_list, subject, context)
            if s.send_mail():
                self.write("Succeed")
            else:
                self.write("Failed")
        else:
            if '\n' in html:
                html = html.replace('\n','<br>')
            s = SendEmail(send_to_list, subject, html)
            if s.send_mail_html():
                self.write("Succeed")
            else:
                self.write("Failed")

   

    def get_argument(self, name, default=tornado.web.RequestHandler._ARG_DEFAULT, strip=True): 
        '''
        overide it to encode all the param in utf-8
        '''
        value = super(SendMailHandler,self).get_argument(name,default,strip)
        if isinstance(value,unicode):
            value = value.encode('utf-8')
        return value

application = tornado.web.Application([
            (r"/", SendMailHandler),])

if __name__ == "__main__":
    application.listen(8888)
    tornado.ioloop.IOLoop.instance().start()






