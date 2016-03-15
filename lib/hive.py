# -*- coding: utf-8 -*-
#!/usr/bin/env python

import sys
import logging
sys.path.append('/usr/local/hive-0.7.1-cdh3u3/lib/py')

#from hive_service import ThriftHive
#from hive_service.ttypes import HiveServerException
#为了客户端运行注释掉thrift模块

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

class HiveClient(object):
    def __init__(self, host, port):
        self.host = host
        self.port = int(port) 
        self.reconnect()

    def __getattr__(self, attr):
        if attr in ('execute','fetchAll','fetchOne'):
            return getattr(self.client,attr)
        else:
            raise AttributeError("'HiveClient' object has no attribute '%s'"%(attr,))

    def execute(self, sql):
        try:
            print sql
            return self.client.execute(sql)
        except Exception,e:
            logging.error('error when (%s):%s'%(sql,str(e)),exc_info=True)
            self.reconnect()

    def reconnect(self):
        if hasattr(self,"transport") and self.transport:
            self.transport.close()
            self.transport = None
        # Make socket
        self.transport = TSocket.TSocket(self.host, self.port)

        # Buffering is critical. Raw sockets are very slow
        self.transport = TTransport.TBufferedTransport(self.transport)

        # Wrap in a protocol
        protocol = TBinaryProtocol.TBinaryProtocol(self.transport)

        # Create a client to use the protocol encoder
        self.client = ThriftHive.Client(protocol)
        self.transport.open()

    def __del__(self):
        self.transport.close()
        self.transport = None


if __name__ == '__main__':
    hc = HiveClient('192.168.0.150','10000')
    hc.execute('select count(1) from service_v6 where ds=20121103 limit 10')
    for i in hc.fetchAll():
        print i

