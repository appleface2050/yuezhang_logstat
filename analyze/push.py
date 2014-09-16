#!/usr/bin/env python

import os
import time
import sys
import json
import logging
import urllib2
import getopt
import datetime

import MySQLdb

sys.path.append(os.path.dirname(os.path.split(os.path.realpath(__file__))[0]))

from lib.database import Model
from conf.settings import HiveConf
from conf.settings import DB_CNF
from conf.settings import DB_TEST
from analyze import HiveQuery


class Mysql(object):
    """
    MySQLdb Wrapper
    """
    min_cycle,max_cycle,total_time,counter,succ,fail,ext = 0xffffL,0L,0L,0L,0L,0L,''
    def __init__(self, dba, ismaster=False):
        self.dba = dba
        self.ismaster = ismaster
        self.conn = None
        self.cursor = None
        self.curdb = ""
        self.connect(dba)
        self.reset_stat()

    def __del__(self):
        self.close()

    def __repr__(self):
        return "Mysql(%s)"%(str(self.dba),)

    def __str__(self):
        return "Mysql(%s)"%(str(self.dba),)

    def close(self):
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.conn:
            self.conn.close()
            self.conn = None

    def connect(self, dba):
        """
        connect to mysql server
        """
        self.dba = dba
        self.conn = MySQLdb.connect(host=str(dba['host']),user=str(dba['user']),passwd=str(dba['passwd']),db=str(dba['db']),unix_socket=str(dba['sock']),port=dba['port'])
        self.cursor = self.conn.cursor(cursorclass = MySQLdb.cursors.DictCursor)
        self.execute("set names 'utf8'")
        self.execute("set autocommit=1")
        self.curdb = dba['db']
        return True

    def auto_reconnect(self):
        """
        ping mysql server
        """
        try:
            self.conn.ping()
        except Exception,e:
            try:
                self.cursor.close()
                self.conn.close()
            except:
                pass
            self.connect(self.dba)
        return True

    def execute(self, sql, values=()):
        """
        execute sql
        """
        if not self.ismaster and self.isupdate(sql):
            raise Exception("cannot execute[%s] on slave"%(sql,))
        start = time.time()
        self.auto_reconnect()
        if sql.upper().startswith("SELECT"):
            result = self.cursor.execute(sql,values)
        else:
            result = self.cursor.execute(sql)
        self.update_stat((time.time()-start)*1000,sql,values)
        return True

    def update_stat(self, t, sql, values):
        self.__class__.min_cycle = min(self.min_cycle,t)
        if t > self.__class__.max_cycle:
            self.__class__.max_cycle = t
            self.__class__.ext = '%s%%%s'%(sql,str(values))
        self.__class__.total_time += t
        self.__class__.counter += 1
        self.__class__.succ += 1

    @classmethod
    def reset_stat(cls):
        cls.min_cycle,cls.max_cycle,cls.total_time,cls.counter,cls.succ,cls.fail,cls.ext = 0xffffL,0L,0L,0L,0L,0L,''

    @classmethod
    def stat(cls):
        return cls.min_cycle,cls.max_cycle,cls.total_time,cls.counter,cls.succ,cls.fail,cls.ext

    def isupdate(self, sql):
        """
        """
        opers = ("INSERT","DELETE","UPDATE","CREATE","RENAME","DROP","ALTER","REPLACE","TRUNCATE")
        return sql.strip().upper().startswith(opers)

    def use_dbase(self, db):
        if self.curdb != db:
            self.execute("use %s"%(db,))
            self.curdb = db
        return True

    @classmethod
    def create_sql(cls, sql, params, noescape=""):
        """
        create sql acorrding to sql and params
        """
        result = sql;
        for each in params:
            val = params[each]
            if noescape=="" or  noescape.find(each)<0:
                val = MySQLdb.escape_string(str(val))
            result = result.replace(each,val)
        return result;

    @classmethod
    def merge_sql(cls, where, cond):
        """
        merge subsqls
        """
        return "%s AND %s"%(where,cond) if where and cond else (where or cond)

    def rows( self ):
        return self.cursor.fetchall()

    def commit(self):
        return self.conn.commit()

    @property
    def lastrowid(self):
        return self.cursor.lastrowid

    def affected_rows(self):
        return self.conn.affected_rows()

    def connection( self ):
        return self.conn

class MysqlPool(object):
    """
    MySQLPool
    """
    def __init__(self, dbcnf):
        self.mcnf,self.scnf = {},{}
        for i in dbcnf['m']:
            for j in dbcnf['m'][i]:
                self.mcnf[j] = json.loads(i)
        for i in dbcnf['s']:
            for j in dbcnf['s'][i]:
                self.scnf[j] = json.loads(i)
        self.mpool,self.spool = {},{}

    def get_server(self, db, dbgroup, ismaster=False):
        cnf = (self.mcnf if ismaster else self.scnf)[dbgroup]
        cnf['db'] = db
        dbastr = '%s:%s:%s'%(cnf['host'],cnf['port'],cnf['db'])
        pool = self.mpool if ismaster else self.spool
        if dbastr not in pool:
            print "--->get_server:",db,dbgroup,dbastr,len(pool)
            pool[dbastr] = Mysql(cnf,ismaster=ismaster)
        return pool[dbastr]

    def disconnect_all(self):
        for i in self.mpool:
            self.mpool[i].close()
        for i in self.spool:
            self.spool[i].close()

class MySQLData(object):
    """
    MySQLData
    """
    _db = "logstatV2"
    _table = ""
    _fields = set()
    _extfds = set()
    _pk = ""
    _scheme = ()
    _engine = "InnoDB"
    _charset = "utf8"

    def __init__(self, obj={}, db=None, ismaster=False, **kargs):
        self.ismaster = ismaster
        self.db = db or self.dbserver(**kargs)
        #super(MySQLData,self).__init__(obj)
        self._changed = set()
        self.extra_sql = None

    def dbserver(self, **kargs):
        db,group = self.get_dbase(**kargs),self.get_db_group(**kargs)
        return self.dpool.get_server(db,group,self.ismaster)

    @property
    def dpool(self):
        if not hasattr(MySQLData,'_dpool'):
            #MySQLData._dpool = MysqlPool(DB_CNF)
            MySQLData._dpool = MysqlPool(DB_TEST)
        return MySQLData._dpool

    def __getattr__(self, name):
        if name in self._fields or name in self._extfds:
            return self.__getitem__(name)

    def __setattr__(self, name, value):
        flag = name in self._fields or name in self._extfds
        if name != '_changed' and flag and hasattr(self, '_changed'):
            if name in self._fields:
                self._changed.add(name)
            super(MySQLData,self).__setitem__(name,value)
        else:
            self.__dict__[name] = value

    def __setitem__(self, name, value):
        if name != '_changed' and name in self._fields and hasattr(self, '_changed'):
            self._changed.add(name)
        super(MySQLData,self).__setitem__(name,value)

    def __delattr__(self, name):
        self.__delitem__(name)

    def get_dbase(self, **kargs):
        """
        get db name
        """
        return self._db

    def get_db_group(self, **kargs):
        """
        get db group
        """
        return self._db

    def set_extra(self, sub):
        '''
        extra query condition for all
        '''
        self.extra_sql = sub

    def execute(self, sql):
        '''
        proxy for self.client.execute
        '''
        if self.extra_sql:
            idx = sql.upper().find('WHERE')
            if idx >= 0:
                sql = '%s %s AND %s' % (sql[:idx+5],self.extra_sql,sql[idx+5:])
            else:
                sql = '%s WHERE %s' % (sql,self.extra_sql)
        return self.client.execute(sql)


class Container(object):
    push = set()
    click = set()
    consume = set()
    download = set()
    amonth = set()

class MySQLQuery(MySQLData):

    def __init__(self):
        super(MySQLQuery,self).__init__()

    def get_user_obtain(self, push, start, end, **kargs):
        #start = datetime.datetime.strftime(start, '%Y%m%d')
        sql = "select sum(pv) pv from push_obtain where ds='%s'" % start #group by
        self.db.execute(sql)
        lines = self.db.rows()
        count = 0 
        if lines:
            for line in lines:
                count += int(line['pv'])
        return count
   
    def get_user_click(self, click, start, end, **kargs):
        #start = datetime.datetime.strftime(start, '%Y%m%d')
        sql = "select distinct(user_name) usr from push_click where ds='%s'" % start #group by
        self.db.execute(sql)
        lines = self.db.rows()
        if lines:
            for line in lines:
                user = str(line['usr']).strip()
                click.add(user)
        return len(click)

class ServiceQuery(HiveQuery):
    '''
    query from service_v6
    '''
    def __init__(self, host, port):
        '''
        init hive client
        '''
        super(ServiceQuery,self).__init__(host,port)

    def get_download_users(self, cache, start, end):
        sql = "select distinct user_name from download_v6 where ds>='%s' and ds<'%s'" % (start, end)
        self.execute(sql)
        for i in self.client.fetchAll():
            a = i.split('\t')
            usr = a[0].strip()
            cache.download.add(usr)
        cache.download = cache.click.intersection(cache.download)
        return len(cache.download)
   
    def get_consume_users(self, cache, start, end):
        sql = "select distinct user_name from download_v6 where ((fee_unit='10' AND price_code in (24,30,22,26,36,27,20,38,32,21,11,13,12,14,17,16,18,31,23,28,29,35,34,33,2,7,6)) OR (fee_unit='20' AND price>0)) and ds>='%s' and ds<'%s'" % (start, end)
        self.execute(sql)
        for i in self.client.fetchAll():
            a = i.split('\t')
            usr = a[0].strip()
            cache.consume.add(usr)
        cache.consume = cache.click.intersection(cache.consume)
        return len(cache.consume)

    def get_a_month_users(self, start, end):
        now = datetime.datetime.now()
        amon = now - datetime.timedelta(days=30)
        amon = amon.strftime('%Y-%m-%d')
        sql = "select distinct user_name from userbasic_v6_new where last_init_time<'%s'" % amon

        self.execute(sql)
        for i in self.client.fetchAll():
            a = i.split('\t')
            usr = a[0].strip()
            Container.amonth.add(usr)
        Container.amonth = Container.push.intersection(Container.amonth)
        return len(Container.amonth)
            
class PushStat(Model):

    _db = 'logstatV2'
    _table = 'push_stat'
    _pk = 'id'
    _fields = set(['id','ds','user_push','user_click', 'user_consume', 'user_download'])

    def get_all_push_stat(self):
        res = {}
        qtype = "SELECT partner_id"
        ext = "partner_id LIKE '108%' OR partner_id LIKE '109%' OR partner_id LIKE '110%' OR partner_id LIKE '111%'"
        q = self.Q(qtype=qtype).extra(ext)
        res = q
        return res

class Work(object):

    def __init__(self, conf=None):
        if not conf:
            conf = HiveConf
        self.mysql = MySQLQuery()
        self.hive = ServiceQuery(conf['host'], conf['port'])
        self.cache = Container()

    def import_push(self, start, end):
        push = PushStat.new() 
        #push.ds = datetime.datetime.strftime(start, '%Y-%m-%d')
        push.ds = start
        push.user_push = self.mysql.get_user_obtain(self.cache.push, start, end)
        push.user_click = self.mysql.get_user_click(self.cache.click, start, end)
        push.user_consume = self.hive.get_consume_users(self.cache, start, end)
        push.user_download = self.hive.get_download_users(self.cache, start, end)
        push.save()

if __name__ == "__main__":
    #now = datetime.datetime.now()
    #yesterday = now - datetime.timedelta(days=1)
    try:
        opts,args = getopt.getopt(sys.argv[1:],'',['start=','end='])
    except getopt.GetoptError,e:
        sys.exit(1)
    start,end = None,None
    for o, a in opts:
        if o == '--start':
            #start = datetime.datetime.strptime(a,'%Y%m%d')
            start = a
        if o == '--end':
            #end = datetime.datetime.strptime(a,'%Y%m%d')
            end = a
    work = Work()
    work.import_push(start, end)
