#!/usr/bin/env python
# -*- coding : utf-8 -*-

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

from conf.settings import DB_CNF
from conf.settings import DB_TEST
from conf.settings import PAGE_TYPE,TOP_TYPE
from conf.settings import API_TRY_TIME_LIMIT,ERROR_EMAIL_MAILTO_LIST,ERROR_EMAIL_SUBJECT,ERROR_EMAIL_HTML
from tools.send_email import SendEmail
from model.stat import SyncThrdPartnerbyplan

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
        #print sql,values
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
    _db = "logstat"
    _table = ""
    _fields = set()
    _extfds = set()
    _pk = ""
    _scheme = ()
    _engine = "InnoDB"
    _charset = "utf8"
    _try_times = 0
    sendemail = SendEmail(ERROR_EMAIL_MAILTO_LIST,ERROR_EMAIL_SUBJECT,ERROR_EMAIL_HTML)

    def __init__(self, obj={}, db=None, ismaster=False, **kargs):
        self.ismaster = ismaster 
        self.db = db or self.dbserver(**kargs)
        #super(MySQLData,self).__init__(obj)
        self._changed = set()
        self.extra_sql = None
        self.sendemail = SendEmail(ERROR_EMAIL_MAILTO_LIST,ERROR_EMAIL_SUBJECT,ERROR_EMAIL_HTML)

    def get_try_times(self):
        return self._try_times

    def zero_try_times(self):
        self._try_times = 0
        return True

    def acc_try_times(self):
        self._try_times += 1
        return True

    def get_send_email(self):
        return self.sendemail

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

    def or_sql(self, field, valist):
        return '(%s)' % ' OR '.join(["%s like '%s%%'"%(field,i) for i in valist])

    def in_sql(self, field, valist):
        vals = ','.join(["%d"%i for i in valist])
        return '%s in (%s)'%(field,vals)
    
    def merge_sql(self, where, cond):
        """
        merge subsqls
        """
        return "%s AND %s"%(where,cond) if where and cond else (where or cond)
    
    def group(self, **kargs):
        run_id = kargs.get('run_id',None)
        plan_id = kargs.get('plan_id',None)
        partner_id = kargs.get('partner_id',None)
        version_name = kargs.get('version_name',None)
        product_name = kargs.get('product_name',None)
        group = []
        if run_id:
            #group.append('run_id')
            group.append('runid')
        if plan_id or partner_id:
            #group.append('partner_id')
            group.append('channelid')
        if version_name:
            #group.append("regexp_extract(version_name,'(ireader_[0-9].[0-9])')")
            #group.append("version regexp '([0-9]\\.[0-9])'")
            group.append("version")
        if product_name:
            #group.append('product_name')
            group.append('model')
        return ','.join(group)

    def group_keys(self, **kargs):
        run_id = kargs.get('run_id',None)
        plan_id = kargs.get('plan_id',None)
        partner_id = kargs.get('partner_id','')
        version_name = kargs.get('version_name',None)
        product_name = kargs.get('product_name',None)
        if plan_id:
            #partnerid_list = self.get_partnerid_list(plan_id)
            partnerid_list = self.get_partnerid_listV2(plan_id)
        else:
            partnerid_list = [partner_id]
        keys = []
        products = [product_name] if product_name!='unknown' else ['','null']
        for i in partnerid_list:
            for p in products:
                key = []
                if run_id:
                    key.append(str(run_id))
                if i:
                    key.append(str(i))
                if version_name:
                    key.append(version_name)
                if p or product_name=='unknown':
                    key.append(p)
                keys.append(','.join(key))
        return keys
 
    def grp(self, group):
        return ',%s'%group if group else ''

    def uid(self):
        return 'CONCAT(userid,user_name,reg_type)'

    def uid2(self):
        return 'CONCAT(userid,username,reg_type)'
   
    def wap_uid(self):
        return 'CONCAT(ucid,loginname,logintype)'

    def uc_userId(self):
        return 'CONCAT(bk,uc_userId)'

    def ds_sql(self, start, end):
        s,e = start.strftime('%Y-%m-%d'),end.strftime('%Y-%m-%d')
        #return "sumdate>='%s' AND sumdate<'%s'"%(s,e)
        return "sumdate='%s'"%(s)
    
    def date_unix_stamp_sql(self, start, end, col='DATE'):
        if col == 'DATE':
            return "DATE>=unix_timestamp('%s') AND DATE<unix_timestamp('%s')" % (start,end)
        else:
            return "%s>=unix_timestamp('%s') AND %s<unix_timestamp('%s')" % (col,start,col,end)

    @classmethod
    def get_partnerid_list(cls, plan_id):
        '''
        get partner id list by plan id, to do
        http://192.168.0.7:10080/scheme?plan_id={plan_id}
        '''
        MySQLData._try_times = 0
        while(True):
            try:
                jsonstr = urllib2.urlopen('http://192.168.0.7:10080/scheme?plan_id=%s'%plan_id).read()
                break
            except Exception, e:
                print e
                MySQLData._try_times += 1
                time.sleep(30)
                if MySQLData._try_times >= API_TRY_TIME_LIMIT:
                    MySQLData.sendemail.send_mail()
                    return False
        return json.loads(jsonstr)['partner_id']

    def get_partnerid_listV2(self, plan_id):
        reslist = []
        entries = SyncThrdPartnerbyplan().mgr().get_partner_list_by_plan_id(plan_id)
        for entry in entries:
            partner_id = entry["partner_id"]
            reslist.append(str(partner_id))
        return reslist

    @classmethod
    def get_plan_list(cls):
        '''
        http://192.168.0.7:10080/scheme?platform_id=6   
        '''
        return [35, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50]

class ServiceQuery(MySQLData):

    def __init__(self):
        super(ServiceQuery,self).__init__(ismaster=True)
        self.cache = {}
        self.cache_pagetype_dict = {}

    def reset_cache(self):
        self.cache = {}
        self.cache_pagetype_dict.clear()

    def load_pagetype(self, start, end, **kargs):
        group = self.group(**kargs)
        sql = "SELECT ifnull(sum(PV), 0) pv, COUNT(DISTINCT(%s)) uv, pagetype %s FROM dm_v6_visit GROUP BY pagetype %s"%(self.uid2(),self.grp(group),self.grp(group))
        print sql
        self.db.execute(sql)
        for line in self.db.rows():
            by = group.split(',')
            key = ','.join([str(line[by[i]]) for i in xrange(len(by)) if by[i] != ''])
            pv, uv, pt = int(line['pv']), int(line['uv']), int(line['pagetype'])
            ckey = str(pt) + "_" + key + group 
            ckey = ckey.strip()
            if ckey in self.cache_pagetype_dict:
                self.cache_pagetype_dict[ckey][1] += uv
                self.cache_pagetype_dict[ckey][2] += pv
            else:
                self.cache_pagetype_dict[ckey] = [pt, uv, pv]

    def get_partner_product(self, start, end):
        #sub = self.ds_sql(start, end)
        #sql = "select partner_id,product_name from dm_v6_visit where %s group by partner_id,product_name"%sub
        sql = "select channelid, model from dm_v6_visit group by channelid, model"
        self.db.execute(sql)
        res = {}
        for line in self.db.rows():
            key = str(line['channelid']).strip()
            val = str(line['model']).strip()
            if key.isdigit() and val:
                res.setdefault(key,['unknown']).append(val)
        return res

    def get_vis_user_count(self, start, end, **kargs):
        '''
        get count of access user
        start: start time
        end: end time
        kargs:run_id,partner_id,version_name,product_name
        '''
        group = self.group(**kargs)
        groupby = ('GROUP BY %s' % group) if group else ''
        sql = "SELECT COUNT(DISTINCT(%s)) ud %s FROM dm_v6_visit WHERE pagetype in (1,2,6,7,8,10,11,12,13,14,15) %s"%(self.uid2(),self.grp(group),groupby)
        ckey = 'vis_ucnt_%s_%s_%s'%(start,end,group)
        res = self.cache.get(ckey,None)
        if res is None:
            res = {}
            print sql
            self.db.execute(sql)
            for line in self.db.rows():
                by = group.split(',')
                key = ','.join([str(line[by[i]]) for i in xrange(len(by)) if by[i] != ''])
                val = int(line['ud'])
                res[key] = val
            self.cache[ckey] = res
        keys,count = self.group_keys(**kargs),0
        for i in keys:
            count += res.get(i,0)
        return count

    def get_active_user_count(self, start, end, **kargs):
        '''
        get count of active user
        start: start time
        end: end time
        kargs:run_id,partner_id,version_name,product_name
        '''
        sub = self.ds_sql(start,end)
        group = self.group(**kargs)
        groupby = ('GROUP BY %s' % group) if group else ''
        #sql = "SELECT COUNT(DISTINCT(%s)) uv%s FROM dm_v6_briefvisit WHERE %s %s"%(self.uid2(),self.grp(group),sub,groupby)
        sql = "SELECT COUNT(DISTINCT(%s)) uv%s FROM dm_v6_briefvisit %s"%(self.uid2(),self.grp(group),groupby)
        ckey = 'active_ucnt_%s_%s_%s'%(start,end,group)
        res = self.cache.get(ckey,None)
        if res is None:
            res = {}
            print sql
            self.db.execute(sql)
            for line in self.db.rows():
                by = group.split(',')
                key = ','.join([str(line[by[i]]) for i in xrange(len(by)) if by[i] != ''])
                val = int(line['uv'])
                res[key] = val
            self.cache[ckey] = res
        keys,count = self.group_keys(**kargs),0
        for i in keys:
            count += res.get(i,0)
        return count

    def get_pv(self, start, end, **kargs):
        '''
        get PV
        start: start time
        end: end time
        kargs:run_id,partner_id,version_name,product_name
        '''

        #keylist = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15] # etc. need to change
        #sub = self.in_sql('pagetype',keylist)
        #sub = self.merge_sql(sub,self.ds_sql(start,end))
        #sub = self.ds_sql(start, end)
        group = self.group(**kargs)
        groupby = ('GROUP BY %s' % group) if group else ''
        #sql = "SELECT ifnull(sum(PV),0) pv %s FROM dm_v6_visit WHERE %s %s"%(self.grp(group), sub, groupby)
        sql = "SELECT ifnull(sum(PV),0) pv %s FROM dm_v6_visit %s"%(self.grp(group), groupby)

        ckey = 'pv_%s_%s_%s'%(start,end,group)
        res = self.cache.get(ckey,None)
        if res is None:
            res = {}
            print sql
            self.db.execute(sql)
            for line in self.db.rows():
                by = group.split(',')
                key = ','.join([str(line[by[i]]) for i in xrange(len(by)) if by[i] != ''])
                val = int(line['pv'])
                res[key] = val
            self.cache[ckey] = res

        keys,count = self.group_keys(**kargs),0
        for i in keys:
            count += res.get(i,0)
        return count

    def get_topN(self, start, end, num=100, **kargs):
        '''
        top N for some kinds of page types
        start: start time
        end: end time
        kargs:run_id,partner_id,version_name,product_name
        '''
        #sub = self.ds_sql(start,end)
        sub = ""
        group = self.group(**kargs)
        groupby = ('GROUP BY %s' % group) if group else ''
        for t in TOP_TYPE:
            sql = ''
            ckey = ''
            if t in ['board', 'tag', 'subject']:
                where = self.merge_sql(sub,"pagetype=%s"%TOP_TYPE[t][0])
                qtype = "SELECT %s type,ifnull(sum(PV),0) AS cnt,COUNT(DISTINCT(%s)) uid %s"%(TOP_TYPE[t][1],self.uid2(),self.grp(group))
                sql = "%s FROM dm_v6_visit WHERE %s GROUP BY %s%s"%(qtype,  where, TOP_TYPE[t][1], self.grp(group))
                ckey = 'topn_each_visit_%s_%s_%s_%s'%(t,start,end,group)
            else:
                where = self.merge_sql(sub,"search_type=%s"%TOP_TYPE[t][0])
                qtype = "SELECT %s type,ifnull(sum(search_times),0) AS cnt,COUNT(DISTINCT(%s)) uid %s"%(TOP_TYPE[t][1],self.uid2(),self.grp(group))
                sql = "%s FROM dm_v6_search WHERE %s GROUP BY %s%s"%(qtype,  where, TOP_TYPE[t][1], self.grp(group))
                ckey = 'topn_each_search_%s_%s_%s_%s'%(t,start,end,group)

            res = self.cache.get(ckey,None)
            if res is None:
                res = {}
                print sql
                self.db.execute(sql)
                for line in self.db.rows():
                    by = group.split(',')
                    key = ','.join([str(line[by[i]]) for i in xrange(len(by)) if by[i] != ''])
                    val,pv,uv = str(line['type']).strip(),int(line['cnt']),int(line['uid'])
                    res.setdefault(key,[]).append((val,pv,uv))
                    self.cache[ckey] = res 

            keys,rdict = self.group_keys(**kargs),{}
            for i in keys:
                for r in res.get(i,[]):
                    val,pv,uv = r[0].strip(),r[1],r[2]
                    if t in ('board','subject') and not val.isdigit():
                        continue
                    if val in rdict:
                        rdict[val]['pv'] += pv
                        rdict[val]['uv'] += uv
                    else:
                        rdict[val] = {'val':val,'pv':pv,'uv':uv}
            rlist = rdict.values()
            rlist.sort(cmp=lambda x,y:cmp(x['pv'],y['pv']),reverse=True)
            yield {'type':t,'list':rlist[:num] if num else rlist}

    def sql_for_pagetype(self, pt):
        '''
        pt: page type, which is in PAGE_TYPE
        '''
        sub = ''
        if pt in PAGE_TYPE:
            #keys = ','.join(["'%s'"%i for i in PAGE_TYPE[pt][0]])
            #sub = "pagetype in (%s)"%(keys,)
            sub = "pagetype=%d"%(PAGE_TYPE[pt][0][0],)
        return sub

    """
    def get_pagetype_count(self, start, end, **kargs):
        '''
        stat for some kinds of page types
        start: start time
        end: end time
        kargs:run_id,partner_id,version_name,product_name
        '''
        flag = 1
        group = self.group(**kargs)
        group = group.strip()
        for key in self.cache_pagetype_dict:
            if key.endswith(group):
                flag = 0
                break
        if flag == 1:
            self.load_pagetype(start, end, **kargs)
        rdict = []
        #ckey = str(PAGE_TYPE[t][0][0]) + "_" + group
        ckey = group
        ckey = ckey.strip()
        res = self.cache_pagetype_dict[ckey] if ckey in self.cache_pagetype_dict else [0, 0, 0]
        pv, uv = res[2], res[1]
        #yield {'type':t,'pv':pv,'uv':uv}
        #rdict.append({'type':t,'pv':pv,'uv':uv})
        keys,pv,uv= self.group_keys(**kargs),0,0
        for i in keys:
            if self.cache_pagetype_dict.find(i) != -1:
                t,_pv,_uv = res.get(i,(0,0,0))
                pv += _pv
                uv += _uv
            yield {'type':t,'pv':pv,'uv':uv}

    """
    def get_pagetype_count(self, start, end, **kargs):
        '''
        stat for some kinds of page types
        start: start time
        end: end time
        kargs:run_id,partner_id,version_name,product_name
        '''
        #sub = self.ds_sql(start,end)
        sub = ""
        group = self.group(**kargs)
        groupby = ('GROUP BY %s' % group) if group else ''
        for t in PAGE_TYPE:
            where = self.merge_sql(sub,self.sql_for_pagetype(t))
            sql = "SELECT ifnull(sum(PV), 0) pv, COUNT(DISTINCT(%s)) uv %s FROM dm_v6_visit WHERE %s %s"%(self.uid2(),self.grp(group),where,groupby)

            ckey = 'pagetype_%s_%s_%s_%s'%(t,start,end,group)
            res = self.cache.get(ckey,None)
            if res is None:
                res = {}
                print sql
                self.db.execute(sql)
                for line in self.db.rows():
                    by = group.split(',')
                    key = ','.join([str(line[by[i]]) for i in xrange(len(by)) if by[i] != ''])
                    _pv, _uv = int(line['pv']), int(line['uv'])
                    res[key] = (_pv, _uv)
                self.cache[ckey] = res

            keys,pv,uv= self.group_keys(**kargs),0,0
            for i in keys:
                _pv,_uv = res.get(i,(0,0))
                pv += _pv
                uv += _uv
            yield {'type':t,'pv':pv,'uv':uv}
        '''
        sql = "SELECT ifnull(sum(PV), 0) pv, COUNT(DISTINCT(%s)) uv, pagetype %s FROM dm_v6_visit GROUP BY pagetype %s"%(self.uid2(),self.grp(group),self.grp(group))
        ckey = 'pagetype_%s_%s_%s'%(start,end,group)
        res = self.cache.get(ckey,None)
        if res is None:
            res = {}
            print sql
            self.db.execute(sql)
            for line in self.db.rows():
                by = group.split(',')
                val = [str(line['pagetype'])]
                for i in xrange(len(by)):
                    if by[i] != '':
                        val.append(str(line[by[i]]))
                key = ','.join(val)
                pagetype, _pv, _uv = line['pagetype'], int(line['pv']), int(line['uv'])
                res[key] = (pagetype, _pv, _uv)
                self.cache[ckey] = res

        rdict, keys= [], self.group_keys(**kargs)
        for i in keys:
             t, pv,uv = res.get(i,(0,0,0))
             rdict.append({'type':t,'pv':pv,'uv':uv})
        return rdict
        '''

    def get_briefvisit(self, start, end, pkeys=[], **kargs):
        '''
        top N for books according to pv
        start: start time
        end: end time
        kargs:run_id,partner_id,version_name,product_name
        '''
        sub = self.ds_sql(start,end)
        if pkeys:
            #pkeys = ['search_main', 'search_button', 'hot_search_word']
            pkeys = ','.join(["'%s'"%i for i in pkeys])
            sub = self.merge_sql(sub,"origin in (%s)"%pkeys)
        group = self.group(**kargs)
        groupby = ('GROUP BY %s' % group) if group else ''
        qtype = "SELECT bookid,ifnull(SUM(view_times),0) pv,COUNT(DISTINCT(%s)) uv %s"%(self.uid2(), self.grp(group))
        sql = "%s FROM dm_v6_briefvisit WHERE %s GROUP BY bookid %s"%(qtype, sub, self.grp(group))
   
        ckey = 'topn_book_%s_%s_%s_%s'%(start,end,pkeys,group)
        res = self.cache.get(ckey,None)
        if res is None:
            res = {}
            print sql
            self.db.execute(sql)
            for line in self.db.rows():
                if line is None:
                    break
                try:
                    by = group.split(',')
                    key = ','.join([str(line[by[i]]) for i in xrange(len(by)) if by[i] != ''])            
                    bid,pv,uv = str(line['bookid']).strip(),int(line['pv']),int(line['uv'])
                except Exception,e:
                    logging.error('%s,%s\n',i,str(e),exc_info=True)
                    continue
                res.setdefault(key,[]).append((bid,pv,uv))
            self.cache[ckey] = res

        keys,rdict = self.group_keys(**kargs),{}
        for i in keys:
            for r in res.get(i,[]):
                bid,pv,uv = r[0],r[1],r[2]
                if bid in rdict:
                    rdict[bid]['pv'] += pv
                    rdict[bid]['uv'] += uv
                else:
                    rdict[bid] = {'bid':bid,'pv':pv,'uv':uv}
        return rdict


    def get_topN_product(self, start, end, num=9, **kargs):
        '''
        top N machines by user id
        start: start time
        end: end time
        num: number of product name
        kargs: run_id,partner_id,version_name,product_name
        '''
        #sub = self.ds_sql(start,end)
        sub = ""
        sub = self.merge_sql(sub,"model<>''")
        group = self.group(**kargs)
        groupby = ('GROUP BY %s' % group) if group else ''
        qtype = "SELECT ifnull(sum(PV), 0) pv, COUNT(DISTINCT(%s)) uv, model %s"%(self.uid2(),self.grp(group))
        sql = "%s FROM dm_v6_visit WHERE %s GROUP BY model %s " % (qtype,sub,self.grp(group))

        ckey = 'topu_product_%s_%s_%s' % (start,end,group)
        res = self.cache.get(ckey,None)
        if res is None:
            res = {}
            print sql
            self.db.execute(sql)
            for line in self.db.rows():
                by = group.split(',')
                key = ','.join([str(line[by[i]]) for i in xrange(len(by)) if by[i] != ''])
                pv,uv,name = int(line['pv']),int(line['uv']),str(line['model']).strip()
                res.setdefault(key,[]).append((name,pv,uv))
            self.cache[ckey] = res

        keys,rdict,pv_total,uv_total = self.group_keys(**kargs),{},0,0
        for i in keys:
            for r in res.get(i,[]):
                name,pv,uv = r[0],r[1],r[2]
                pv_total += pv
                uv_total += uv
                if name in rdict:
                    rdict[name]['pv'] += pv
                    rdict[name]['uv'] += uv
                else:
                    rdict[name] = {'name':name,'type':'user','pv':pv,'uv':uv}

        rlist = rdict.values()
        rlist.sort(cmp=lambda x,y:cmp(x['uv'],y['uv']),reverse=True)
        if num:
            rlist = rlist[:num]
        other = 0
        for i in rlist:
            i['ratio'] = i['uv']/uv_total if uv_total else 0
            other += i['uv']
        rlist.append({'name':'other','type':'user','uv':other,'ratio':other/uv_total if uv_total else 0})
        return uv_total,rlist

class DownloadQuery(MySQLData):

    FEE_CODE = {'1':0,'8':0,'3':0,'4':0,'5':0,'6':12,'7':1,'2':2,'9':0,'10':0,
                '11':2,'12':8,'13':2,'14':5,'15':0,'16':3,'17':2,'18':3,'19':0,'20':2,
                '21':2,'22':2,'23':2,'24':2,'25':0,'26':4,'27':5,'28':3,'29':2,'30':2,
                '31':6,'32':2,'33':2,'34':2,'35':2,'36':2,'37':0,'38':2}

    def __init__(self):
        super(DownloadQuery,self).__init__(ismaster=True)
        self.fcode_map = {}
        for k,v in self.FEE_CODE.items():
            self.fcode_map.setdefault(v,[]).append(k)
        self.cache = {}
        #self.download_dict = {}
        self.split_table()

    def reset_cache(self):
        self.cache = {}
        self.download_dict = {}

    def split_table(self):
        sql_fu20 = "drop table if exists dm_v6_download_fu20;create table dm_v6_download_fu20 as select * from dm_v6_download where fee_unit=20"
        sql_fu10 = "drop table if exists dm_v6_download_fu10;create table dm_v6_download_fu10 as select * from dm_v6_download where fee_unit=10"
        sql_dt8 = "drop table if exists dm_v6_download_dt8;create table dm_v6_download_dt8 as select * from dm_v6_download where fee_unit=20 and down_type=8"
        self.db.execute(sql_fu20);
        self.db.execute(sql_fu10);
        self.db.execute(sql_dt8);


    def load_download_dict(self, **kargs):
        '''
        download_dict = {bookid_charge_type_ispay:[bookid, chapter_down, user_down, fee, payorfree],...}
        example: {"1041307_chapter_pay":[1041307, 1000, 500, 3000, "pay"]}
        '''
        group = self.group(**kargs)
        for charge_type in ["chapter", "book"]:
            sql = ""
            if charge_type == "chapter":
                sql = "SELECT IFNULL(SUM(chaptercount),0) cd, COUNT(DISTINCT(CONCAT(userid,user_name,reg_type))) ud, SUM(downloadfee) fee, bookid, case when downloadfee>0 then 'pay' else 'free' end  freeorpay %s FROM dm_v6_download WHERE down_type=2 AND fee_unit=20 GROUP BY bookid, freeorpay %s" % (self.grp(group), self.grp(group))
            else:
                sql = "SELECT IFNULL(SUM(chaptercount),0) cd, COUNT(DISTINCT(CONCAT(userid,user_name,reg_type))) ud, SUM(downloadfee) fee, bookid, case when down_type in (1,2,4,5,6,7) and downloadfee>0 then 'pay' when down_type in (1,4,5,6,7) and download=0 then 'free' else 'other' end payorfree %s FROM dm_v6_download WHERE fee_unit=10 GROUP BY bookid, payorfree %s" % (self.grp(group), self.grp(group))

            self.db.execute(sql)
            for line in self.db.rows():
                by = group.split(',')
                key = ','.join([str(line[by[i]]) for i in xrange(len(by)) if by[i] != ''])
                cd, ud, fee, bid, payorfree = int(line['cd']), int(line['ud']), float(line['fee']), line['bookid'] 
                key = bid + "_chapter_" + payorfree + "_" + key  if charge_type == "chapter" else bid + "_book_" + payorfree + "_" + key
                self.download_dict[key] = [bid, cd, ud, fee, payorfree]  

    def pr_sql(self, ispay):
        '''
        '''
        prlist = [k for k in self.FEE_CODE if self.FEE_CODE[k]>0]
        sql = 'price_code in (%s)' % ','.join(prlist)
        if not ispay:
            sql = "not %s" % sql
        return sql

    def charge_type_sql(self, charge_type, ispay):
        '''
        '''
        assert charge_type in ('book','chapter')
        if charge_type == 'book':
            if ispay:
                #sql = "down_type in (1,2,4,5,6,7) AND fee_unit=10 AND %s" % self.pr_sql(ispay)
                #sql = "down_type in (1,2,4,5,6,7) AND fee_unit=10 AND downloadfee>0"
                sql = "down_type in (1,2,4,5,6,7) AND downloadfee>0"  # use for splited table
            else:
                #sql = "down_type in (1,4,5,6,7) AND fee_unit=10 AND %s" % self.pr_sql(ispay)
                #sql = "down_type in (1,4,5,6,7) AND fee_unit=10 AND downloadfee=0"
                sql = "down_type in (1,4,5,6,7) AND downloadfee=0" # use for splited table
        else:
            if ispay:
                #sql = "down_type=2 AND fee_unit=20 AND downloadfee>0"
                sql = "down_type=2 AND downloadfee>0" # use for splited table
            else:
                #sql = "down_type=2 AND fee_unit=20 AND downloadfee=0"
                sql = "down_type=2 AND downloadfee=0" # use for splited table
        return sql
 
    def get_split_table(self, charge_type):
        if charge_type == 'chapter':
            return "dm_v6_download_fu20"
        elif charge_type == 'book':
            return "dm_v6_download_fu10" 
        else:
            return "dm_v6_download_dt8"

    def get_pay_user_count(self, start, end, **kargs):
        '''
        paying user count
        start: start time
        end: end time
        kargs:run_id,partner_id,version_name,product_name
        '''
        #sub = "((fee_unit='10' AND %s) OR (fee_unit='20' AND downloadfee>0))" % self.pr_sql(True)
        sub = "downloadfee>0"
        #sub = self.merge_sql(sub,self.ds_sql(start,end))
        group = self.group(**kargs)
        groupby = ('GROUP BY %s' % group) if group else ''
        #sql = "SELECT COUNT(DISTINCT(%s)) ud %s FROM dm_v6_download WHERE %s %s" % (self.uid(), self.grp(group), sub, groupby)
        sql = "SELECT COUNT(DISTINCT(num)) ud %s FROM (SELECT (%s) as num %s FROM dm_v6_download WHERE %s)tmp %s" % (self.grp(group), self.uid(), self.grp(group), sub, groupby)
        ckey = 'pay_ucnt_%s_%s_%s'%(start,end,group)
        res = self.cache.get(ckey,None)
        if res is None:
            res = {}
            print sql
            self.db.execute(sql)
            for line in self.db.rows():
                by = group.split(',')
                key = ','.join([str(line[by[i]]) for i in xrange(len(by)) if by[i] != ''])
                val = int(line['ud'])
                res[key] = val
            self.cache[ckey] = res

        keys,count = self.group_keys(**kargs),0
        for i in keys:
            count += res.get(i,0)
        return count

    def get_downcount(self, start, end, charge_type, ispay=False, is_user_unique=False, **kargs):
        '''
        free/paying download (user) count by book/chapter
        start: start time
        end: end time
        charge_type: book or chapter
        ispay: ispaid or not
        is_user_unique: if user count
        kargs: run_id,partner_id,version_name,product_name
        '''
        sub = self.charge_type_sql(charge_type, ispay)
        table = self.get_split_table(charge_type)
        #sub = self.merge_sql(sub, self.ds_sql(start, end))
        group = self.group(**kargs)
        groupby = 'GROUP BY %s' % group if group else ''

        qtype = "SELECT ifnull(SUM(chaptercount),0) ud" if charge_type == 'chapter' else "SELECT COUNT(DISTINCT(CONCAT(%s, bookid))) ud" % self.uid()
        if is_user_unique:
            qtype = 'SELECT COUNT(DISTINCT(%s)) ud'%self.uid()
        #sql = "%s %s FROM dm_v6_download WHERE %s %s"%(qtype,self.grp(group),sub,groupby)
        sql = "%s %s FROM %s WHERE %s %s"%(qtype,self.grp(group), table,sub,groupby)
        
        ckey = 'dcnt_%s_%s_%s_%s_%s_%s'%(start,end,charge_type,ispay,is_user_unique,group)
        res = self.cache.get(ckey,None)
        if res is None:
            res = {}
            print sql
            self.db.execute(sql)
            for line in self.db.rows():
                by = group.split(',')
                key = ','.join([str(line[by[i]]) for i in xrange(len(by)) if by[i] != ''])
                val = int(line['ud'])
                res[key] = val
            self.cache[ckey] = res

        keys,count = self.group_keys(**kargs),0
        for i in keys:
            count += res.get(i,0)
        return count

    def get_fee_by_chapter(self, start, end, **kargs):
        '''
        fee sum by chapter
        start: start time
        end: end time
        kargs: run_id,partner_id,version_name,product_name
        '''
        #sub = self.ds_sql(start, end)
        sub = ""
        sub = self.merge_sql(sub, self.charge_type_sql('chapter', True))
        table = self.get_split_table('chapter')
        group = self.group(**kargs)
        #sql = "SELECT sum(chaptercount) cd,COUNT(DISTINCT(%s)) ud,SUM(downloadfee) fee %s FROM dm_v6_download WHERE %s GROUP BY bookid %s" % (self.uid(),self.grp(group),sub,self.grp(group))
        #sql = "SELECT SUM(downloadfee) fee %s FROM dm_v6_download WHERE %s GROUP BY bookid %s" % (self.grp(group),sub,self.grp(group))
        sql = "SELECT SUM(downloadfee) fee %s FROM %s WHERE %s GROUP BY bookid %s" % (self.grp(group),table,sub,self.grp(group))
        
        ckey = 'fee_ch_%s_%s_%s' % (start,end,group)
        res = self.cache.get(ckey,None)
        if res is None:
            res = {}
            print sql
            self.db.execute(sql)
            for line in self.db.rows():
                by = group.split(',')
                key = ','.join([str(line[by[i]]) for i in xrange(len(by)) if by[i] != ''])
                #down, user, fee = int(line['cd']),int(line['ud']),float(line['fee'])
                #val = fee*user/down if down else 0
                fee = float(line['fee'])
                if key in res:
                    #res[key] += val
                    res[key] += fee
                else:
                    #res[key] = val
                    res[key] = fee

            self.cache[ckey] = res
        keys,fee = self.group_keys(**kargs),0
        for i in keys:
            fee += res.get(i,0)
        return fee

    def get_fee_by_book(self, start, end, **kargs):
        '''
        fee sum by book
        start: start time
        end: end time
        kargs: run_id,partner_id,version_name,product_name
        '''
        #sub = self.ds_sql(start,end)
        sub = ""
        sub = self.merge_sql(sub,self.charge_type_sql('book',True))
        table = self.get_split_table('book')
        group = self.group(**kargs)
        #qtype = "SELECT COUNT(DISTINCT(%s)) ud, SUM(downloadfee) fee %s"%(self.uid(), self.grp(group))
        qtype = "SELECT SUM(downloadfee) fee %s"%(self.grp(group))
        #sql = "%s FROM dm_v6_download WHERE %s GROUP BY bookid %s" % (qtype,sub,self.grp(group))
        sql = "%s FROM %s WHERE %s GROUP BY bookid %s" % (qtype,table,sub,self.grp(group))
 
        ckey = 'fee_bk_%s_%s_%s' % (start,end,group)
        res = self.cache.get(ckey,None)
        if res is None:
            res = {}
            print sql
            self.db.execute(sql)
            for line in self.db.rows():
                by = group.split(',')
                key = ','.join([str(line[by[i]]) for i in xrange(len(by)) if by[i] != ''])
                #user, price_code = int(line['ud']),line['price_code']
                #user, fee = int(line['ud']), float(line['fee'])
                fee = float(line['fee'])
                #fee = user * self.FEE_CODE.get(price_code,0)
                if key not in res:
                    res[key] = fee
                else:
                    res[key] += fee
            self.cache[ckey] = res

        keys,fee = self.group_keys(**kargs),0
        for i in keys:
            fee += res.get(i,0)
        return fee

    def get_topN_book_bybook(self, start, end, ispay, **kargs):
        '''
        get top n book by book down 
        '''
        #sub = self.ds_sql(start,end)
        sub = ""
        sub = self.merge_sql(sub,self.charge_type_sql('book',ispay))
        table = self.get_split_table('book')
        group = self.group(**kargs)
        qtype = "SELECT COUNT(DISTINCT(%s)) ud, SUM(downloadfee) fee,SUM(downloadrealfee) real_fee, bookid %s"%(self.uid(), self.grp(group))
        #sql = "%s FROM dm_v6_download WHERE %s GROUP BY bookid %s" % (qtype,sub,self.grp(group))
        sql = "%s FROM %s WHERE %s GROUP BY bookid %s" % (qtype,table,sub,self.grp(group))

        ckey = 'topn_bk_book_%s_%s_%s_%s' % (start,end,ispay,group)
        res = self.cache.get(ckey,None)
        if res is None:
            res = {}
            print sql
            self.db.execute(sql)
            for line in self.db.rows():
                by = group.split(',')
                val = [str(line['bookid'])]
                for i in xrange(len(by)):
                    if by[i] != '':
                        val.append(str(line[by[i]]))
                #key = line['bookid'] + ',' + ','.join([str(line[by[i]]) for i in xrange(len(by)) if by[i] != ''])
                key = ','.join(val)
                bid,user,fee,real_fee = str(line['bookid']).strip(),int(line['ud']),float(line['fee']),float(line['real_fee'])
                if key in res:
                    res[key][1] += user
                    res[key][2] += fee
                    res[key][3] += real_fee
                else:
                    res[key] = [bid,user,fee,real_fee]
            result = {}
            for i in res:
                key = ','.join(i.split(',')[1:])
                result.setdefault(key,[]).append(res[i])
            res = result
            self.cache[ckey] = res
        keys,rdict = self.group_keys(**kargs),{}
        for i in keys:
            for r in res.get(i,[]):
                bid,user,fee,real_fee = r[0],r[1],r[2],r[3]
                if bid in rdict:
                    rdict[bid]['down'] += user
                    rdict[bid]['user'] += user
                    rdict[bid]['fee'] += fee
                    rdict[bid]['real_fee'] += real_fee
                else:
                    rdict[bid] = {'bid':bid,'down':user,'user':user,'fee':fee,'real_fee':real_fee}
        return rdict

    def get_book_usercnt_bychapter(self, start, end, ispay, **kargs):
        '''
        '''
        #sub = self.ds_sql(start,end)
        sub = ""
        sub = self.merge_sql(sub,self.charge_type_sql('chapter',ispay))
        table = self.get_split_table('chapter')
        group = self.group(**kargs)
        qtype = "SELECT COUNT(DISTINCT(%s)) ud,bookid %s"%(self.uid(), self.grp(group))
        #sql = "%s FROM dm_v6_download WHERE %s GROUP BY bookid %s" % (qtype,sub,self.grp(group))
        sql = "%s FROM %s WHERE %s GROUP BY bookid %s" % (qtype,table,sub,self.grp(group))

        ckey = 'usercnt_bk_chapter_%s_%s_%s_%s' % (start,end,ispay,group)
        res = self.cache.get(ckey,None)
        if res is None:
            res = {}
            print sql
            self.db.execute(sql)
            for line in self.db.rows():
                by = group.split(',')
                key = line['bookid'] + ',' + ','.join([str(line[by[i]]) for i in xrange(len(by)) if by[i] != ''])
                bid,user = str(line['bookid']).strip(),int(line['ud'])
                if key in res:
                    res[key][bid] = user
                else:
                    res[key] = {bid:user}
            self.cache[ckey] = res
 
        keys,rdict = self.group_keys(**kargs),{}
        for i in keys:
            bkmap = res.get(i,{})
            for bid in bkmap:
                user = bkmap[bid]
                if bid in rdict:
                    rdict[bid] += user
                else:
                    rdict[bid] = user
        return rdict

    def get_topN_book_bychapter(self, start, end, ispay, **kargs):
        '''
        '''
        #usercnt_dict = self.get_book_usercnt_bychapter(start,end,ispay,**kargs)
        #sub = self.ds_sql(start,end)
        sub = ""
        sub = self.merge_sql(sub,self.charge_type_sql('chapter',ispay))
        table = self.get_split_table('chapter')
        group = self.group(**kargs)
        qtype = "SELECT SUM(chaptercount) cd,COUNT(DISTINCT(%s)) ud,SUM(downloadfee) fee, SUM(downloadrealfee) real_fee, bookid %s"%(self.uid(), self.grp(group))
        #sql = "%s FROM dm_v6_download WHERE %s GROUP BY bookid %s" % (qtype,sub,self.grp(group))
        sql = "%s FROM %s WHERE %s GROUP BY bookid %s" % (qtype,table,sub,self.grp(group))

        ckey = 'topn_bk_chapter_%s_%s_%s_%s' % (start,end,ispay,group)
        res = self.cache.get(ckey,None)
        if res is None:
            res = {}
            print sql
            self.db.execute(sql)
            for line in self.db.rows():
                by = group.split(',')
                val = [str(line['bookid'])]
                #key = line['bookid'] + ',' + ','.join([str(line[by[i]]) for i in xrange(len(by)) if by[i] != ''])
                for i in xrange(len(by)):
                    if by[i] != '':
                        val.append(str(line[by[i]]))
                key = ','.join(val)
                bid,cd,down,fee,real_fee = str(line['bookid']).strip(),int(line['cd']),int(line['ud']),float(line['fee']),float(line['real_fee'])
                #fee = fee*down/cd if cd else 0
                if key in res:
                    res[key][1] += cd
                    res[key][2] += down
                    res[key][3] += fee
                    res[key][4] += real_fee
                else:
                    res[key] = [bid, cd, down, fee, real_fee]
            result = {}
            for i in res:
                key = ','.join(i.split(',')[1:])
                result.setdefault(key,[]).append(res[i])
            res = result
            self.cache[ckey] = res

        keys,rdict = self.group_keys(**kargs),{}
        for i in keys:
            for r in res.get(i,[]):
                bid,cd,down,fee,real_fee = r[0],r[1],r[2],r[3],r[4]
                if bid in rdict:
                    #rdict[bid]['down'] += down
                    rdict[bid]['user'] += down
                    rdict[bid]['down'] += cd
                    rdict[bid]['fee'] += fee
                    rdict[bid]['real_fee'] += real_fee
                else:
                    #rdict[bid] = {'bid':bid,'down':down,'user':usercnt_dict.get(bid,0),'fee':fee}
                    rdict[bid] = {'bid':bid,'down':cd,'user':down,'fee':fee,'real_fee':real_fee}
        return rdict

    def get_topN_product(self, start, end, charge_type, num, **kargs):
        #sub = self.ds_sql(start,end)
        sub = ""
        sub = self.merge_sql(sub,self.charge_type_sql(charge_type,True))
        table = self.get_split_table(charge_type)
        sub = self.merge_sql(sub,"model<>''")
        group = self.group(**kargs)
        qtype = "SELECT SUM(downloadfee) AS fee,model %s" % self.grp(group)
        #sql = "%s FROM dm_v6_download WHERE %s GROUP BY model %s" % (qtype,sub,self.grp(group))
        sql = "%s FROM %s WHERE %s GROUP BY model %s" % (qtype,table,sub,self.grp(group))

        ckey = 'topn_pro_%s_%s_%s_%s' % (start,end,charge_type,group)
        res = self.cache.get(ckey,None)
        if res is None:
            res = {}
            print sql
            self.db.execute(sql)
            for line in self.db.rows():
                by = group.split(',')
                key = ','.join([str(line[by[i]]) for i in xrange(len(by)) if by[i] != ''])
                name,fee = str(line['model']).strip(),float(line['fee'])
                res.setdefault(key,[]).append((name,fee))
            self.cache[ckey] = res

        keys,rdict,total = self.group_keys(**kargs),{},0
        for i in keys:
            for r in res.get(i,[]):
                name,fee = r[0],r[1]
                total += fee
                if name in rdict:
                    rdict[name]['fee'] += fee
                else:
                    rdict[name] = {'name':name,'type':charge_type,'fee':fee}

        rlist = rdict.values()
        rlist.sort(cmp=lambda x,y:cmp(x['fee'],y['fee']),reverse=True)
        if num:
            rlist = rlist[:num]
        other = 0
        for i in rlist:
            i['ratio'] = i['fee']/total if total else 0
            other += i['fee']
        rlist.append({'name':'other','type':charge_type,'fee':other,'ratio':other/total if total else 0})
        return total,rlist

    def get_recharge(self, start, end, **kargs):
        '''
        recharge info: to do
        start: start time
        end: end time
        kargs: run_id,plan_id,partner_id,version_name,product_name
        '''
        k,result = kargs,''
        if not k['run_id'] and not k['plan_id'] and not k['product_name'] and not k['version_name'] and k['partner_id']:
            sub = self.ds_sql(start,end)
            group = 'partner_id'
            qtype = "SELECT SUM(recharge) re, SUM(note_seeding) no %s" % (self.grp(group))
            #sql = "%s  FROM dm_v6_down_recharge WHERE %s GROUP BY partner_id" % (qtype,sub)
            #sql = "%s  FROM dm_v6_down_recharge GROUP BY partner_id" % (qtype)
            sql = "%s  FROM dm_v6_down_recharge WHERE partner_id IS NOT NULL AND recharge IS NOT NULL AND note_seeding IS NOT NULL GROUP BY partner_id" % (qtype)
            print sql
            ckey = 'recharge_%s_%s_%s' % (start,end,group)
            res = self.cache.get(ckey,None)
            if res is None:
                res={}
                self.db.execute(sql)
                for line in self.db.rows():
                    by = group.split(',')
                    key = ','.join([str(line[by[i]]) for i in xrange(len(by)) if by[i] != ''])
                    consumefee,msgfee = float(line['re']),float(line['no'])
                    res[key] = [consumefee,msgfee]
                self.cache[ckey] = res
            keys,recharge = self.group_keys(**kargs),[0,0]
            for i in keys:
                r = res.get(i,[0,0])
                recharge[0] += r[0]
                recharge[1] += r[1]
            result = '/'.join([str(i) for i in recharge]) + '/0.0'
        return result

    def get_batch_download(self, start, end, **kargs):
        #sub = ""
        #sub = self.merge_sql(sub,self.charge_type_sql('chapter',True))
        table = self.get_split_table('batch')
        group = self.group(**kargs)

        qtype = "SELECT ifnull(SUM(chaptercount),0) pv, COUNT(DISTINCT(%s)) uv, ifnull(SUM(downloadfee),0) fee, bookid %s"%(self.uid(), self.grp(group))
        sql = "%s FROM %s GROUP BY bookid %s" % (qtype,table,self.grp(group))

        ckey = 'batch_down_chapter_%s_%s_%s' % (start,end,group)
        res = self.cache.get(ckey,None)
        if res is None:
            res = {}
            print sql
            self.db.execute(sql)
            for line in self.db.rows():
                by = group.split(',')
                val = [str(line['bookid'])]
                for i in xrange(len(by)):
                    if by[i] != '':
                        val.append(str(line[by[i]]))
                key = ','.join(val)
                bid,cd,down,fee = str(line['bookid']).strip(),int(line['pv']),int(line['uv']),float(line['fee'])
                if key in res:
                    res[key][1] += cd
                    res[key][2] += down
                    res[key][3] += fee
                else:
                    res[key] = [bid, cd, down, fee]
            result = {}
            for i in res:
                key = ','.join(i.split(',')[1:])
                result.setdefault(key,[]).append(res[i])
            res = result
            self.cache[ckey] = res

        keys,rdict = self.group_keys(**kargs),{}
        for i in keys:
            for r in res.get(i,[]):
                bid,cd,down,fee = r[0],r[1],r[2],r[3]
                if bid in rdict:
                    rdict[bid]['batch_uv'] += down
                    rdict[bid]['batch_pv'] += cd
                    rdict[bid]['batch_fee'] += fee
                else:
                    rdict[bid] = {'bid':bid,'batch_pv':cd,'batch_uv':down,'batch_fee':fee}
        return rdict

    def get_batch_basic(self, start, end, **kargs):
        #sub = ""
        #sub = self.merge_sql(sub,self.charge_type_sql('chapter',True))
        table = self.get_split_table('batch')
        group = self.group(**kargs)

        qtype = "SELECT ifnull(SUM(chaptercount),0) pv, COUNT(DISTINCT(%s)) uv, ifnull(SUM(downloadfee),0) fee %s"%(self.uid(), self.grp(group))
        groupby = ('GROUP BY %s' % group) if group else ''
        sql = "%s FROM %s %s" % (qtype,table,groupby)

        ckey = 'batch_down_basic_%s_%s_%s' % (start,end,group)
        res = self.cache.get(ckey,None)
        if res is None:
            res = {}
            print sql
            self.db.execute(sql)
            for line in self.db.rows():
                by = group.split(',')
                key = ','.join([str(line[by[i]]) for i in xrange(len(by)) if by[i] != ''])
                cd,down,fee = int(line['pv']),int(line['uv']),float(line['fee'])
                if key in res:
                    res[key][1] += cd
                    res[key][2] += down
                    res[key][3] += fee
                else:
                    res[key] = [cd, down, fee]
            result = {}
            for i in res:
                key = ','.join(i.split(',')[:])
                result.setdefault(key,[]).append(res[i])
            res = result
            self.cache[ckey] = res
        keys,rdict = self.group_keys(**kargs),{'batch_pv':0,'batch_uv':0,'batch_fee':0}
        for i in keys:
            for r in res.get(i,[]):
                cd,down,fee = r[0],r[1],r[2]
                rdict['batch_pv'] += cd
                rdict['batch_uv'] += down
                rdict['batch_fee'] += fee
                #rdict = {'batch_pv':cd,'batch_uv':down,'batch_fee':fee}
            #self.cache[ckey] = res
        #keys,rdict,cd,down,fee = self.group_keys(**kargs),{},0,0,0
        #for i in keys:
        #    r = res.get(i,[0,0,0])
        #    cd = r[0]
        #    down = r[1]
        #    fee = r[2]
        #rdict = {'batch_pv':cd,'batch_uv':down,'batch_fee':fee}
        return rdict

class FactoryQuery(MySQLData):
    pass

class WapQuery(MySQLData):
    pass

if __name__ == '__main__':
    d = DownloadQuery()
    try: 
        opts,args = getopt.getopt(sys.argv[1:],'',['start=','end=','version_name=','partner_id='])
    except getopt.GetoptError,e:
        logging.error('%s\n',str(e),exc_info=True)
        sys.exit(2)
    jobtype,start,end,version_name,partner_id = '',None,None,None,None
    for o, a in opts:
        if o == '--start':
            start = datetime.datetime.strptime(a,'%Y-%m-%d')
        if o == '--end':
            end = datetime.datetime.strptime(a,'%Y-%m-%d')
        if o == '--version_name':
            version_name = a
        if o == '--partner_id':
            partner_id = a
    now = datetime.datetime.now()
    yest = now - datetime.timedelta(days=1)
    #start = now - datetime.timedelta(days=18)
    end = now
    start = yest
    #run_id = '501607'
    #partner_id = 108037
    kargs = {'version_name':None,'partner_id':None,'product_name':None,'plan_id':None}
    print '%s --> %s, kargs=%s'%(start,end,kargs)
    #print 'topN books book/pay\t',d.get_topN_book_bybook(start,end,ispay=True,**kargs)
    print d.get_batch_basic(start,end,**kargs)
    #print d.get_topN_book_bybook(start,end,ispay=True,**kargs)
    #print d.get_batch_download(start,end,**kargs)
    #print d.get_pay_user_count(start,end,**kargs)


