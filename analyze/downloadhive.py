#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import getopt
import datetime
import logging

sys.path.append(os.path.dirname(os.path.split(os.path.realpath(__file__))[0]))

from lib.utils import time_start
from analyze import HiveQuery

class DownloadQuery(HiveQuery):
    '''
    query data from hive table download_v6
    '''
    # fee code => fee
    FEE_CODE = {'1':0,'8':0,'3':0,'4':0,'5':0,'6':12,'7':1,'2':2,'9':0,'10':0,
                '11':2,'12':8,'13':2,'14':5,'15':0,'16':3,'17':2,'18':3,'19':0,'20':2,
                '21':2,'22':2,'23':2,'24':2,'25':0,'26':4,'27':5,'28':3,'29':2,'30':2,
                '31':6,'32':2,'33':2,'34':2,'35':2,'36':2,'37':0,'38':2}
    def __init__(self, host, port):
        '''
        init hive client, preprocess fee code
        '''
        super(DownloadQuery,self).__init__(host,port)
        self.fcode_map = {}
        for k,v in self.FEE_CODE.items():
            self.fcode_map.setdefault(v,[]).append(k)
        self.cache = {}

    def reset_cache(self):
        self.cache = {}

    def pr_sql(self, ispay):
        '''
        针对按本计费, 按章计费的计算不用管pr
        '''
        prlist = [k for k in self.FEE_CODE if self.FEE_CODE[k]>0] 
        sql = 'price_code in (%s)' % ','.join(prlist)
        if not ispay:
            sql = "not %s" % sql
        return sql
    
    def charge_type_sql(self, charge_type, ispay):
        '''
        按本计费、按章下载时，fu=10,downtype=2
        '''
        assert charge_type in ('book','chapter')
        if charge_type == 'book':
            if ispay:
                sql = "down_type in (1,2,4,5,6,7) AND fee_unit='10' AND %s" % self.pr_sql(ispay)
            else:
                sql = "down_type in (1,4,5,6,7) AND fee_unit='10' AND %s" % self.pr_sql(ispay)
        else:
            if ispay:
                sql = "down_type=2 AND fee_unit='20' AND price>0 AND price<100"
            else:
                sql = "down_type=2 AND fee_unit='20' AND (price=0 OR price='')"
        return sql
    
    def get_pay_user_count(self, start, end, **kargs):
        '''
        paying user count
        start: start time
        end: end time
        kargs:run_id,partner_id,version_name,product_name
        '''
        sub = "((fee_unit='10' AND %s) OR (fee_unit='20' AND price>0))" % self.pr_sql(True)
        sub = self.merge_sql(sub,self.ds_sql(start,end))
        group = self.group(**kargs)
        groupby = ('GROUP BY %s' % group) if group else ''
        sql = "SELECT COUNT(DISTINCT(%s)) %s FROM download_v6 WHERE %s %s" % (self.uid(),self.grp(group),sub,groupby)
        ckey = 'pay_ucnt_%s_%s_%s'%(start,end,group)
        res = self.cache.get(ckey,None)
        if res is None:
            res = {}
            self.execute(sql)
            for i in self.client.fetchAll():
                arr = i.split('\t')
                key,val = ','.join(arr[1:]),int(arr[0])
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
        sub = self.charge_type_sql(charge_type,ispay)
        sub = self.merge_sql(sub,self.ds_sql(start,end))
        group = self.group(**kargs)
        groupby = 'GROUP BY %s' % group if group else ''
        qtype = 'SELECT COUNT(DISTINCT(CONCAT(%s,bid%s)))'%(self.uid(),',cid' if charge_type=='chapter' else '')
        if is_user_unique:
            qtype = 'SELECT COUNT(DISTINCT(%s))'%self.uid()
        sql = "%s %s FROM download_v6 WHERE %s %s"%(qtype,self.grp(group),sub,groupby)
        ckey = 'dcnt_%s_%s_%s_%s_%s_%s'%(start,end,charge_type,ispay,is_user_unique,group)
        res = self.cache.get(ckey,None)
        if res is None:
            res = {}
            self.execute(sql)
            for i in self.client.fetchAll():
                arr = i.split('\t')
                key,val = ','.join(arr[1:]),int(arr[0])
                res[key] = val
            self.cache[ckey] = res
        keys,count = self.group_keys(**kargs),0
        for i in keys:
            count += res.get(i,0)
        return count

    def get_fee_by_book(self, start, end, **kargs):
        '''
        fee sum by book
        start: start time
        end: end time
        kargs: run_id,partner_id,version_name,product_name
        '''
        sub = self.ds_sql(start,end)
        sub = self.merge_sql(sub,self.charge_type_sql('book',True))
        group = self.group(**kargs)
        qtype = "SELECT COUNT(DISTINCT(%s)),price_code %s"%(self.uid(),self.grp(group))
        sql = "%s FROM download_v6 WHERE %s GROUP BY bid,price_code %s" % (qtype,sub,self.grp(group))
        ckey = 'fee_bk_%s_%s_%s' % (start,end,group)
        res = self.cache.get(ckey,None)
        if res is None:
            self.execute(sql)
            res = {}
            for i in self.client.fetchAll():
                arr = i.split('\t')
                key,user,price_code = ','.join(arr[2:]),int(arr[0]),arr[1]
                fee = user * self.FEE_CODE.get(price_code,0)
                if key not in res:
                    res[key] = fee
                else:
                    res[key] += fee
            self.cache[ckey] = res
        keys,fee = self.group_keys(**kargs),0
        for i in keys:
            fee += res.get(i,0)
        return fee 
     
    def get_fee_by_chapter(self, start, end, **kargs):
        '''
        fee sum by chapter
        start: start time
        end: end time
        kargs: run_id,partner_id,version_name,product_name
        '''
        sub = self.ds_sql(start,end)
        sub = self.merge_sql(sub,self.charge_type_sql('chapter',True))
        group = self.group(**kargs)
        sql = "SELECT COUNT(1),COUNT(DISTINCT(%s)),SUM(price) %s FROM download_v6 WHERE %s GROUP BY bid,cid %s" % (self.uid(),self.grp(group),sub,self.grp(group))
        ckey = 'fee_ch_%s_%s_%s' % (start,end,group)
        res = self.cache.get(ckey,None)
        if res is None:
            self.execute(sql)
            res = {}
            for i in self.client.fetchAll():
                arr = i.split('\t')
                key,down,user,fee = ','.join(arr[3:]),int(arr[0]),int(arr[1]),float(arr[2])
                val = fee*user/down if down else 0
                if key in res:
                    res[key] += val
                else:
                    res[key] = val
            self.cache[ckey] = res
        keys,fee = self.group_keys(**kargs),0
        for i in keys:
            fee += res.get(i,0)
        return fee 

    def get_topN_book_bybook(self, start, end, ispay, **kargs):
        '''
        get top n book by book down 
        '''
        sub = self.ds_sql(start,end)
        sub = self.merge_sql(sub,self.charge_type_sql('book',ispay))
        group = self.group(**kargs)
        qtype = "SELECT COUNT(DISTINCT(%s)),price_code,bid %s"%(self.uid(),self.grp(group))
        sql = "%s FROM download_v6 WHERE %s GROUP BY price_code,bid %s" % (qtype,sub,self.grp(group))
        ckey = 'topn_bk_book_%s_%s_%s_%s' % (start,end,ispay,group)
        res = self.cache.get(ckey,None)
        if res is None:
            res = {}
            self.execute(sql)
            for i in self.client.fetchAll():
                arr = i.split('\t')
                key,bid,user,price_code = ','.join(arr[2:]),arr[2].strip(),int(arr[0]),arr[1]
                fee = user*self.FEE_CODE.get(price_code,0)
                if key in res:
                    res[key][1] += user 
                    res[key][2] += fee
                else:
                    res[key] = [bid,user,fee]
            result = {}
            for i in res:
                key = ','.join(i.split(',')[1:])
                result.setdefault(key,[]).append(res[i])
            res = result
            self.cache[ckey] = res
        keys,rdict = self.group_keys(**kargs),{}
        for i in keys:
            for r in res.get(i,[]):
                bid,user,fee = r[0],r[1],r[2]
                if bid in rdict:
                    rdict[bid]['down'] += user
                    rdict[bid]['user'] += user
                    rdict[bid]['fee'] += fee
                else:
                    rdict[bid] = {'bid':bid,'down':user,'user':user,'fee':fee}
        return rdict

    def get_topN_book_bychapter(self, start, end, ispay, **kargs):
        '''
        '''
        usercnt_dict = self.get_book_usercnt_bychapter(start,end,ispay,**kargs)
        sub = self.ds_sql(start,end)
        sub = self.merge_sql(sub,self.charge_type_sql('chapter',ispay))
        group = self.group(**kargs)
        qtype = "SELECT COUNT(1) AS cnt,COUNT(DISTINCT(%s)),SUM(price),bid %s"%(self.uid(),self.grp(group))
        sql = "%s FROM download_v6 WHERE %s GROUP BY bid,cid %s" % (qtype,sub,self.grp(group))
        ckey = 'topn_bk_chapter_%s_%s_%s_%s' % (start,end,ispay,group)
        res = self.cache.get(ckey,None)
        if res is None:
            res = {}
            self.execute(sql)
            for i in self.client.fetchAll():
                arr = i.split('\t')
                bid = arr[3].strip()
                key,bid,cnt,down,fee = ','.join(arr[3:]),arr[3].strip(),int(arr[0]),int(arr[1]),float(arr[2])
                fee = fee*down/cnt if cnt else 0
                if key in res:
                    res[key][1] += down
                    res[key][2] += fee
                else:
                    res[key] = [bid,down,fee]
            result = {}
            for i in res:
                key = ','.join(i.split(',')[1:])
                result.setdefault(key,[]).append(res[i])
            res = result
            self.cache[ckey] = res
        keys,rdict = self.group_keys(**kargs),{}
        for i in keys:
            for r in res.get(i,[]):
                bid,down,fee = r[0],r[1],r[2]
                if bid in rdict:
                    rdict[bid]['down'] += down
                    rdict[bid]['fee'] += fee
                else:
                    rdict[bid] = {'bid':bid,'down':down,'user':usercnt_dict.get(bid,0),'fee':fee}
        return rdict

    def get_book_usercnt_bychapter(self, start, end, ispay, **kargs):
        '''
        '''
        sub = self.ds_sql(start,end)
        sub = self.merge_sql(sub,self.charge_type_sql('chapter',ispay))
        group = self.group(**kargs)
        qtype = "SELECT COUNT(DISTINCT(%s)),bid %s"%(self.uid(),self.grp(group))
        sql = "%s FROM download_v6 WHERE %s GROUP BY bid %s" % (qtype,sub,self.grp(group))
        ckey = 'usercnt_bk_chapter_%s_%s_%s_%s' % (start,end,ispay,group)
        res = self.cache.get(ckey,None)
        if res is None:
            res = {}
            self.execute(sql)
            for i in self.client.fetchAll():
                arr = i.split('\t')
                key,bid,user = ','.join(arr[2:]),arr[1].strip(),int(arr[0])
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

    def get_topN_product(self, start, end, charge_type, num, **kargs):
        sub = self.ds_sql(start,end)
        sub = self.merge_sql(sub,self.charge_type_sql(charge_type,True))
        sub = self.merge_sql(sub,"product_name<>''")
        group = self.group(**kargs)
        qtype = "SELECT SUM(price) AS fee,product_name %s" % self.grp(group)
        sql = "%s FROM download_v6 WHERE %s GROUP BY product_name %s" % (qtype,sub,self.grp(group))
        ckey = 'topn_pro_%s_%s_%s_%s' % (start,end,charge_type,group)
        res = self.cache.get(ckey,None)
        if res is None:
            res = {}
            self.execute(sql)
            for i in self.client.fetchAll():
                arr = i.split('\t')
                key,name,fee = ','.join(arr[2:]),arr[1].strip(),float(arr[0])
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
            qtype = "SELECT round(SUM(rechargingnum),2), round(SUM(case prcode when '2' then cast(price as double) else 0.0 end),2),round(SUM(giftrechargingnum),2) %s" % (self.grp(group))
            sql = "%s  FROM uc_down_recharge WHERE %s GROUP BY partner_id" % (qtype,sub)
            ckey = 'recharge_%s_%s_%s' % (start,end,group)
            res = self.cache.get(ckey,None)
            if res is None:
                res={}
                self.execute(sql)
                for i in self.client.fetchAll():
                    arr = i.split('\t')
                    key,consumefee,msgfee,giftfee = ','.join(arr[3:]),float(arr[0]),float(arr[1]),float(arr[2])
                    res[key] = [consumefee,msgfee,giftfee]
                self.cache[ckey] = res
            keys,recharge = self.group_keys(**kargs),[0,0,0]
            for i in keys:
                r = res.get(i,[0,0,0])
                recharge[0] += r[0]
                recharge[1] += r[1]
                recharge[2] += r[2]
            result = '/'.join([str(i) for i in recharge])
        return result

if __name__ == '__main__':
    d = DownloadQuery('192.168.0.150','10000')
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
    #now = now - datetime.timedelta(days=3)
    end = now
    start = now - datetime.timedelta(days=1)
    #version_name = "ireader_2.2"
    run_id = '501607'
    kargs = {'version_name':version_name,'partner_id':partner_id,'run_id':run_id}
    print '%s --> %s, kargs=%s'%(start,end,kargs)
    #print d.get_recharge(start,end,**kargs)
    #print '付费用户数\t',d.get_pay_user_count(start,end,**kargs)
    #print '按章付费下载数\t',d.get_downcount(start,end,'chapter',ispay=True,**kargs)
    #print '按本付费下载数\t',d.get_downcount(start,end,'book',ispay=True,**kargs)
    #print '按章付费下载用户数\t',d.get_downcount(start,end,'chapter',ispay=True,is_user_unique=True,**kargs)
    #print '按本付费下载用户数\t',d.get_downcount(start,end,'book',ispay=True,is_user_unique=True,**kargs)
    #print '按章免费下载数\t',d.get_downcount(start,end,'chapter',**kargs)
    #print '按本免费下载数\t',d.get_downcount(start,end,'book',**kargs)
    #print '按章免费下载用户数\t',d.get_downcount(start,end,'chapter',is_user_unique=True,**kargs)
    #print '按本免费下载用户数\t',d.get_downcount(start,end,'book',is_user_unique=True,**kargs)
    #print '按章收入\t',d.get_fee_by_chapter(start,end,**kargs)
    #print '按本收入\t',d.get_fee_by_book(start,end,**kargs)
    #print '按章收入\t',d.get_fee_by_chapter(start,end,version_name=version_name,partner_id=partner_id)
    #print 'topN books chapter/pay\t',d.get_topN_book_bychapter(start,end,ispay=0,**kargs)['1023927']
    #print 'topN books chapter/pay\t',d.get_topN_book_bychapter(start,end,ispay=True,version_name='ireader_1.8')
    #print 'topN books chapter/pay\t',d.get_topN_book_bychapter(start,end,ispay=True,**kargs)
    print 'topN books book/pay\t',d.get_topN_book_bybook(start,end,ispay=True,**kargs)
    #print 'topN books book/pay\t',d.get_topN_book_bybook(start,end,ispay=True,**kargs)['1023927']
    #print 'topN books book/free\t',d.get_topN_book_bybook(start,end,ispay=False,**kargs)['1023927']
    #print 'topN product/book\t',d.get_topN_product(start,end,'book',num=9,**kargs)
    #print 'topN product/chapter\t',d.get_topN_product(start,end,'chapter',num=9,**kargs)
    print datetime.datetime.now() - now
    
