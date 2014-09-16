#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import os
import sys

sys.path.append(os.path.dirname(os.path.split(os.path.realpath(__file__))[0]))

from lib.utils import time_start
from model.factory import Factory,Partner
from model.stat import Scope
from model.user import Perm,PermAttr,Role,RolePerm,User,UserRole

def merge_perm_attr_sql():
    '''
    from perm and perm.name find factory id 
    generate perm_attr insert sql
    '''
    ext = "uptime >='2013-12-01' AND id NOT IN (SELECT perm_id FROM perm_attr) and id!=262"
    perms = Perm.mgr().Q().extra(ext)
    if perms:
        for i in perms:
            fname = i['name'].split('-')[3]
            factorys = Factory.mgr().Q().filter(name=fname)
            if not factorys:
                print fname
            i['factory_id'] = factorys[0]['id']
# merge sql
    for i in perms:
        sql1 = "insert into `logstatv3`.`perm_attr`(`id`,`perm_id`,`attr_name`,`attr_val`,`uptime`) values ( NULL,'%s','factory_id','%s',CURRENT_TIMESTAMP);"%(i['id'],i['factory_id'])
        sql2 = "insert into `logstatv3`.`perm_attr`(`id`,`perm_id`,`attr_name`,`attr_val`,`uptime`) values ( NULL,'%s','group','all',CURRENT_TIMESTAMP);"%(i['id'])
        print sql1
        print sql2

def merge_role_attr_sql():
    '''
    insert into `logstatv3`.`role_perm`(`id`,`role_id`,`perm_id`,`uptime`) values ( NULL,'204','523',CURRENT_TIMESTAMP)
    insert into `logstatv3`.`role_perm`(`id`,`role_id`,`perm_id`,`uptime`) values ( NULL,'204','524',CURRENT_TIMESTAMP)
    '''
    ext = "id NOT IN (SELECT DISTINCT role_id FROM role_perm) AND uptime >='2013-12-01'"
    roles = Role.mgr().Q().extra(ext)
    print len(roles)
    if roles:
        for i in roles:
            perm_name1 = "查看-厂商结算-厂商收入-"+i['name'].split('acc')[0]
            perm_name2 = "查看-厂商结算-机型日报-"+i['name'].split('acc')[0]
            #print perm_name1
            #print perm_name2
            perm_id1 = Perm.mgr().Q().filter(name=perm_name1)
            perm_id2 = Perm.mgr().Q().filter(name=perm_name2)
            if not perm_id1:
                print i
                print perm_name1
            if not perm_id2:
                print i
                print perm_name2
            #print i['id']
            #print perm_id1[0]['id']
            #print perm_id2[0]['id']
            sql1 = "insert into `logstatv3`.`role_perm`(`id`,`role_id`,`perm_id`,`uptime`) values ( NULL,'%s','%s',CURRENT_TIMESTAMP);"%(i['id'],perm_id1[0]['id'])
            sql2 = "insert into `logstatv3`.`role_perm`(`id`,`role_id`,`perm_id`,`uptime`) values ( NULL,'%s','%s',CURRENT_TIMESTAMP);"%(i['id'],perm_id2[0]['id'])
            print sql1
            print sql2

def merge_user_role_sql():
    ext = "id NOT IN (SELECT DISTINCT uid FROM user_role)"    
    users = User.mgr().Q().extra(ext)
    print len(users)
    if users:
        for i in users:
            role_name = i['name'].split('_')[0]+"acc数据查看"
            role_id = Role.mgr().Q().filter(name=role_name)
            if role_id:
                sql = "insert into `logstatv3`.`user_role`(`id`,`uid`,`role_id`,`uptime`) values ( NULL,'%s','%s',CURRENT_TIMESTAMP);"%(i['id'],role_id[0]['id'])
                print sql
            else:
                print "not merge",role_name

def import_user():
    '''
    from a file split by \t import User
    深圳欣沣晟通讯_acc  145605
    '''
    fn = open("/home/zongkai/logstat_mysql/logstat/tools/import_user.txt")
    for i in fn.readlines():
        line = i.strip().split('\t')
        name = line[0]
        passwd = line[1]
        try: 
            u = User.new()
            u.name = name
            u.passwd = passwd
            u.is_root = 0
            u.is_staff = 0
            u.orgid = 18
            u.email = name+"@163.com"
        
            #print u
            #u.save()
        except:
            print u


if __name__ == "__main__":
    #merge_perm_attr_sql()
    #merge_role_attr_sql()
    #import_user() #will save into User
    merge_user_role_sql()



