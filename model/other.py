#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Abstract: Stat Models

import datetime
from lib.database import Model
from model import TemplateModel
from model.factory import Factory
from model.stat import Partner
from conf.settings import WAP_PAGE_TYPE,WAP_PAGE_TYPE_BASIC


class Ebk5BookExpand(TemplateModel):
    '''
    polarisebk5.ebk5_book_expand
    '''
    _db = 'polarisebk5'
    _table = 'ebk5_book_expand'
    _fields = set(['ID','book_id','last_chapter_time','fee_user_num','category_id_base','chap_list_version','chap_list_tip_content',
    'new_edit_bookid','book_uv','book_pv','fee_download_num','free_user_num','free_download_num','consume_money','creat_time'])

class HuangBook(TemplateModel):
    '''
    '''
    _db = 'logstatV2'
    _table = 'temp_huang_book'
    _fields = set(['book_id']) 
    _scheme = ("`book_id` INT NOT NULL DEFAULT '0'",
                "PRIMARY KEY `idx_book_id` (`book_id`)",
                "UNIQUE KEY `temp_huang_book_bookid` (`book_id`)")

    
class Temp(TemplateModel):
    '''
    temp table 
    '''
    _db = 'logstat'
    _table = 'temp'
    _fields = set(['id','book_id','pv','uv','pay_down','free_down','fee','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`book_id` INT NOT NULL DEFAULT '0'",
               "`pv` INT NOT NULL DEFAULT '0'",
               "`uv` INT NOT NULL DEFAULT '0'",
               "`pay_down` INT NOT NULL DEFAULT '0'",
               "`free_down` INT NOT NULL DEFAULT '0'",
               "`fee` float NOT NULL DEFAULT '0'",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `bookid` (`book_id`)")

    def get_all_data(self):
        res = Temp.mgr().raw("select book_id,pv,uv,fee,pay_down,free_down from temp")
        return res

class Ebk5Category(TemplateModel):
    '''
    polarisebk5.ebk5_category_v6
    '''
    _db = 'polarisebk5'
    _table = 'ebk5_category_v6'
    _fields = set(['ID','category_name','creat_time','valid','category_id_v3','parent_category_id_v5','child_category_id_v5','parent_id'])

    def get_all_category(self):
        sub = ','.join([i for i in self._fields])    
        qtype = 'SELECT %s' % sub
        q = self.Q(qtype=qtype)
        return q

    def get_cate_ID_by_bookid(self,bookid):
        '''
        SELECT eb.id,eb.`bookName`,ec.`ID` as category1,ecp.`ID` as category2,ecpp.`ID` as category3 
        FROM ebk5_book  eb 
        LEFT JOIN ebk5_category_v6  ec ON eb.`category_id_v6`=ec.`ID`
        LEFT JOIN ebk5_category_v6 ecp ON ec.`parent_id`=ecp.`ID`
        LEFT JOIN ebk5_category_v6 ecpp ON ecpp.`ID`=ecp.`parent_id` 
        where eb.`ID`= '10013583';
        '''
        sql = '''
        SELECT eb.id,eb.`bookName`,ec.`ID` as category1,ecp.`ID` as category2,ecpp.`ID` as category3 
        FROM ebk5_book  eb 
        LEFT JOIN ebk5_category_v6  ec ON eb.`category_id_v6`=ec.`ID`
        LEFT JOIN ebk5_category_v6 ecp ON ec.`parent_id`=ecp.`ID`
        LEFT JOIN ebk5_category_v6 ecpp ON ecpp.`ID`=ecp.`parent_id` 
        where eb.`ID`= %s;
        ''' % (bookid)
        q = self.raw(sql)
        return q

    def get_cate_name_by_cateID(self,cateID):
        qtype = 'SELECT category_name'
        q = self.Q(qtype=qtype).filter(ID=cateID)
        return q

    def get_parent_category_by_cateID(self, cateID):
        cate3 = cateID
        qtype = 'SELECT parent_id'
        q = self.Q(qtype=qtype).filter(ID=cate3)[0]
        cate2 = q['parent_id']
        q2 = self.Q(qtype=qtype).filter(ID=cate2)[0]
        cate1 = q2['parent_id']
        return {'cate1':cate1,'cate2':cate2,'cate3':cate3}







