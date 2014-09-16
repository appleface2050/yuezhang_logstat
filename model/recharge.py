#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Abstract: recharge log Model

import os
import sys
import datetime
from lib.database import Model
  
class RechargeLog(Model):
    '''
    '''
    _db = 'book'
    _table = 'zy_user_recharginglog'
    _pk = 'id'
    _fields = set(['bktype','bk','DATE'])

class OrderOfRecharge(Model):
    '''
    '''
    _db = 'book'
    _table = 'zy_user_orderofrecharging'
    _pk = 'id'
    _fields = set(['bktype','bk','DATE'])

class FactorySumStat(Model):
    '''
    '''
    _db = 'logstatV2'
    _table = 'factory'
    _pk = 'id'
    _fields = set(['id'])
