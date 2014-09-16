#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Abstract: run platform Model

import os
import sys
import datetime

sys.path.append(os.path.dirname(os.path.split(os.path.realpath(__file__))[0]))

from lib.database import Model

class Run(Model):
    '''
    run platform model, such as ios，android，wp7
    '''
    _db = 'logstatV2'
    _table = 'run_platform'
    _pk = 'id'
    _fields = set(['id','run_id','run_name','status','uptime'])
    _scheme = ("`id` BIGINT NOT NULL AUTO_INCREMENT",
               "`run_id` INT NOT NULL DEFAULT '0'",
               "`run_name` varchar(32) NOT NULL DEFAULT ''",
               "`status` enum('hide','pas','nice') NOT NULL DEFAULT 'pas'",
               "`uptime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
               "PRIMARY KEY `idx_id` (`id`)",
               "UNIQUE KEY `run_id` (`run_id`)",
               "UNIQUE KEY `run_name` (`run_name`)")

if __name__ == "__main__":
    r = Run.new()
    r.init_table()

