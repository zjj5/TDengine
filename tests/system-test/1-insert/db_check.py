###################################################################
#           Copyright (c) 2020 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import taos
import time

from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *
from util.common_refactor import tdCom
import copy


class TDTestCase:
    def __init__(self):
        self.err_case = 0
        self.curret_case = 0

    def caseDescription(self):

        '''
        case1 <jayden>: [TD-11282] : db name length check ;\n
        case2 <jayden>: [TD-11282] : "group by ts order by first-tag" should return error
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        tdSql.execute('reset query cache')

    def dbname_length_check(self):
        '''
            max length: 32
        '''
        dbname = tdCom.get_long_name(len=32, mode="letters")
        tdSql.execute(f'create database if not exists {dbname}')
        res = tdSql.query('show databases', True)
        tdSql.check_equal(res[0][0], dbname)
        dbname_exceed = tdCom.get_long_name(len=33, mode="letters")
        tdSql.error(f'create database if not exists {dbname_exceed}')
        tdSql.execute(f'drop database if exists {dbname}')

    def dbname_backquote_unsupport_check(self):
        '''
            backquote unsupported
        '''
        dbname = tdCom.get_long_name(len=5, mode="letters")
        tdSql.error(f'create database if not exists `{dbname}`')

    def upper_lower_dbname_check(self):
        '''
            case insensitive
        '''
        for dbname in [tdCom.get_long_name(len=5, mode="mixed"), tdCom.get_long_name(len=5, mode="mixed").upper()]:
            tdSql.execute(f'create database if not exists {dbname}')
            res = tdSql.query('show databases', True)
            tdSql.check_equal(res[0][0], dbname.lower())
            tdSql.execute(f'drop database if exists {dbname}')

    def illegal_dbname_check(self):
        '''
            starts with number
            mixed invalid symbol
            mixed space
        '''
        dbname = '1' + tdCom.get_long_name(len=5, mode="letters")
        tdSql.error(f'create database if not exists {dbname}')
        dbname = tdCom.get_long_name(len=3, mode="letters")
        for insert_str in [' ', '~', '`', '!', '@', '#', '$', '¥', '%', '^', '&', '*', '(', ')', 
                    '-', '+', '=', '{', '「', '[', ']', '}', '」', '、', '|', '\\', ':', ';', '\'', 
                    '\"', ',', '<', '《', '.', '>', '》', '/', '?']:
            d_list = list(dbname)
            for i in range(len(d_list)+1):
                d_list_new = copy.deepcopy(d_list)
                d_list_new.insert(i, insert_str)
                dbname_new = ''.join(d_list_new)
                tdSql.error(f'create database if not exists `{dbname_new}`')


    def run(self):
        self.dbname_length_check()
        self.dbname_backquote_unsupport_check()
        self.upper_lower_dbname_check()
        self.illegal_dbname_check()

        if self.err_case > 0:
            tdLog.exit(f"{self.err_case} is failed")
        else:
            tdLog.success("2 case is all passed")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
