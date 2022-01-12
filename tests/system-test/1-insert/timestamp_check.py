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
from util.log import *
from util.cases import *
from util.sql import *
from util.common_refactor import tdCom
import copy
class TDTestCase:
    def __init__(self):
        self.err_case = 0
        self.curret_case = 0

    def caseDescription(self):

        '''
        case1 <jayden>: [TD-11282] : db name length check (max 32);\n
        case2 <jayden>: [TD-11282] : unsupport backquote;\n
        case3 <jayden>: [TD-11282] : case insensitive;\n
        case4 <jayden>: [TD-11282] : illegal dbname check
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        tdSql.execute('reset query cache')

    def ms_us_ns_db_check(self):
        '''
            precision = ["ms", "us", "ns"]
        '''
        for ts in ["ms", "us", "ns"]:
            dbname = tdCom.get_long_name(len=5, mode="letters")
            tdSql.execute(f'create database if not exists {dbname} precision "{ts}"')
            res = tdSql.query('show databases', True)
            tdSql.check_equal(res[0][16], ts)
            tdSql.execute(f'create table {dbname}.{dbname} (ts timestamp, c1 int)')
            timestamp, dt = tdCom.genTs(ts)
            tdSql.execute(f'insert into {dbname}.{dbname} values ({timestamp}, 1)')
            res = tdSql.query(f'select ts from {dbname}.{dbname}', True)
            tdSql.check_equal(str(res[0][0]), str(dt))
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
        for dbname in [tdCom.get_long_name(len=5, mode="letters_mixed"), tdCom.get_long_name(len=5, mode="letters_mixed").upper()]:
            tdSql.execute(f'create database if not exists {dbname}')
            res = tdSql.query('show databases', True)
            tdSql.check_equal(res[0][16], dbname.lower())
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
        for insert_str in tdCom.gen_symbol_list():
            d_list = list(dbname)
            for i in range(len(d_list)+1):
                d_list_new = copy.deepcopy(d_list)
                d_list_new.insert(i, insert_str)
                dbname_new = ''.join(d_list_new)
                tdSql.error(f'create database if not exists `{dbname_new}`')

    def run(self):
        self.ms_us_ns_db_check()
        # self.dbname_backquote_unsupport_check()
        # self.upper_lower_dbname_check()
        # self.illegal_dbname_check()

        if self.err_case > 0:
            tdLog.exit(f"{self.err_case} failed")
        else:
            tdLog.success("4 cases passed")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
