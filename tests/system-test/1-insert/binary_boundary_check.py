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
        case1 <jayden>: [TD-11282] : binary length check (max 16374);\n
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        tdSql.prepare()

    def binary_length_check(self):
        '''
            max length: 16374
        '''
        str_16374 = tdCom.get_long_name(len=16374, mode="letters")
        str_16375 = tdCom.get_long_name(len=16375, mode="letters")
        tdSql.execute(f'create stable if not exists db.stb (col_ts timestamp, c1 binary(16374)) tags (t1 binary(16374))')
        tdSql.execute(f'create table if not exists db.tb using db.stb tags ("{str_16374}")')
        tdSql.execute(f'insert into db.tb values (now, "{str_16374}")')
        res = tdSql.query(f'select t1, c1 from db.tb', True)
        tdSql.check_equal(str(res[0][0]), str_16374)
        tdSql.check_equal(str(res[0][1]), str_16374)
        tdSql.error(f'create stable if not exists db.stb_error1 (col_ts timestamp, c1 binary(16374)) tags (t1 binary(16375))')
        tdSql.error(f'create stable if not exists db.stb_error2 (col_ts timestamp, c1 binary(16375)) tags (t1 binary(16374))')
        tdSql.error(f'create table if not exists db.tb using db.stb tags (now-2h, "{str_16375}")')
        tdSql.error(f'insert into db.tb values (now-1h, "{str_16375}")')
        tdSql.execute(f'drop database if exists db')

    def run(self):
        self.binary_length_check()

        if self.err_case > 0:
            tdLog.exit(f"{self.err_case} failed")
        else:
            tdLog.success("1 cases passed")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
