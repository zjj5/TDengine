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
        case1 <jayden>: [TD-11282] : nchar length check (max 4093);\n
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        tdSql.prepare()

    def nchar_length_check(self):
        '''
            max length: 4093
        '''
        str_4093 = tdCom.get_long_name(len=4093, mode="letters")
        str_4094 = tdCom.get_long_name(len=4094, mode="letters")
        tdSql.execute(f'create stable if not exists db.stb (col_ts timestamp, c1 nchar(4093)) tags (t1 nchar(4093))')
        tdSql.execute(f'create table if not exists db.tb using db.stb tags ("{str_4093}")')
        tdSql.execute(f'insert into db.tb values (now, "{str_4093}")')
        res = tdSql.query(f'select t1, c1 from db.tb', True)
        tdSql.check_equal(str(res[0][0]), str_4093)
        tdSql.check_equal(str(res[0][1]), str_4093)
        tdSql.error(f'create stable if not exists db.stb_error1 (col_ts timestamp, c1 nchar(4093)) tags (t1 nchar(4094))')
        tdSql.error(f'create stable if not exists db.stb_error2 (col_ts timestamp, c1 nchar(4094)) tags (t1 nchar(4093))')
        tdSql.error(f'create table if not exists db.tb using db.stb tags (now-2h, "{str_4094}")')
        tdSql.error(f'insert into db.tb values (now-1h, "{str_4094}")')
        tdSql.execute(f'drop database if exists db')

    def run(self):
        self.nchar_length_check()

        if self.err_case > 0:
            tdLog.exit(f"{self.err_case} failed")
        else:
            tdLog.success("1 cases passed")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
