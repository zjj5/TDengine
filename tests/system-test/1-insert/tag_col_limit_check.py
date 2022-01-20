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
        case1 <jayden>: [TD-11282] : check db ms/us/ns precision;\n
        case2 <jayden>: [TD-11282] : check ts second-level >= 60;\n
        case3 <jayden>: [TD-11282] : human date check;\n
        case4 <jayden>: [TD-11282] : now check;\n
        case5 <jayden>: [TD-11282] : epoch check;\n
        case6 <jayden>: [TD-11282] : erro check;\n

        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        tdSql.execute('reset query cache')

    def tag_max_count_check(self):
        '''
            max count: 128
        '''
        tag_str_exceed = tdCom.gen_tag_col_str("tag", "int", 129)
        tag_str = tdCom.gen_tag_col_str("tag", "int", 128)
        dbname = tdCom.get_long_name(len=5, mode="letters")
        tdSql.execute(f'create database if not exists {dbname} precision "ms"')
        tdSql.error(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 int) tags ({tag_str_exceed})')
        tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 int) tags ({tag_str})')
        tag_value_str = '1, ' * 127 + '1' 
        tdSql.execute(f'create table if not exists {dbname}.tb using {dbname}.stb tags ({tag_value_str})')
        tdSql.execute(f'insert into {dbname}.tb values (now, 1)')
        res = tdSql.query(f'select tag127 from {dbname}.stb', True)
        tdSql.check_equal(int(res[0][0]), 1)
        tdSql.execute(f'drop database if exists {dbname}')

    def run(self):
        self.tag_max_count_check()
        
        if self.err_case > 0:
            tdLog.exit(f"{self.err_case} failed")
        else:
            tdLog.success("5 cases passed")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
