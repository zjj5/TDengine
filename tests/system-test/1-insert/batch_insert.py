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
class TDTestCase:
    def __init__(self):
        self.err_case = 0
        self.curret_case = 0

    def caseDescription(self):

        '''
        case1 <jayden>: [TD-11282] : batch_insert;

        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        tdSql.execute('reset query cache')

    def batch_insert(self):
        '''
            batch_insert 
        '''
        dbname = tdCom.get_long_name(len=5, mode="letters")
        tdSql.execute(f'create database if not exists {dbname}')
        tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 tinyint unsigned) tags \
            (tag_ts timestamp, t1 tinyint, t2 smallint, t3 int, t4 bigint, t5 tinyint unsigned)')
        tdSql.execute(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now, 1, 2, 3, 4, 5)')
        tdSql.execute(f'insert into {dbname}.tb values (now, 1, 2, 3, 4, 5), (now+1h, 1, 2, 3, 4, 5), (now+2h, 1, 2, 3, 4, 5);')
        res = tdSql.query(f'select count(*) from {dbname}.stb', True)
        tdSql.check_equal(int(res[0][0]), 3)
        tdSql.execute(f'insert into {dbname}.tb (col_ts, c1, c2) values (now-1h, 1, 2),(now-2h, 1, 2),(now-3h, 1, 2)')
        res = tdSql.query(f'select count(*) from {dbname}.stb', True)
        tdSql.check_equal(int(res[0][0]), 6)
        tdSql.error(f'insert into {dbname}.tb (col_ts, c1, c2, c9) values (now-1h, 1, 2, 1), (now-2h, 1, 2, 1), (now-2h, 1, 2, 1)')
        tdSql.error(f'insert into {dbname}.tb (col_ts, c1, c2) values (now-1h, 1, "binary"), (now-2h, 1, 2), (now-2h, 1, 2)')
        tdSql.error(f'insert into {dbname}.tb (col_ts, c1, c2) values (now-1h, 1, 2)&&(now-2h, 1, 2), (now-2h, 1, 2)')
        tdSql.execute(f'drop database if exists {dbname}')

    def run(self):
        self.batch_insert()

        if self.err_case > 0:
            tdLog.exit(f"{self.err_case} failed")
        else:
            tdLog.success("1 cases passed")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
