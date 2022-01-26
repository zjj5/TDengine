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
        case1 <jayden>: [TD-11282] : specified column insert;\n
        case2 <jayden>: [TD-11282] : different update value of specified column;\n

        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        tdSql.execute('reset query cache')

    def specified_column_insert(self):
        '''
            specified_column_insert 
        '''
        dbname = tdCom.get_long_name(len=5, mode="letters")
        tdSql.execute(f'create database if not exists {dbname}')
        tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 tinyint unsigned) tags \
            (tag_ts timestamp, t1 tinyint, t2 smallint, t3 int, t4 bigint, t5 tinyint unsigned)')
        tdSql.execute(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now, 1, 2, 3, 4, 5)')
        tdSql.error(f'create table if not exists {dbname}.tb_error using {dbname}.stb tags (now, 1, 2, 3, 4, 5, 6)')
        tdSql.execute(f'insert into {dbname}.tb values (now, 1, 2, 3, 4, 5)')
        tdSql.execute(f'insert into {dbname}.tb (col_ts, c1, c2, c3, c4, c5) values (now+1h, 1, 2, 3, 4, 5)')
        tdSql.execute(f'insert into {dbname}.tb (col_ts, c1, c2) values (now+2h, 1, 2)')
        tdSql.error(f'insert into {dbname}.tb (col_ts, c1, c2, c3, c4, c5, c6) values (now, 1, 2, 3, 4, 5, 6)')
        tdSql.error(f'insert into {dbname}.tb (col_ts, c1, c6) values (now, 1, 6)')
        res = tdSql.query(f'select count(*) from {dbname}.stb', True)
        tdSql.check_equal(int(res[0][0]), 3)
        tdSql.execute(f'drop database if exists {dbname}')

    def dif_update_specified_column(self):
        '''
            update = 0, 1, 2
        '''
        for update in [0, 1, 2]:
            dbname = tdCom.get_long_name(len=5, mode="letters")
            ts = tdCom.genTs()[0]
            tdSql.execute(f'create database if not exists {dbname} update {update}')
            tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 tinyint unsigned) tags \
                (tag_ts timestamp, t1 tinyint, t2 smallint, t3 int, t4 bigint, t5 tinyint unsigned)')
            tdSql.execute(f'create table if not exists {dbname}.tb using {dbname}.stb tags ({ts}, 1, 2, 3, 4, 5)')
            tdSql.execute(f'insert into {dbname}.tb values ({ts}, 1, 2, 3, 4, 5)')
            tdSql.execute(f'insert into {dbname}.tb (col_ts, c1, c2) values ({ts}, 1, null)')
            res = tdSql.query(f'select c1, c2, c3, c4, c5 from {dbname}.stb', True)
            if update == 0:
                tdSql.check_equal(res, [(1, 2, 3, 4, 5)])
            if update == 1:
                tdSql.check_equal(res, [(1, None, None, None, None)])
            if update == 2:
                tdSql.check_equal(res, [(1, 2, 3, 4, 5)])
            tdSql.execute(f'drop database if exists {dbname}')

    def run(self):
        self.specified_column_insert()
        self.dif_update_specified_column()

        if self.err_case > 0:
            tdLog.exit(f"{self.err_case} failed")
        else:
            tdLog.success("2 cases passed")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
