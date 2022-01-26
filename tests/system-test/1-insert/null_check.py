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
        case1 <jayden>: [TD-11282] : null dbname check;\n
        case2 <jayden>: [TD-11282] : stb null check;\n
        case3 <jayden>: [TD-11282] : tb null check;\n
        case4 <jayden>: [TD-11282] : polling insert check;

        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        tdSql.execute('reset query cache')

    def null_dbname_check(self):
        '''
            dbname = "null"
        '''
        dbname = "null"
        tdSql.error(f'create database if not exists {dbname}')

    def stb_null_check(self):
        '''
            stbname/tag/col = "null"
        '''
        dbname = tdCom.get_long_name(len=5, mode="letters")
        tdSql.execute(f'create database if not exists {dbname} precision "ms"')
        stbname = "null"
        tdSql.error(f'create stable if not exists {dbname}.{stbname} (col_ts timestamp, c1 int) tags (tag_ts timestamp, t1 int)')
        tdSql.error(f'create stable if not exists {dbname}.stb (null timestamp, c1 int) tags (tag_ts timestamp, t1 int)')
        tdSql.error(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 int) tags (tag_ts timestamp, null int)')
        tdSql.error(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 null) tags (tag_ts timestamp, t1 int)')
        tdSql.error(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 int) tags (tag_ts timestamp, t1 null)')
        tdSql.execute(f'drop database if exists {dbname}')
    
    def tb_null_check(self):
        '''
            tbname/tag/col = "null"
        '''
        dbname = tdCom.get_long_name(len=5, mode="letters")
        tdSql.execute(f'create database if not exists {dbname} precision "ms"')
        tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 int) tags (tag_ts timestamp, t1 int)')
        tbname = "null"
        tdSql.error(f'create table if not exists {dbname}.{tbname} using {dbname}.stb tags (now, 1)')
        tdSql.execute(f'create table if not exists {dbname}.tb using {dbname}.stb tags (null, null)')
        tdSql.execute(f'insert into {dbname}.tb values (now, null)')
        tdSql.error(f'insert into {dbname}.tb values (null, 1)')
        res = tdSql.query(f'select tag_ts, t1, c1 from {dbname}.stb', True)
        tdSql.check_equal(res[0][0], None)
        tdSql.check_equal(res[0][1], None)
        tdSql.check_equal(res[0][2], None)
        tdSql.execute(f'drop database if exists {dbname}')
    
    def polling_insert_check(self):
        '''
            null and normal poll insert 
        '''
        dbname = tdCom.get_long_name(len=5, mode="letters")
        tdSql.execute(f'create database if not exists {dbname}')
        tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 tinyint unsigned, c6 smallint unsigned, \
                c7 int unsigned, c8 bigint unsigned, c9 float, c10 double, c11 binary(16), c12 nchar(16), c13 bool) tags (tag_ts timestamp, t1 tinyint, t2 smallint, t3 int, \
                t4 bigint, t5 tinyint unsigned, t6 smallint unsigned, t7 int unsigned, t8 bigint unsigned, t9 float, t10 double, t11 binary(16), t12 nchar(16), t13 bool)')
        tdSql.execute(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now, 1, 2, 3, 4, 5, 6, 7, 8, 9.9, 10.1, "binary", "nchar", True)')
        tdSql.execute(f'create table if not exists {dbname}.tb_null using {dbname}.stb tags (now, null, null, null, null, null, null, null, null, null, null, null, null, null)')
        tdSql.execute(f'insert into {dbname}.tb values (now, 1, 2, 3, 4, 5, 6, 7, 8, 9.9, 10.1, "binary", "nchar", True)')
        tdSql.execute(f'insert into {dbname}.tb values (now, null, null, null, null, null, null, null, null, null, null, null, null, null)')
        tdSql.execute(f'insert into {dbname}.tb values (now-1h, 1, 2, 3, 4, 5, 6, 7, 8, 9.9, 10.1, "binary", "nchar", True)')
        tdSql.execute(f'insert into {dbname}.tb values (now-2h, null, null, null, null, null, null, null, null, null, null, null, null, null)')
        tdSql.execute(f'insert into {dbname}.tb (col_ts, c3 , c7, c9, c11, c13) values (now+1h, 3, 7, 9.9, "binary", True)')
        tdSql.execute(f'insert into {dbname}.tb (col_ts, c3 , c7, c9, c11, c13) values (now+2h, null, null, null, null, null)')
        tdSql.execute(f'insert into {dbname}.tb (col_ts, c3 , c7, c9, c11, c13) values (now+3h, 3, null, 7, null, False)')
        res = tdSql.query(f'select count(*) from {dbname}.stb', True)
        tdSql.check_equal(int(res[0][0]), 7)
        tdSql.execute(f'drop database if exists {dbname}')

    def run(self):
        self.null_dbname_check()
        self.stb_null_check()
        self.tb_null_check()
        self.polling_insert_check()
        if self.err_case > 0:
            tdLog.exit(f"{self.err_case} failed")
        else:
            tdLog.success("4 cases passed")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
