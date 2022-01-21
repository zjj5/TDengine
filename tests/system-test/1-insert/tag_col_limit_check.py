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
        case1 <jayden>: [TD-11282] : tag_max_count_check;\n
        case2 <jayden>: [TD-11282] : col_max_count_check;\n
        case3 <jayden>: [TD-11282] : sensitive_check;\n
        case4 <jayden>: [TD-11282] : tag_col_name_length_check;
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

    def col_max_count_check(self):
        '''
            max col count: 4096
        '''
        col_str_exceed = tdCom.gen_tag_col_str("col", "int", 4095)
        col_str = tdCom.gen_tag_col_str("col", "int", 4094)
        dbname = tdCom.get_long_name(len=5, mode="letters")
        tdSql.execute(f'create database if not exists {dbname} precision "ms"')
        tdSql.error(f'create stable if not exists {dbname}.stb (col_ts timestamp, {col_str_exceed}) tags (t1 int)')
        tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, {col_str}) tags (t1 int)')
        col_value_str = '1, ' * 4093 + '1' 
        tdSql.execute(f'create table if not exists {dbname}.tb using {dbname}.stb tags (1)')
        tdSql.execute(f'insert into {dbname}.tb values (now, {col_value_str})')
        res = tdSql.query(f'select col4093 from {dbname}.stb', True)
        tdSql.check_equal(int(res[0][0]), 1)
        tdSql.execute(f'drop database if exists {dbname}')

    def sensitive_check(self):
        '''
            tag_key/col_key sensitive
        '''
        for test_type in ['binary', 'nchar']:
            dbname = tdCom.get_long_name(len=5, mode="letters")
            tdSql.execute(f'create database if not exists {dbname}')
            tdSql.execute(f'create stable if not exists {dbname}.stb (Col_ts timestamp, CC1 {test_type}(16), Cc2 {test_type}(16), `3Cc%3` {test_type}(16)) tags (`1Tag_ts^` timestamp, TT1 {test_type}(16), Tt2 {test_type}(16), `3Tt%3` {test_type}(16))')
            tdSql.execute(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now, "TT1", "Tt2", "3Tt%3")')
            tdSql.execute(f'insert into {dbname}.tb values (now, "TT1", "Tt2", "3Tt%3")')
            col_key_list = tdSql.getColNameList(f'select * from {dbname}.stb', True)[0]
            tdSql.check_equal(col_key_list, ['col_ts', 'cc1', 'cc2', '3Cc%3', '1Tag_ts^', 'tt1', 'tt2', '3Tt%3'])
            res = tdSql.query(f'select * from {dbname}.stb', True)[0]
            lres = list(res)
            lres.pop(0)
            lres.pop(3)
            tdSql.check_equal(lres, ['TT1', 'Tt2', '3Tt%3', 'TT1', 'Tt2', '3Tt%3'])
            tdSql.execute(f'drop database if exists {dbname}')

    def tag_col_name_length_check(self):
        '''
            max tag key length:
        '''
        dbname = tdCom.get_long_name(len=5, mode="letters")
        tag_key_name = tdCom.get_long_name(len=64, mode="letters")
        col_key_name = tdCom.get_long_name(len=64, mode="letters")
        tdSql.execute(f'create database if not exists {dbname}')
        tdSql.error(f'create stable if not exists {dbname}.stb_error (col_ts timestamp, {col_key_name}a int) tags ({tag_key_name} int)')
        tdSql.error(f'create stable if not exists {dbname}.stb_error (col_ts timestamp, {col_key_name} int) tags ({tag_key_name}a int)')
        tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, {col_key_name} int) tags ({tag_key_name} int)')
        tdSql.execute(f'create table if not exists {dbname}.tb using {dbname}.stb tags (1)')
        tdSql.execute(f'insert into {dbname}.tb values (now, 1)')
        col_key_list = tdSql.getColNameList(f'select * from {dbname}.stb', True)[0]
        tdSql.check_equal(col_key_list, ['col_ts', col_key_name, tag_key_name])

    def run(self):
        self.tag_max_count_check()
        self.col_max_count_check()
        self.sensitive_check()
        self.tag_col_name_length_check()
        
        if self.err_case > 0:
            tdLog.exit(f"{self.err_case} failed")
        else:
            tdLog.success("4 cases passed")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
