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
        case1 <jayden>: [TD-11282] : tbname length check (max 192);\n
        case2 <jayden>: [TD-11282] : backquote supported;\n
        case3 <jayden>: [TD-11282] : error occured when illegal tbname without backquote;\n
        case4 <jayden>: [TD-11282] : upper lower tbname check
        case5 <jayden>: [TD-11282] : illegal tbsql check
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        tdSql.prepare()

    def tbname_length_check(self):
        '''
            max length: 192
        '''
        tbname = tdCom.get_long_name(len=192, mode="letters")
        tdSql.execute(f'create table if not exists {tbname} (ts timestamp, c1 int)')
        tdSql.error(f'create table {tbname} (ts timestamp, c1 int)')
        res = tdSql.query('show tables', True)
        tdSql.check_equal(res[0][0], tbname)
        dbname_exceed = tdCom.get_long_name(len=193, mode="letters")
        tdSql.error(f'create table if not exists {dbname_exceed} (ts timestamp, c1 int)')

    def tbname_with_backquote(self):
        '''
            backquote supported
        '''
        tdCom.cleanTb()
        tbname = '1' + tdCom.get_long_name(len=5, mode="letters")
        tdSql.execute(f'create table if not exists `{tbname}` (ts timestamp, c1 int)')
        res = tdSql.query('show tables', True)
        tdSql.check_equal(res[0][0], tbname)
        tdSql.execute(f'drop table if exists `{tbname}`')
        tbname = tdCom.get_long_name(len=3, mode="letters")
        symbol_list = tdCom.gen_symbol_list()
        symbol_list.remove('`')
        for insert_str in symbol_list:
            d_list = list(tbname)
            for i in range(len(d_list)+1):
                d_list_new = copy.deepcopy(d_list)
                d_list_new.insert(i, insert_str)
                tbname_new = ''.join(d_list_new)
                tdSql.execute(f'create table if not exists `{tbname_new}` (ts timestamp, c1 int)')
                res = tdSql.query('show tables', True)
                tdSql.check_equal(res[0][0], tbname_new)
                tdSql.execute(f'drop table if exists `{tbname_new}`')

    def tbname_without_backquote(self):
        '''
            error occured when illegal tbname without backquote
        '''
        tdCom.cleanTb()
        tbname = '1' + tdCom.get_long_name(len=5, mode="letters")
        tdSql.error(f'create table if not exists {tbname} (ts timestamp, c1 int)')
        tbname = tdCom.get_long_name(len=3, mode="letters")
        symbol_list = tdCom.gen_symbol_list()
        symbol_list.remove(' ')
        for insert_str in symbol_list:
            d_list = list(tbname)
            for i in range(len(d_list)+1):
                d_list_new = copy.deepcopy(d_list)
                d_list_new.insert(i, insert_str)
                tbname_new = ''.join(d_list_new)
                tdSql.error(f'create table if not exists {tbname_new} (ts timestamp, c1 int)')

    def upper_lower_tbname_check(self):
        '''
            without backquote: case insensitive
            with backquote: keep upper or mixed
        '''
        for tbname in [tdCom.get_long_name(len=5, mode="letters_mixed"), tdCom.get_long_name(len=5, mode="letters_mixed").upper()]:
            tdSql.execute(f'create table if not exists {tbname} (ts timestamp, c1 int)')
            res = tdSql.query('show tables', True)
            tdSql.check_equal(res[0][0], tbname.lower())
            tdSql.execute(f'drop table if exists `{tbname.lower()}`')
        
        for tbname in [tdCom.get_long_name(len=5, mode="letters_mixed"), tdCom.get_long_name(len=5, mode="letters_mixed").upper()]:
            tdSql.execute(f'create table if not exists `{tbname}` (ts timestamp, c1 int)')
            res = tdSql.query('show tables', True)
            tdSql.check_equal(res[0][0], tbname)
            tdSql.execute(f'drop table if exists `{tbname}`')

    def illegal_tbsql_check(self):
        '''
            mixed invalid symbol
        '''
        dbname = tdCom.get_long_name(len=5, mode="letters")
        tdSql.execute(f'create database if not exists {dbname}')
        stbname = tdCom.get_long_name(len=3, mode="letters")
        tbname = tdCom.get_long_name(len=2, mode="letters")
        tdSql.execute(f'create stable if not exists {dbname}.{stbname} (col_ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 tinyint unsigned, c6 smallint unsigned, \
                c7 int unsigned, c8 bigint unsigned, c9 float, c10 double, c13 bool) tags (tag_ts timestamp, t1 tinyint, t2 smallint, t3 int, \
                t4 bigint, t5 tinyint unsigned, t6 smallint unsigned, t7 int unsigned, t8 bigint unsigned, t9 float, t10 double, t13 bool)')
        base_sql1 = f'create table if not exists {dbname}.{tbname} using {dbname}.stb tags (now, 1, 2, 3, 4, 5, 6, 7, 8, 9.9, 10.1, True)'
        base_sql2 = f'create table if not exists {dbname}.{tbname} (col_ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 tinyint unsigned, c6 smallint unsigned, c7 int unsigned, c8 bigint unsigned, c9 float, c10 double, c13 bool)'

        symbol_list = tdCom.gen_symbol_list()
        symbol_list.remove(' ')
        symbol_list.remove('+')
        symbol_list.remove(';')
        for insert_str in symbol_list:
            for base_sql in [base_sql1, base_sql2]:
                d_list = list(base_sql)
                for i in range(len(d_list)+1):
                    d_list_new = copy.deepcopy(d_list)
                    d_list_new.insert(i, insert_str)
                    sql_new = ''.join(d_list_new)
                    tdSql.error(sql_new)
        tdSql.execute(f'drop stable if exists `{dbname}`')


    def run(self):
        self.tbname_length_check()
        self.tbname_with_backquote()
        self.tbname_without_backquote()
        self.upper_lower_tbname_check()
        self.illegal_tbsql_check()

        if self.err_case > 0:
            tdLog.exit(f"{self.err_case} failed")
        else:
            tdLog.success("5 cases passed")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
