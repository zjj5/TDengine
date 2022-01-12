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
        case1 <jayden>: [TD-11282] : stb name length check (max 192);\n
        case2 <jayden>: [TD-11282] : backquote supported;\n
        case3 <jayden>: [TD-11282] : error occured when illegal stbname without backquote;\n
        case4 <jayden>: [TD-11282] : upper lower stbname check
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        tdSql.prepare()

    def stbname_length_check(self):
        '''
            max length: 192
        '''
        stbname = tdCom.get_long_name(len=192, mode="letters")
        tdSql.execute(f'create stable if not exists {stbname} (ts timestamp, c1 int) tags (t1 int)')
        res = tdSql.query('show stables', True)
        tdSql.check_equal(res[0][0], stbname)
        dbname_exceed = tdCom.get_long_name(len=193, mode="letters")
        tdSql.error(f'create stable if not exists {dbname_exceed} (ts timestamp, c1 int) tags (t1 int)')

    def stbname_with_backquote(self):
        '''
            backquote supported
        '''
        tdCom.cleanTb()
        stbname = '1' + tdCom.get_long_name(len=5, mode="letters")
        tdSql.execute(f'create stable if not exists `{stbname}` (ts timestamp, c1 int) tags (t1 int)')
        res = tdSql.query('show stables', True)
        tdSql.check_equal(res[0][0], stbname)
        tdSql.execute(f'drop table if exists `{stbname}`')
        stbname = tdCom.get_long_name(len=3, mode="letters")
        symbol_list = tdCom.gen_symbol_list()
        symbol_list.remove('`')
        for insert_str in symbol_list:
            d_list = list(stbname)
            for i in range(len(d_list)+1):
                d_list_new = copy.deepcopy(d_list)
                d_list_new.insert(i, insert_str)
                stbname_new = ''.join(d_list_new)
                tdSql.execute(f'create stable if not exists `{stbname_new}` (ts timestamp, c1 int) tags (t1 int)')
                res = tdSql.query('show stables', True)
                tdSql.check_equal(res[0][0], stbname_new)
                tdSql.execute(f'drop table if exists `{stbname_new}`')

    def stbname_without_backquote(self):
        '''
            error occured when illegal stbname without backquote
        '''
        tdCom.cleanTb()
        stbname = '1' + tdCom.get_long_name(len=5, mode="letters")
        tdSql.error(f'create stable if not exists {stbname} (ts timestamp, c1 int) tags (t1 int)')
        stbname = tdCom.get_long_name(len=3, mode="letters")
        symbol_list = tdCom.gen_symbol_list()
        symbol_list.remove(' ')
        for insert_str in symbol_list:
            d_list = list(stbname)
            for i in range(len(d_list)+1):
                d_list_new = copy.deepcopy(d_list)
                d_list_new.insert(i, insert_str)
                stbname_new = ''.join(d_list_new)
                tdSql.error(f'create stable if not exists {stbname_new} (ts timestamp, c1 int) tags (t1 int)')

    def upper_lower_stbname_check(self):
        '''
            without backquote: case insensitive
            with backquote: keep upper or mixed
        '''
        for stbname in [tdCom.get_long_name(len=5, mode="letters_mixed"), tdCom.get_long_name(len=5, mode="letters_mixed").upper()]:
            tdSql.execute(f'create stable if not exists {stbname} (ts timestamp, c1 int) tags (t1 int)')
            res = tdSql.query('show stables', True)
            tdSql.check_equal(res[0][0], stbname.lower())
            tdSql.execute(f'drop stable if exists `{stbname.lower()}`')
        
        for stbname in [tdCom.get_long_name(len=5, mode="letters_mixed"), tdCom.get_long_name(len=5, mode="letters_mixed").upper()]:
            tdSql.execute(f'create stable if not exists `{stbname}` (ts timestamp, c1 int) tags (t1 int)')
            res = tdSql.query('show stables', True)
            tdSql.check_equal(res[0][0], stbname)
            tdSql.execute(f'drop stable if exists `{stbname}`')

    def run(self):
        self.stbname_length_check()
        self.stbname_with_backquote()
        self.stbname_without_backquote()
        self.upper_lower_stbname_check()

        if self.err_case > 0:
            tdLog.exit(f"{self.err_case} failed")
        else:
            tdLog.success("4 cases passed")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
