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
        case1 <jayden>: [TD-11282] : int_illegal_type_check;\n
        case2 <jayden>: [TD-11282] : float_illegal_type_check;\n
        case3 <jayden>: [TD-11282] : bool_illegal_type_check;\n
        case4 <jayden>: [TD-11282] : binary_illegal_type_check;
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        tdSql.execute('reset query cache')

    def int_illegal_type_check(self):
        '''
            int_type_list = ['tinyint', 'smallint', 'int', 'bigint', 'tinyint unsigned', 'smallint unsigned', 'int unsigned', 'bigint unsigned']
            error_type_list = ['contain letters', 'contain illegal symbols', 'contain spaces', 'bool values']
        '''
        int_type_list = ['tinyint', 'smallint', 'int', 'bigint', 'tinyint unsigned', 'smallint unsigned', 'int unsigned', 'bigint unsigned']
        # TODO confirm bool values: bool in tag: create ok and trans to 1, bool in insert: insert error
        error_type_list = ['a10', '1b0', '10c', '%10', '1$0', '10*', '1 0']
        for test_type in int_type_list:
            dbname = tdCom.get_long_name(len=5, mode="letters")
            tdSql.execute(f'create database if not exists {dbname} precision "ms"')
            tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 {test_type}) tags (tag_ts timestamp, t1 {test_type})')
            tdSql.execute(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now, 1)')
            for error_type in error_type_list:
                tdSql.error(f'create table if not exists {dbname}.tb_error using {dbname}.stb tags (now, {error_type})')
            tdSql.error(f'insert into {dbname}.tb values (now, {error_type})')
            tdSql.execute(f'drop database if exists {dbname}')

    def float_illegal_type_check(self):
        '''
            float_type_list = ['float', 'double']
            error_type_list = ['contain letters', 'contain illegal symbols', 'contain spaces', 'bool values']
        '''
        float_type_list = ['float', 'double']
        # TODO confirm bool values: bool in tag: create ok and trans to 1, bool in insert: insert error
        error_type_list = ['a1.1', '1b.1', '1.1c', '%1.1', '1$.1', '1.1*', '1 .0', '1. 0']
        for test_type in float_type_list:
            dbname = tdCom.get_long_name(len=5, mode="letters")
            tdSql.execute(f'create database if not exists {dbname} precision "ms"')
            tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 {test_type}) tags (tag_ts timestamp, t1 {test_type})')
            tdSql.execute(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now, 1.1)')
            for error_type in error_type_list:
                tdSql.error(f'create table if not exists {dbname}.tb_error using {dbname}.stb tags (now, {error_type})')
            tdSql.error(f'insert into {dbname}.tb values (now, {error_type})')
            tdSql.execute(f'drop database if exists {dbname}')

    def bool_illegal_type_check(self):
        '''
            bool_type_list = ['true', 'false']
            error_type_list = ['contain letters', 'contain digit', 'contain illegal symbols', 'contain spaces']
        '''
        bool_type_list = ['bool']
        error_type_list = ['aTrue', 'Fablse', 'Falsec', '1True', 'Fa2lse', 'False3', '*True', 'Fa.lse', 'False%', 'Tru e']
        for test_type in bool_type_list:
            dbname = tdCom.get_long_name(len=5, mode="letters")
            tdSql.execute(f'create database if not exists {dbname} precision "ms"')
            tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 {test_type}) tags (tag_ts timestamp, t1 {test_type})')
            tdSql.execute(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now, True)')
            for error_type in error_type_list:
                tdSql.error(f'create table if not exists {dbname}.tb_error using {dbname}.stb tags (now, {error_type})')
            tdSql.error(f'insert into {dbname}.tb values (now, {error_type})')
            tdSql.execute(f'drop database if exists {dbname}')

    def binary_illegal_type_check(self):
        '''
            binary_type_list = ['binary', 'nchar']
            error_type_list = ['contain illegal symbols', 'contain spaces']
        '''
        binary_type_list = ['binary(16)', 'nchar(16)']
        # TODO confirm bool values: bool in tag: create ok and trans to 1, bool in insert: insert error
        error_type_list = ['%hh', 'h$h', 'hh*', 'h h']
        for test_type in binary_type_list:
            dbname = tdCom.get_long_name(len=5, mode="letters")
            tdSql.execute(f'create database if not exists {dbname} precision "ms"')
            tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 {test_type}) tags (tag_ts timestamp, t1 {test_type})')
            tdSql.execute(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now, 1.1)')
            for error_type in error_type_list:
                tdSql.error(f'create table if not exists {dbname}.tb_error using {dbname}.stb tags (now, {error_type})')
            tdSql.error(f'insert into {dbname}.tb values (now, {error_type})')
            tdSql.execute(f'drop database if exists {dbname}')

    

    def run(self):
        self.int_illegal_type_check()
        self.float_illegal_type_check()
        self.bool_illegal_type_check()
        self.binary_illegal_type_check()

        if self.err_case > 0:
            tdLog.exit(f"{self.err_case} failed")
        else:
            tdLog.success("4 cases passed")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
