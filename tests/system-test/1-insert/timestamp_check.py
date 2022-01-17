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

    def ms_us_ns_db_check(self):
        '''
            precision = ["ms", "us", "ns"]
        '''
        for ts in ["ms", "us", "ns"]:
            dbname = tdCom.get_long_name(len=5, mode="letters")
            tdSql.execute(f'create database if not exists {dbname} precision "{ts}"')
            res = tdSql.query('show databases', True)
            tdSql.check_equal(res[0][16], ts)
            tdSql.execute(f'create table {dbname}.{dbname} (ts timestamp, c1 int)')
            timestamp, dt = tdCom.genTs(ts)
            tdSql.execute(f'insert into {dbname}.{dbname} values ({timestamp}, 1)')
            res = tdSql.query(f'select ts from {dbname}.{dbname}', True)
            tdSql.check_equal(str(res[0][0]), str(dt))
            tdSql.execute(f'drop database if exists {dbname}')

    def h_m_s_check(self):
        '''
            check hh:mm:ss
        '''
        dbname = tdCom.get_long_name(len=5, mode="letters")
        tdSql.execute(f'create database if not exists {dbname} precision "ms"')
        tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 int) tags (tag_ts timestamp, t1 int)')
        tdSql.execute(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now, 1)')
        tdSql.execute(f'insert into {dbname}.tb values ("2022-01-16 21:17:01", 1)')
        res = tdSql.query(f'select * from {dbname}.tb where c1 = 1', True)
        tdSql.check_equal(str(res[0][0]), "2022-01-16 21:17:01")
        tdSql.execute(f'insert into {dbname}.tb values ("2022-01-16 21:17:61", 2)')
        res = tdSql.query(f'select * from {dbname}.tb where c1 = 2', True)
        tdSql.check_equal(str(res[0][0]), "2022-01-16 21:18:01")
        tdSql.execute(f'insert into {dbname}.tb values ("2022-01-16 21:17:121", 3)')
        res = tdSql.query(f'select * from {dbname}.tb where c1 = 3', True)
        tdSql.check_equal(str(res[0][0]), "2022-01-16 21:17:12")
        # TODO confirm 
        tdSql.error(f'insert into {dbname}.tb values ("2022-01-16 21:17:62", 2)')
        tdSql.execute(f'drop database if exists {dbname}')

    # ! bug
    def human_date_check(self):
        '''
            human date check
        '''
        for ts in ["ms"]:
            dbname = tdCom.get_long_name(len=5, mode="letters")
            dbname = dbname + '_' + ts
            timestamp = tdCom.genTs(ts)[1]
            tdSql.execute(f'create database if not exists {dbname} precision "{ts}"')
            tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 int) tags (tag_ts timestamp, t1 int)')
            tdSql.execute(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now, 1)')
            for ts_unit in tdCom.gen_ts_support_unit_list():
                if ts_unit == "b" or ts_unit == "u" or ts_unit == "a":
                    step = 10000000
                else:
                    step = 1
                # tdSql.execute(f'create table if not exists {dbname}.{ts_unit}{step}_add using {dbname}.stb tags ("{timestamp}+1000{ts_unit}", 1)')
                # tdSql.execute(f'create table if not exists {dbname}.{ts_unit}{step}_sub using {dbname}.stb tags ("{timestamp}-1{ts_unit}", 1)')
                tdSql.execute(f'insert into {dbname}.tb values ("{timestamp}+1{ts_unit}", 1)')
                tdSql.execute(f'insert into {dbname}.tb values ("{timestamp}-1{ts_unit}", 1)')
            res = tdSql.query(f'select count(*) from {dbname}.tb', True)
            tdSql.check_equal(res[0][0], 16)
            res = tdSql.query(f'show {dbname}.stables', True)
            tdSql.check_equal(res[0][4], 17)
            tdSql.execute(f'drop database if exists {dbname}')
        
        
    def now_check(self):
        '''
            now check
        '''
        for ts in ["ms", "us", "ns"]:
            dbname = tdCom.get_long_name(len=5, mode="letters")
            dbname = dbname + '_' + ts
            tdSql.execute(f'create database if not exists {dbname} precision "{ts}"')
            tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 int) tags (tag_ts timestamp, t1 int)')
            tdSql.execute(f'create table if not exists {dbname}.tb using {dbname}.stb tags (now, 1)')
            for ts_unit in tdCom.gen_ts_support_unit_list():
                if ts_unit == "b" or ts_unit == "u" or ts_unit == "a":
                    step = 10000000
                else:
                    step = 1
                tdSql.execute(f'create table if not exists {dbname}.{ts_unit}{step}_add using {dbname}.stb tags (now+{step}{ts_unit}, 1)')
                tdSql.execute(f'create table if not exists {dbname}.{ts_unit}{step}_sub using {dbname}.stb tags (now-{step}{ts_unit}, 1)')
                tdSql.execute(f'insert into {dbname}.tb values (now+{step}{ts_unit}, 1)')
                tdSql.execute(f'insert into {dbname}.tb values (now-{step}{ts_unit}, 1)')
            res = tdSql.query(f'select count(*) from {dbname}.tb', True)
            tdSql.check_equal(res[0][0], 16)
            res = tdSql.query(f'show {dbname}.stables', True)
            tdSql.check_equal(res[0][4], 17)
            tdSql.execute(f'drop database if exists {dbname}')

    def epoch_check(self):
        '''
            epoch check
        '''
        for ts in ["ms", "us", "ns"]:
            dbname = tdCom.get_long_name(len=5, mode="letters")
            dbname = dbname + '_' + ts
            timestamp = tdCom.genTs(ts)[0]
            tdSql.execute(f'create database if not exists {dbname} precision "{ts}"')
            tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 int) tags (tag_ts timestamp, t1 int)')
            tdSql.execute(f'create table if not exists {dbname}.tb using {dbname}.stb tags ({timestamp}, 1)')
            for ts_unit in tdCom.gen_ts_support_unit_list():
                if ts_unit == "b" or ts_unit == "u" or ts_unit == "a":
                    step = 10000000
                else:
                    step = 1
                tdSql.error(f'create table if not exists {dbname}.tb_error using {dbname}.stb tags ({timestamp}+1{ts_unit}, 1)')
                tdSql.error(f'create table if not exists {dbname}.tb_error using {dbname}.stb tags ({timestamp}-1{ts_unit}, 1)')
                tdSql.execute(f'insert into {dbname}.tb values ({timestamp}+{step}{ts_unit}, 1)')
                tdSql.execute(f'insert into {dbname}.tb values ({timestamp}-{step}{ts_unit}, 1)')
            res = tdSql.query(f'select count(*) from {dbname}.tb', True)
            tdSql.check_equal(res[0][0], 16)
            tdSql.execute(f'drop database if exists {dbname}')

    def error_check(self):
        '''
            ts error check
        '''
        # inconsistent precision
        pricision_list = ["ms", "us", "ns"]
        for ts in pricision_list:
            dbname = tdCom.get_long_name(len=5, mode="letters")
            dbname = dbname + '_' + ts
            timestamp = tdCom.genTs(ts)[0]
            tdSql.execute(f'create database if not exists {dbname} precision "{ts}"')
            tdSql.execute(f'create stable if not exists {dbname}.stb (col_ts timestamp, c1 int) tags (tag_ts timestamp, t1 int)')
            tdSql.execute(f'create table if not exists {dbname}.tb using {dbname}.stb tags ({timestamp}, 1)')
            pricision_list_tmp = copy.deepcopy(pricision_list)
            pricision_list_tmp.remove(ts)
            for illegal_ts in pricision_list_tmp:
                # TODO confirm
                # tdSql.error(f'create table if not exists {dbname}.tb1 using {dbname}.stb tags ({tdCom.genTs(illegal_ts)[0]}, 1)')
                tdSql.error(f'insert into {dbname}.tb values ({tdCom.genTs(illegal_ts)[0]}, 1)')

            # * The second level can exceed 60
            for error_sql in [
                f'insert into {dbname}.tb values ("2022-01-143 00:05:55", 1)',
                f'insert into {dbname}.tb values ("2022-01-14# 00:05:55", 1)',
                f'insert into {dbname}.tb values ("2022-01-14 00:05:55.*_*", 1)',
                f'insert into {dbname}.tb values ("2022-01-14 0 0:05:55", 1)',
                f'insert into {dbname}.tb values ("2022-01-1 4 00:05:55", 1)',
                f'insert into {dbname}.tb values ("9999-01-14 00:05:55", 1)',
                f'insert into {dbname}.tb values ("2022-00-14 00:05:55", 1)',
                f'insert into {dbname}.tb values ("2022-13-14 00:05:55", 1)',
                f'insert into {dbname}.tb values ("2022-01-00 00:05:55", 1)',
                f'insert into {dbname}.tb values ("2022-01-32 00:05:55", 1)',
                f'insert into {dbname}.tb values ("2022-02-31 00:05:55", 1)',
                f'insert into {dbname}.tb values ("2022-04-31 00:05:55", 1)',
                f'insert into {dbname}.tb values ("2022-01-14 25:05:55", 1)',
                f'insert into {dbname}.tb values ("2022-01-14 00:61:55", 1)',
                f'insert into {dbname}.tb values (now + 1n, 1)',
                f'insert into {dbname}.tb values (now - 1n, 1)',
                f'insert into {dbname}.tb values (now + 1y, 1)',
                f'insert into {dbname}.tb values (now - 1y, 1)'
                ]:
                tdSql.error(error_sql)

    def run(self):
        self.ms_us_ns_db_check()
        self.h_m_s_check()
        #! bug
        # self.human_date_check()
        self.now_check()
        self.epoch_check()
        self.error_check()
        

        if self.err_case > 0:
            tdLog.exit(f"{self.err_case} failed")
        else:
            tdLog.success("5 cases passed")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
