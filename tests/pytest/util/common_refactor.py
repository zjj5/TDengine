###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import random
import string
from util.sql import tdSql
from util.dnodes import tdDnodes
from util.log import *
import inspect
import math
import requests
import json
import time
from datetime import datetime


class TDCom:
    def init(self, conn, logSql):
        tdSql.init(conn.cursor(), logSql)

    def __init__(self):
        self.a = 1

    def preDefine(self):
        header = {'Authorization': 'Basic cm9vdDp0YW9zZGF0YQ=='}
        sql_url = "http://127.0.0.1:6041/rest/sql"
        sqlt_url = "http://127.0.0.1:6041/rest/sqlt"
        sqlutc_url = "http://127.0.0.1:6041/rest/sqlutc"
        influx_url = "http://127.0.0.1:6041/influxdb/v1/write"
        telnet_url = "http://127.0.0.1:6041/opentsdb/v1/put/telnet"
        return header, sql_url, sqlt_url, sqlutc_url, influx_url, telnet_url

    def restApiPost(self, sql):
        res = requests.post(self.preDefine()[1], sql.encode("utf-8"), headers=self.preDefine()[0])
        return res.json(), res.status_code

    def genRandomSchema(self):
        schema_type_list = ["TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE","BINARY", "NCHAR", "BOOL", 
                            "TINYINT UNSIGNED", "SMALLINT UNSIGNED", "INT UNSIGNED", "BIGINT UNSIGNED"]
        random.shuffle(schema_type_list)
        return schema_type_list

    def gen_full_type_schema(self):
        full_type_list = ["TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE","BINARY", "NCHAR", "BOOL", 
                            "TINYINT UNSIGNED", "SMALLINT UNSIGNED", "INT UNSIGNED", "BIGINT UNSIGNED"]
        return full_type_list

    def genTs(self, precision="ms", ts=""):
        if precision == "ns":
            if ts == "" or ts is None:
                ts = time.time_ns()
            else:
                ts = ts
            # dt = datetime.fromtimestamp(ts // 1000000000)
            # dt = dt.strftime('%Y-%m-%d %H:%M:%S') + '.' + str(int(ts % 1000000000)).zfill(9)
            dt = ts
        else:
            if ts == "" or ts is None:
                ts = time.time()
            else:
                ts = ts
            if precision == "ms" or precision == None:
                ts = int(round(ts * 1000))
                dt = datetime.fromtimestamp(ts // 1000)
                dt = dt.strftime('%Y-%m-%d %H:%M:%S') + '.' + str(int(ts % 1000)).zfill(3) + '000'
            elif precision == "us":
                ts = int(round(ts * 1000000))
                dt = datetime.fromtimestamp(ts // 1000000)
                dt = dt.strftime('%Y-%m-%d %H:%M:%S') + '.' + str(int(ts % 1000000)).zfill(6)
        return ts, dt

    def gen_tag_col_str(self, gen_type, data_type, count):
        tag_col_str = ''
        for i in range(count):
            if i != count-1:
                tag_col_str += f'{gen_type}{i} {data_type},'
            else:
                tag_col_str += f'{gen_type}{i} {data_type}'
        return tag_col_str

    def genTbParams(self, precision, ts, ctinyint_value, csmallint_value, cint_value, cbigint_value, cfloat_value, cdouble_value, 
                    cbinary_value, cnchar_value, cbool_value, cutinyint_value, cusmallint_value, cuint_value, cubigint_value):
        schema_type_list = self.genRandomSchema()
        ts, dt = self.genTs(precision, ts)
        td_col_type_list = []
        input_col_key_list = list()
        input_col_key_list.append("ts")
        output_col_value_list = list()
        td_col_type_list.append(9)
        output_col_value_list.append(dt)
        for col_type in schema_type_list:
            if col_type == "TINYINT":
                output_col_value_list.append(ctinyint_value)
                td_col_type_list.append(2)
            elif col_type == "SMALLINT":
                output_col_value_list.append(csmallint_value)
                td_col_type_list.append(3)
            elif col_type == "INT":
                output_col_value_list.append(cint_value)
                td_col_type_list.append(4)
            elif col_type == "BIGINT":
                output_col_value_list.append(cbigint_value)
                td_col_type_list.append(5)
            elif col_type == "FLOAT":
                output_col_value_list.append(cfloat_value)
                td_col_type_list.append(6)
            elif col_type == "DOUBLE":
                output_col_value_list.append(cdouble_value)
                td_col_type_list.append(7)
            elif col_type == "BINARY":
                output_col_value_list.append(cbinary_value)
                td_col_type_list.append(8)
            elif col_type == "NCHAR":
                output_col_value_list.append(cnchar_value)
                td_col_type_list.append(10)
            elif col_type == "BOOL":
                output_col_value_list.append(cbool_value)
                td_col_type_list.append(1)
            elif col_type == "TINYINT UNSIGNED":
                output_col_value_list.append(cutinyint_value)
                td_col_type_list.append(11)
            elif col_type == "SMALLINT UNSIGNED":
                output_col_value_list.append(cusmallint_value)
                td_col_type_list.append(12)
            elif col_type == "INT UNSIGNED":
                output_col_value_list.append(cuint_value)
                td_col_type_list.append(13)
            elif col_type == "BIGINT UNSIGNED":
                output_col_value_list.append(cubigint_value)
                td_col_type_list.append(14)
            input_col_key_list.append(col_type)
        return input_col_key_list, td_col_type_list, output_col_value_list

    def genTbSql(self, tbname=None, precision="ms", ts="", ctinyint_value=127, csmallint_value=32767, cint_value=pow(2, 31)-1, cbigint_value=pow(2, 63)-1, 
                cfloat_value=3.4*pow(10, 38), cdouble_value=1.7*pow(10, 308), cbinary_value="binary_value", cnchar_value="nchar_value",
                cbool_value=True, cutinyint_value=254, cusmallint_value=65534, cuint_value=4294967294, cubigint_value=18446744073709551614):
        if tbname is None:
            tbname = self.getLongName(10)
        input_col_key_list, td_col_type_list, output_col_value_list = self.genTbParams(precision, ts, ctinyint_value, csmallint_value, cint_value, cbigint_value, cfloat_value, cdouble_value, 
                    cbinary_value, cnchar_value, cbool_value, cutinyint_value, cusmallint_value, cuint_value, cubigint_value)
        schema = ""
        col_key_list = list()
        for key in input_col_key_list:
            if " " in key:
                if key != input_col_key_list[-1]:
                    _key = key.replace(" ", "_")
                    col_key_list.append(_key)
                    schema += f'c_{_key} {key} '
                else:
                    _key = key.replace(" ", "_")
                    col_key_list.append(_key)
                    schema += f'c_{_key} {key}'
            else:
                if key != input_col_key_list[-1]:
                    col_key_list.append(key)
                    schema += f'c_{key} {key} '
                else:
                    col_key_list.append(key)
                    schema += f'c_{key} {key}'
        
        input_sql = f'create table {tbname} ({schema})'
        return input_sql, col_key_list, output_col_value_list



    def taoscCreateDb(self, dbname="taosc_db", **kwargs):
        tdSql.execute(f"drop database if exists {dbname}")
        create_sql = f"create database if not exists {dbname}"
        for key, value in kwargs.items():
            create_sql += f' {key} {value}'
        tdSql.execute(create_sql)
        tdSql.execute(f'use {dbname}')

    def restfulCreateDb(self, dbname="restful_db", **kwargs):
        self.restApiPost(f"drop database if exists {dbname}")
        create_sql = f"create database if not exists {dbname}"
        for key, value in kwargs.items():
            create_sql += f' {key} {value}'
        res = self.restApiPost(create_sql)
        print(self.a)
        self.checkEqual(create_sql, res[1], 200)

    def taoscCreateStb(self):
        stb_name = self.getLongName(10)
        # create_db
        # create_table
        # insert_data

        # check table schema
        # check data



    def createDb(self, dbname="test", db_update_tag=0, api_type="taosc", precision="ms"):
        if api_type == "taosc":
            if db_update_tag == 0:
                tdSql.execute(f"drop database if exists {dbname}")
                tdSql.execute(
                    f"create database if not exists {dbname} precision '{precision}'")
            else:
                tdSql.execute(f"drop database if exists {dbname}")
                tdSql.execute(
                    f"create database if not exists {dbname} precision '{precision}' update 1")
        elif api_type == "restful":
            if db_update_tag == 0:
                self.restApiPost(f"drop database if exists {dbname}")
                self.restApiPost(
                    f"create database if not exists {dbname} precision '{precision}'")
            else:
                self.restApiPost(f"drop database if exists {dbname}")
                self.restApiPost(
                    f"create database if not exists {dbname} precision '{precision}' update 1")
        tdSql.execute(f'use {dbname}')

    def genUrl(self, url_type, dbname, precision):
        if url_type == "influxdb":
            if precision is None:
                url = self.preDefine()[4] + "?" + "db=" + dbname
            else:
                url = self.preDefine()[4] + "?" + "db=" + \
                    dbname + "&precision=" + precision
        elif url_type == "telnet":
            url = self.preDefine()[5] + "/" + dbname
        else:
            url = self.preDefine()[1]
        return url

    def gen_symbol_list(self):
        return [' ', '~', '`', '!', '@', '#', '$', '¥', '%', '^', '&', '*', '(', ')', 
                '-', '+', '=', '{', '「', '[', ']', '}', '」', '、', '|', '\\', ':', 
                ';', '\'', '\"', ',', '<', '《', '.', '>', '》', '/', '?']

    def gen_ts_support_unit_list(self):
        return ["b", "u", "a", "s", "m", "h", "d", "w"]

    def schemalessApiPost(self, sql, url_type="influxdb", dbname="test", precision=None):
        if url_type == "influxdb":
            url = self.genUrl(url_type, dbname, precision)
        elif url_type == "telnet":
            url = self.genUrl(url_type, dbname, precision)
        res = requests.post(url, sql.encode("utf-8"),
                            headers=self.preDefine()[0])
        return res

    def cleanTb(self, type="taosc"):
        '''
            type is taosc or restful
        '''
        for query_sql in ['show stables', 'show tables']:
            res_row_list = tdSql.query(query_sql, True)
            stb_list = map(lambda x: x[0], res_row_list)
            for stb in stb_list:
                if type == "taosc":
                    tdSql.execute(f'drop table if exists {stb}')
                    tdSql.execute(f'drop table if exists `{stb}`')
                elif type == "restful":
                    self.restApiPost(f"drop table if exists {stb}")
                    self.restApiPost(f"drop table if exists `{stb}`")

    def dateToTs(self, datetime_input):
        return int(time.mktime(time.strptime(datetime_input, "%Y-%m-%d %H:%M:%S.%f")))

    def get_long_name(self, len, mode="mixed"):
        """
            generate long name
            mode could be numbers/letters/letters_mixed/mixed
        """
        if mode == "numbers":
            chars = ''.join(random.choice(string.digits) for i in range(len))
        elif mode == "letters":
            chars = ''.join(random.choice(string.ascii_letters.lower())
                            for i in range(len))
        elif mode == "letters_mixed":
            chars = ''.join(random.choice(string.ascii_letters.upper(
            ) + string.ascii_letters.lower()) for i in range(len))
        else:
            chars = ''.join(random.choice(
                string.ascii_letters.lower() + string.digits) for i in range(len))
        return chars

    def restartTaosd(self, index=1, db_name="db"):
        tdDnodes.stop(index)
        tdDnodes.startWithoutSleep(index)
        tdSql.execute(f"use {db_name}")

    def typeof(self, variate):
        v_type = None
        if type(variate) is int:
            v_type = "int"
        elif type(variate) is str:
            v_type = "str"
        elif type(variate) is float:
            v_type = "float"
        elif type(variate) is bool:
            v_type = "bool"
        elif type(variate) is list:
            v_type = "list"
        elif type(variate) is tuple:
            v_type = "tuple"
        elif type(variate) is dict:
            v_type = "dict"
        elif type(variate) is set:
            v_type = "set"
        return v_type

    def splitNumLetter(self, input_mix_str):
        nums, letters = "", ""
        for i in input_mix_str:
            if i.isdigit():
                nums += i
            elif i.isspace():
                pass
            else:
                letters += i
        return nums, letters

    def smlPass(self, func):
        def wrapper(*args):
            if tdSql.getVariable("smlChildTableName")[0].upper() == "ID":
                return func(*args)
            else:
                pass
        return wrapper

    def checkEqual(self, sql, result, expected):
        if result == expected:
            tdLog.info("%s, result:%s == expected:%s" %
                       (sql, result, expected))
        else:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            args = (caller.filename, caller.lineno, sql, result, expected)
            tdLog.exit("%s(%d) failed: sql:%s, result:%s != expected:%s" % args)

    def close(self):
        self.cursor.close()


tdCom = TDCom()
