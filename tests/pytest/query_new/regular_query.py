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
import os
import sys
import time
import taos
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql
from util.dnodes import tdDnodes
from util.dnodes import *
from util.createdata import *
from util.where import *
import itertools
from itertools import product
from itertools import combinations
from faker import Faker
import subprocess

class TDTestCase:
    def caseDescription(self):
        '''
        case1<xyguo>:select * from regular_table where condition && select * from ( select front )
        case2<xyguo>:select * from regular_table where condition order by ts asc | desc && select * from ( select front )
        case3<xyguo>:select * from regular_table where condition order by ts limit && select * from ( select front )
        case4<xyguo>:select * from regular_table where condition order by ts limit offset && select * from ( select front )
        case5<xyguo>:
        case6<xyguo>:
        case7<xyguo>:
        case8<xyguo>:
        case9<xyguo>:
        case10<xyguo>:
        ''' 
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf query_new/%s.sql" % testcaseFilename )

    def run(self):
        tdSql.prepare()

        db = "regular_db"
        tdCreateData.dropandcreateDB_random("%s" %db,1) 

        conn1 = taos.connect(host="127.0.0.1", user="root", password="taosdata", config="/etc/taos/")
        print(conn1)
        cur1 = conn1.cursor()
        tdSql.init(cur1, True)        
        cur1.execute('use "%s";' %db)
        sql = 'select * from regular_table_1 limit 5;'
        cur1.execute(sql)
        for data in cur1:
            print("ts = %s" %data[0])       
        print(conn1)

        for i in range(2):
            try:
                testcaseFilename = os.path.split(__file__)[-1]
                taos_cmd1 = "taos -f query_new/%s.sql" % testcaseFilename
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)

                for i in range(2):
                    print(db)
                    cur1.execute('use "%s";' %db)                 

                    print("case1:select * from regular_table where condition && select * from ( select front )")

                    regular_where = tdWhere.regular_where()
                    sql1 = 'select * from regular_table_1;' 
                    for i in range(2,len(regular_where[0])+1):
                        q_where_new = list(combinations(regular_where[0],i))
                        for q_where_new in q_where_new:
                            q_where_new = str(q_where_new).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                            q_in_where_new = str(regular_where[1]).replace("[","").replace("]","").replace("'","")
                            sql2 = "select * from regular_table_1 where %s %s " %(q_where_new,q_in_where_new)
                            tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                            cur1.execute(sql2)

                            sql2 = "select * from (select * from regular_table_1 where %s %s )" %(q_where_new,q_in_where_new)
                            tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                            cur1.execute(sql2)

                            sql2 = "select * from (select * from regular_table_1) where %s %s " %(q_where_new,q_in_where_new)
                            tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                            cur1.execute(sql2)

                    print("case2:select * from regular_table where condition order by ts asc | desc && select * from ( select front )")

                    regular_where = tdWhere.regular_where()
                    sql1 = 'select * from regular_table_1;' 
                    for i in range(2,len(regular_where[0])+1):
                        q_where_new = list(combinations(regular_where[0],i))
                        for q_where_new in q_where_new:
                            q_where_new = str(q_where_new).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                            q_in_where_new = str(regular_where[1]).replace("[","").replace("]","").replace("'","")
                            sql2 = "select * from regular_table_1 where %s %s order by ts" %(q_where_new,q_in_where_new)
                            tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                            cur1.execute(sql2)

                            sql2 = "select * from (select * from regular_table_1 where %s %s order by ts)" %(q_where_new,q_in_where_new)
                            tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                            cur1.execute(sql2)

                            sql2 = "select * from (select * from regular_table_1) where %s %s order by ts" %(q_where_new,q_in_where_new)
                            tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                            cur1.execute(sql2)
                    
                    regular_where = tdWhere.regular_where()
                    sql1 = 'select * from regular_table_1 order by ts desc;' 
                    for i in range(2,len(regular_where[0])+1):
                        q_where_new = list(combinations(regular_where[0],i))
                        for q_where_new in q_where_new:
                            q_where_new = str(q_where_new).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                            q_in_where_new = str(regular_where[1]).replace("[","").replace("]","").replace("'","")
                            sql2 = "select * from regular_table_1 where %s %s order by ts desc" %(q_where_new,q_in_where_new)
                            tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                            cur1.execute(sql2)

                            sql2 = "select * from (select * from regular_table_1 where %s %s order by ts desc)" %(q_where_new,q_in_where_new)
                            tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                            cur1.execute(sql2)

                            sql2 = "select * from (select * from regular_table_1) where %s %s order by ts desc" %(q_where_new,q_in_where_new)
                            tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                            cur1.execute(sql2)

                    print("case3:select * from regular_table where condition order by ts limit && select * from ( select front )")

                    regular_where = tdWhere.regular_where()
                    sql1 = 'select * from regular_table_1;' 
                    for i in range(2,len(regular_where[0])+1):
                        q_where_new = list(combinations(regular_where[0],i))
                        for q_where_new in q_where_new:
                            q_where_new = str(q_where_new).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                            q_in_where_new = str(regular_where[1]).replace("[","").replace("]","").replace("'","")
                            sql2 = "select * from regular_table_1 where %s %s order by ts limit 10" %(q_where_new,q_in_where_new)
                            tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                            cur1.execute(sql2)

                            sql2 = "select * from (select * from regular_table_1 where %s %s order by ts limit 10)" %(q_where_new,q_in_where_new)
                            tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                            cur1.execute(sql2)

                            sql2 = "select * from (select * from regular_table_1) where %s %s order by ts limit 10" %(q_where_new,q_in_where_new)
                            tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                            cur1.execute(sql2)

                    print("case4:select * from regular_table where condition order by ts limit offset && select * from ( select front )")

                    regular_where = tdWhere.regular_where()
                    sql1 = 'select * from regular_table_1 limit 10 offset 5;' 
                    for i in range(2,len(regular_where[0])+1):
                        q_where_new = list(combinations(regular_where[0],i))
                        for q_where_new in q_where_new:
                            q_where_new = str(q_where_new).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                            q_in_where_new = str(regular_where[1]).replace("[","").replace("]","").replace("'","")
                            sql2 = "select * from regular_table_1 where %s %s order by ts limit 10 offset 5" %(q_where_new,q_in_where_new)
                            tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                            cur1.execute(sql2)

                            sql2 = "select * from (select * from regular_table_1 where %s %s order by ts limit 10 offset 5)" %(q_where_new,q_in_where_new)
                            tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                            cur1.execute(sql2)

                            sql2 = "select * from (select * from regular_table_1) where %s %s order by ts limit 10 offset 5" %(q_where_new,q_in_where_new)
                            tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                            cur1.execute(sql2)


                    #方法2，直接复现一组sql
                    # stable_where = tdWhere.stable_where()
                    # #print(stable_where_all)      
                    # sql1 = 'select * from stable_1;'         
                    # for i in range(2,len(stable_where[0])+1):
                    #     qt_where_new = list(combinations(stable_where[0],i))
                    #     for qt_where_new in qt_where_new:
                    #         qt_where_new = str(qt_where_new).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                    #         qt_in_where_new = str(random.sample(stable_where[1],1)).replace("[","").replace("]","").replace("'","")
                    #         hanshu_column_new = str(stable_where[2]).replace("[","").replace("]","").replace("'","").replace(",","")
                    #         #sql = "select * from stable_1 where %s %s ts < now +1s order by ts limit 2" %(qt_where_add_new,qt_where_sub_new)
                    #         #sql = "select * from stable_1 where %s %s %s order by ts limit 2" %(qt_where_add_new,qt_where_sub_new,qt_in_new)
                    #         #sql = "select * from stable_1 where %s %s order by ts limit 2" %(qt_where_new,qt_in_where_new)
                    #         sql = "select * from stable_1 where ts < now+1d"
                    #         print(sql)
                    #         # dcCK = tdCreateData.dataequal('%s' %sql1 ,10,2,'%s' %sql ,10,2)
                    #         # dcCK = tdCreateData.data2in1('%s' %sql1 ,10,2,'%s' %sql ,10,2)
                    #         cur1.execute(sql)
                    #         for data in cur1:
                    #             print("ts = %s" %data[0])

                    #         sql2 = "select * from stable_1 where %s %s order by ts limit 2" %(qt_where_new,qt_in_where_new)
                    #         dcCK = tdCreateData.data2in1('%s' %sql1 ,20,3,'%s' %sql2 ,2,4)
                    #         print(sql2)
                    #         cur1.execute(sql2)
                    #         for data in cur1:
                    #             print("ts = %s" %data[0])
                            

                    #方法2，直接复现一组sql
                    # stable_where_all = tdWhere.stable_where_all()
                    # #print(stable_where_all)
                    # for i in range(2,len(stable_where_all[0])+1):
                    #     qt_where_add_new = list(combinations(stable_where_all[0],i))
                    #     for qt_where_add_new in qt_where_add_new:
                    #         qt_where_add_new = str(qt_where_add_new).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                
                    # for j in range(2,len(stable_where_all[1])+1):
                    #     qt_where_sub_new = list(combinations(stable_where_all[1],j))
                    #     for qt_where_sub_new in qt_where_sub_new:
                    #         qt_where_sub_new = str(qt_where_sub_new).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                    #         qt_in_new = str(random.sample(stable_where_all[2],1)).replace("[","").replace("]","").replace("'","")
                    #         hanshu_column_new = str(stable_where_all[3]).replace("[","").replace("]","").replace("'","").replace(",","")
                    #         #sql = "select * from stable_1 where %s %s ts < now +1s order by ts limit 2" %(qt_where_add_new,qt_where_sub_new)
                    #         #sql = "select * from stable_1 where %s %s %s order by ts limit 2" %(qt_where_add_new,qt_where_sub_new,qt_in_new)
                    #         sql = "select %s from stable_1 where %s %s %s order by ts limit 2" %(hanshu_column_new,qt_where_add_new,qt_where_sub_new,qt_in_new)
                    #         print(sql)
                    #         cur1.execute(sql)
                    #         for data in cur1:
                    #             print("ts = %s" %data[0])

                    #方法2，直接复现一组sql-union
                    # stable_where_all = tdWhere.stable_where_all()
                    # #print(stable_where_all)
                    # for i in range(len(stable_where_all[0])+1):
                    #     qt_where_add_new = list(combinations(stable_where_all[0],i))
                    #     for qt_where_add_new in qt_where_add_new:
                    #         qt_where_add_new = str(qt_where_add_new).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                
                    #     for j in range(len(stable_where_all[1])+1):
                    #         qt_where_sub_new = list(combinations(stable_where_all[1],j))
                    #         for qt_where_sub_new in qt_where_sub_new:
                    #             qt_where_sub_new = str(qt_where_sub_new).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                    #             # sql = "(select sum(q_int),sum(q_bigint),sum(q_SMALLINT),sum(q_TINYINT),sum(q_DOUBLE),sum(q_FLOAT) from stable_1 where %s ts < now +1s group by tbname order by ts limit 100) union all " %(qt_where_add_new)
                    #             # sql += " (select sum(q_int),sum(q_bigint),sum(q_SMALLINT),sum(q_TINYINT),sum(q_DOUBLE),sum(q_FLOAT) from stable_1 where %s ts < now +1s group by tbname order by ts asc limit 200 offset 20 ) ;" %(qt_where_sub_new)
                    #             #sql = "(select count(q_int),count(q_bigint),count(q_SMALLINT),count(q_TINYINT),count(q_DOUBLE),count(q_FLOAT),count(q_BOOL),count(q_NCHAR),count(q_BINARY),count(q_ts),count(q_int_null),count(q_bigint_null),count(q_SMALLINT_null),count(q_TINYINT_null),count(q_DOUBLE_null),count(q_FLOAT_null),count(q_BOOL_null),count(q_NCHAR_null),count(q_BINARY_null),count(q_ts_null) from stable_1 where %s ts < now +1s group by tbname order by ts limit 100) union all " %(qt_where_add_new)
                    #             #sql += " (select count(q_int),count(q_bigint),count(q_SMALLINT),count(q_TINYINT),count(q_DOUBLE),count(q_FLOAT),count(q_BOOL),count(q_NCHAR),count(q_BINARY),count(q_ts),count(q_int_null),count(q_bigint_null),count(q_SMALLINT_null),count(q_TINYINT_null),count(q_DOUBLE_null),count(q_FLOAT_null),count(q_BOOL_null),count(q_NCHAR_null),count(q_BINARY_null),count(q_ts_null) from stable_1 where %s ts < now +1s group by tbname order by ts asc limit 200 offset 20 ) ;" %(qt_where_sub_new)
                    #             sql = "(select elapsed(ts)  from stable_1 where %s ts < now +1s group by tbname order by ts limit 100) union all " %(qt_where_add_new)
                    #             sql += " (select elapsed(ts)  from stable_1 where %s ts < now +1s group by tbname order by ts asc limit 200 offset 20 ) ;" %(qt_where_sub_new)
                    #             print(sql)
                    #             cur1.execute(sql)
                    #             for data in cur1:
                    #                 print("ts = %s" %data[0])

                    #             sql = "(select csum(q_int)  from stable_1 where %s ts < now +1s group by tbname order by ts limit 1) union all " %(qt_where_add_new)
                    #             sql += " (select csum(q_int)  from stable_1 where %s ts < now +1s group by tbname order by ts asc limit 2 ) ;" %(qt_where_sub_new)
                    #             print(sql)
                    #             cur1.execute(sql)
                    #             for data in cur1:
                    #                 print("ts = %s" %data[0])

                    #             sql = "(select diff(q_int)  from stable_1 where %s ts < now +1s group by tbname order by ts limit 1) union all " %(qt_where_add_new)
                    #             sql += " (select diff(q_int)  from stable_1 where %s ts < now +1s group by tbname order by ts asc limit 2 ) ;" %(qt_where_sub_new)
                    #             print(sql)
                    #             cur1.execute(sql)
                    #             for data in cur1:
                    #                 print("ts = %s" %data[0])
                                
                    #             sql = "(select ROUND(q_int)  from stable_1 where %s ts < now +1s order by ts limit 1) union all " %(qt_where_add_new)
                    #             sql += " (select ROUND(q_int)  from stable_1 where %s ts < now +1s order by ts asc limit 2 ) ;" %(qt_where_sub_new)
                    #             print(sql)
                    #             cur1.execute(sql)
                    #             for data in cur1:
                    #                 print("ts = %s" %data[0])

                    #             sql = "(select twa(q_int)  from stable_1 where %s ts < now +1s group by tbname order by ts limit 1) union all " %(qt_where_add_new)
                    #             sql += " (select twa(q_int)  from stable_1 where %s ts < now +1s group by tbname order by ts asc limit 2 ) ;" %(qt_where_sub_new)
                    #             print(sql)
                    #             cur1.execute(sql)
                    #             for data in cur1:
                    #                 print("ts = %s" %data[0])

                    #             sql = "(select FLOOR(q_FLOAT)  from stable_1 where %s ts < now +1s order by ts limit 1) union all " %(qt_where_add_new)
                    #             sql += " (select FLOOR(q_FLOAT)  from stable_1 where %s ts < now +1s order by ts asc limit 2 ) ;" %(qt_where_sub_new)
                    #             print(sql)
                    #             cur1.execute(sql)
                    #             for data in cur1:
                    #                 print("ts = %s" %data[0])

                    #             sql = "(select SPREAD(q_FLOAT)  from stable_1 where %s ts < now +1s group by tbname order by ts limit 1) union all " %(qt_where_add_new)
                    #             sql += " (select SPREAD(q_FLOAT)  from stable_1 where %s ts < now +1s group by tbname order by ts asc limit 2 ) ;" %(qt_where_sub_new)
                    #             print(sql)
                    #             cur1.execute(sql)
                    #             for data in cur1:
                    #                 print("ts = %s" %data[0])

                    #             sql = "(select CEIL(q_FLOAT)  from stable_1 where %s ts < now +1s order by ts limit 1) union all " %(qt_where_add_new)
                    #             sql += " (select CEIL(q_FLOAT)  from stable_1 where %s ts < now +1s order by ts asc limit 2 ) ;" %(qt_where_sub_new)
                    #             print(sql)
                    #             cur1.execute(sql)
                    #             for data in cur1:
                    #                 print("ts = %s" %data[0])

                    #             sql = "(select IRATE(q_int)  from stable_1 where %s ts < now +1s group by tbname order by ts limit 1) union all " %(qt_where_add_new)
                    #             sql += " (select IRATE(q_int)  from stable_1 where %s ts < now +1s group by tbname order by ts asc limit 2 ) ;" %(qt_where_sub_new)
                    #             print(sql)
                    #             cur1.execute(sql)
                    #             for data in cur1:
                    #                 print("ts = %s" %data[0])
                                
                    #             sql = "(select DERIVATIVE(q_FLOAT,1m,1)  from stable_1 where %s ts < now +1s group by tbname order by ts limit 1) union all " %(qt_where_add_new)
                    #             sql += " (select DERIVATIVE(q_FLOAT,1m,1)  from stable_1 where %s ts < now +1s group by tbname order by ts asc limit 2 ) ;" %(qt_where_sub_new)
                    #             print(sql)
                    #             cur1.execute(sql)
                    #             for data in cur1:
                    #                 print("ts = %s" %data[0])
                                
                    #             sql = "(select FIRST(q_int),FIRST(q_bigint),FIRST(q_SMALLINT),FIRST(q_TINYINT),FIRST(q_DOUBLE),FIRST(q_FLOAT)  from stable_1 where %s ts < now +1s group by tbname order by ts limit 1) union all " %(qt_where_add_new)
                    #             sql += " (select FIRST(q_int),FIRST(q_bigint),FIRST(q_SMALLINT),FIRST(q_TINYINT),FIRST(q_DOUBLE),FIRST(q_FLOAT)  from stable_1 where %s ts < now +1s group by tbname order by ts asc limit 2 ) ;" %(qt_where_sub_new)
                    #             #sql += " (select LAST(q_int),LAST(q_bigint),LAST(q_SMALLINT),LAST(q_TINYINT),LAST(q_DOUBLE),LAST(q_FLOAT)  from stable_1 where %s ts < now +1s group by tbname order by ts asc limit 2 ) ;" %(qt_where_sub_new)
                    #             print(sql)
                    #             cur1.execute(sql)
                    #             for data in cur1:
                    #                 print("ts = %s" %data[0])

                    #             sql = "(select LAST(q_int),LAST(q_bigint),LAST(q_SMALLINT),LAST(q_TINYINT),LAST(q_DOUBLE),LAST(q_FLOAT)  from stable_1 where %s ts < now +1s group by tbname order by ts limit 1) union all " %(qt_where_add_new)
                    #             sql += " (select LAST(q_int),LAST(q_bigint),LAST(q_SMALLINT),LAST(q_TINYINT),LAST(q_DOUBLE),LAST(q_FLOAT)  from stable_1 where %s ts < now +1s group by tbname order by ts asc limit 2 ) ;" %(qt_where_sub_new)
                    #             print(sql)
                    #             cur1.execute(sql)
                    #             for data in cur1:
                    #                 print("ts = %s" %data[0])
                    #             sql = "(select LAST_ROW(q_int),LAST_ROW(q_bigint),LAST_ROW(q_SMALLINT),LAST_ROW(q_TINYINT),LAST_ROW(q_DOUBLE),LAST_ROW(q_FLOAT),LAST_ROW(q_BOOL),LAST_ROW(q_NCHAR),LAST_ROW(q_BINARY),LAST_ROW(q_ts)  from stable_1 where %s ts < now +1s group by tbname order by ts limit 1) union all " %(qt_where_add_new)
                    #             sql += " (select LAST_ROW(q_int),LAST_ROW(q_bigint),LAST_ROW(q_SMALLINT),LAST_ROW(q_TINYINT),LAST_ROW(q_DOUBLE),LAST_ROW(q_FLOAT),LAST_ROW(q_BOOL),LAST_ROW(q_NCHAR),LAST_ROW(q_BINARY),LAST_ROW(q_ts)  from stable_1 where %s ts < now +1s group by tbname order by ts asc limit 2 ) ;" %(qt_where_sub_new)
                    #             print(sql)
                    #             cur1.execute(sql)
                    #             for data in cur1:
                    #                 print("ts = %s" %data[0])

                    #             # sql = "(select TOP(q_int,10),TOP(q_bigint,10),TOP(q_SMALLINT,10),TOP(q_TINYINT,10),TOP(q_DOUBLE,10),TOP(q_FLOAT,10)  from stable_1 where %s ts < now +1s group by tbname order by ts limit 1) union all " %(qt_where_add_new)
                    #             # sql += " (select TOP(q_int,10),TOP(q_bigint,10),TOP(q_SMALLINT,10),TOP(q_TINYINT,10),TOP(q_DOUBLE,10),TOP(q_FLOAT,10)  from stable_1 where %s ts < now +1s group by tbname order by ts asc limit 2 ) ;" %(qt_where_sub_new)
                    #             sql = "(select TOP(q_FLOAT,10)  from stable_1 where %s ts < now +1s group by tbname order by ts limit 1) union all " %(qt_where_add_new)
                    #             sql += " (select TOP(q_FLOAT,10)  from stable_1 where %s ts < now +1s group by tbname order by ts asc limit 2 ) ;" %(qt_where_sub_new)
                    #             print(sql)
                    #             cur1.execute(sql)
                    #             for data in cur1:
                    #                 print("ts = %s" %data[0])
                    #             sql = "(select bottom(q_FLOAT,10)  from stable_1 where %s ts < now +1s group by tbname order by ts limit 1) union all " %(qt_where_add_new)
                    #             sql += " (select bottom(q_FLOAT,10)  from stable_1 where %s ts < now +1s group by tbname order by ts asc limit 2 ) ;" %(qt_where_sub_new)
                    #             print(sql)
                    #             cur1.execute(sql)
                    #             for data in cur1:
                    #                 print("ts = %s" %data[0])
                    #             sql = "(select INTERP(q_FLOAT)  from stable_1 where %s ts < now +1s group by tbname order by ts limit 1) union all " %(qt_where_add_new)
                    #             sql += " (select INTERP(q_FLOAT)  from stable_2 where %s ts < now +1s group by tbname order by ts asc limit 2 ) ;" %(qt_where_sub_new)
                    #             print(sql)
                    #             cur1.execute(sql)
                    #             for data in cur1:
                    #                 print("ts = %s" %data[0])
                    #             sql = "(select APERCENTILE(q_FLOAT,10)  from stable_1 where %s ts < now +1s group by tbname order by ts limit 1) union all " %(qt_where_add_new)
                    #             sql += " (select APERCENTILE(q_FLOAT,10)  from stable_1 where %s ts < now +1s group by tbname order by ts asc limit 2 ) ;" %(qt_where_sub_new)
                    #             print(sql)
                    #             cur1.execute(sql)
                    #             for data in cur1:
                    #                 print("ts = %s" %data[0])

                    #             sql = "(select sum(q_int),avg(q_bigint),max(q_SMALLINT),min(q_TINYINT),count(q_DOUBLE),avg(q_FLOAT)  from stable_1 where %s ts < now +1s group by tbname order by ts limit 1) union all " %(qt_where_add_new)
                    #             sql += " (select sum(q_int),avg(q_bigint),max(q_SMALLINT),min(q_TINYINT),count(q_DOUBLE),avg(q_FLOAT)  from stable_1 where %s ts < now +1s group by tbname order by ts asc limit 2 offset 1 ) ;" %(qt_where_sub_new)
                    #             print(sql)
                    #             cur1.execute(sql)
                    #             for data in cur1:
                    #                 print("ts = %s" %data[0])                                    

            except Exception as e:
                raise e   

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())