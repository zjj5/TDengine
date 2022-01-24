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
from re import S
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
from util.where_makesql import *
import itertools
from itertools import product
from itertools import combinations
from faker import Faker
import subprocess

class TDTestCase:
    def caseDescription(self):
        '''
        case1<xyguo>:select column_hanshu from table where condition time_window order\group_by (s)limit_(s)offset
        case2<xyguo>:select different column_hanshu from table where condition time_window order\group_by (s)limit_(s)offset
        case3<xyguo>:select column_hanshu from table1、table2 where join_condition (s)limit_(s)offset
        ''' 
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf query_new/%s.sql" % testcaseFilename )

    def run(self):
        tdSql.prepare()
        startTime = time.time() 
        db = "db1"
        tdCreateData.dropandcreateDB_random("%s" %db,2) 

        conn1 = taos.connect(host="127.0.0.1", user="root", password="taosdata", config="/etc/taos/")
        print(conn1)
        cur1 = conn1.cursor()
        tdSql.init(cur1, True)        
        cur1.execute('use "%s";' %db)
        sql = 'select * from regular_table_1 limit 5;'
        cur1.execute(sql)

        for i in range(2):
            try:
                testcaseFilename = os.path.split(__file__)[-1]
                taos_cmd1 = "taos -f query_new/%s.sql" % testcaseFilename
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)

                for i in range(2):
                    print(db)
                    cur1.execute('use "%s";' %db)                 

                    print("case1:select column_hanshu from table where condition time_window order\group_by (s)limit_(s)offset")

                    tdCreateData.restartDnodes()
                    regular_where = tdWhere_makesql.regular_where()
                    tdWhere_makesql.altertable()
                    column_hanshu = regular_where[0]
                    table = regular_where[1]                    
                    for i in range(1,len(regular_where[2])+1):
                        q_where = list(combinations(regular_where[2],i))
                        for q_where in q_where:
                            q_where = str(q_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                            q_like_match = regular_where[3]   
                            q_in_where = regular_where[4]                         
                            time_window = regular_where[5]
                            og_by = regular_where[6]
                            limit_offset = regular_where[7]

                            sql = "select %s from %s where %s %s " %(column_hanshu,table,q_like_match,q_in_where)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from %s where %s %s %s" %(column_hanshu,table,q_where,q_like_match,q_in_where)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "(select %s from %s where %s %s )" %(column_hanshu,table,q_where,q_in_where)
                            sql += " union all (select %s from %s where %s %s )" %(column_hanshu,table,q_in_where,limit_offset)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from %s where %s %s %s %s %s" %(column_hanshu,table,q_where,q_in_where,time_window,og_by,limit_offset)
                            tdWhere_makesql.execution_sql(sql)                            
                            
                            sql = "select * from (select %s from %s where %s %s %s %s %s)" %(column_hanshu,table,q_where,q_in_where,time_window,og_by,limit_offset)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s) where %s %s %s %s %s" %(column_hanshu,table,q_where,q_in_where,time_window,og_by,limit_offset)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s where %s %s %s ) %s %s" %(column_hanshu,table,q_where,q_in_where,time_window,og_by,limit_offset)
                            tdWhere_makesql.execution_sql(sql)
                  
                            # sql="select count(*) in (\"a\",\"b\") from stable_1 dd where %s %s group by tbname;" %(q_where,q_in_where)
                            # tdWhere_makesql.execution_sql(sql)

                            
                    print("case2:select different column_hanshu from table where condition time_window order\group_by (s)limit_(s)offset")

                    regular_where2 = tdWhere_makesql.regular_where2()
                    tdWhere_makesql.altertable()
                    column_hanshu = regular_where2[0]
                    table = regular_where2[1]
                    for i in range(2,len(regular_where2[2])+1):
                        q_where = list(combinations(regular_where2[2],i))
                        for q_where in q_where:
                            q_where = str(q_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                            q_in_where = regular_where2[3]                            
                            time_window = regular_where2[4]
                            og_by = regular_where2[5]
                            limit_offset = regular_where2[6]

                            sql = "select %s from %s where %s %s " %(column_hanshu,table,q_where,q_in_where)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "(select %s from %s where %s %s )" %(column_hanshu,table,q_where,q_in_where)
                            sql += " union all (select %s from %s where %s %s )" %(column_hanshu,table,q_in_where,limit_offset)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from %s where %s %s %s %s %s" %(column_hanshu,table,q_where,q_in_where,time_window,og_by,limit_offset)
                            tdWhere_makesql.execution_sql(sql)
                            
                            sql = "select * from (select %s from %s where %s %s %s %s %s)" %(column_hanshu,table,q_where,q_in_where,time_window,og_by,limit_offset)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s) where %s %s %s %s %s" %(column_hanshu,table,q_where,q_in_where,time_window,og_by,limit_offset)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s where %s %s %s) %s %s" %(column_hanshu,table,q_where,q_in_where,time_window,og_by,limit_offset)
                            tdWhere_makesql.execution_sql(sql)

                    print("case3:select column_hanshu from table1、table2 where join_condition (s)limit_(s)offset")

                    #tdCreateData.restartDnodes()
                    regular_2table = tdWhere_makesql.regular_2table()
                    tdWhere_makesql.altertable()
                    column_hanshu = regular_2table[0]
                    table1 = regular_2table[1]    
                    table2 = regular_2table[2]                 
                    for i in range(1,len(regular_2table[3])+1):
                        q_where = list(combinations(regular_2table[3],i))
                        for q_where in q_where:
                            q_where = str(q_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                            q_like_match = regular_2table[4]   
                            q_in_where = regular_2table[5]                         
                            time_window = regular_2table[6]
                            og_by = regular_2table[7]
                            limit_offset = regular_2table[8]

                            sql = "select %s from %s t1 , %s t2 where %s %s %s " %(column_hanshu,table1,table2,q_where,q_in_where,limit_offset)
                            tdWhere_makesql.execution_sql(sql)
                            
            except Exception as e:
                raise e   

        endTime = time.time()
        print("total time %ds" % (endTime - startTime))

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())