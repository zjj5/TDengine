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

        db = "db1"
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
                    print("=========================================case1=========================================")

                    regular_where = tdWhere_makesql.regular_where()
                    tdWhere_makesql.altertable()
                    column = regular_where[0]
                    hanshu_column = regular_where[1]
                    table = regular_where[2]
                    for i in range(2,len(regular_where[3])+1):
                        q_where = list(combinations(regular_where[3],i))
                        for q_where in q_where:
                            q_where = str(q_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                            q_in_where = regular_where[4]                            
                            time_window = regular_where[5]
                            sql = "select * from %s where %s %s " %(table,q_where,q_in_where)
                            tdWhere_makesql.execution_sql(sql)
                            

                            sql = "select * from (select * from %s where %s %s )" %(table,q_where,q_in_where)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select * from (select * from %s) where %s %s " %(table,q_where,q_in_where)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select * from %s where %s %s %s " %(table,q_where,q_in_where,time_window )
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select * from (select * from %s where %s %s %s )" %(table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select * from (select * from %s) where %s %s %s" %(table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from %s where %s %s " %(column,table,q_where,q_in_where )
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from %s where %s %s " %(hanshu_column,table,q_where,q_in_where )
                            tdWhere_makesql.execution_sql(sql)

                            #sql = "select %s != %s from %s where %s %s " %(hanshu_column,hanshu_column,table,q_where,q_in_where )
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s where %s %s %s )" %(column,table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s where %s %s %s )" %(hanshu_column,table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s) where %s %s %s" %(column,table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s) where %s %s %s" %(hanshu_column,table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)
                            
                            sql = "select %s from (select * from %s) where %s %s %s " %(column,table,q_where,q_in_where,time_window )
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s) where %s %s %s " %(hanshu_column,table,q_where,q_in_where,time_window )
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s) where %s %s %s " %(column,table,q_where,q_in_where,time_window )
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s) where %s %s %s " %(hanshu_column,table,q_where,q_in_where,time_window )
                            tdWhere_makesql.execution_sql(sql)

                    print("case2:select * from regular_table where condition order by ts asc | desc && select * from ( select front )")
                    print("=========================================case2=========================================")

                    regular_where = tdWhere_makesql.regular_where()
                    column = regular_where[0]
                    hanshu_column = regular_where[1]
                    table = regular_where[2]
                    for i in range(2,len(regular_where[3])+1):
                        q_where = list(combinations(regular_where[3],i))
                        for q_where in q_where:
                            q_where = str(q_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                            q_in_where = regular_where[4]
                            time_window = regular_where[5]
                            sql = "select * from %s where %s %s order by ts" %(table,q_where,q_in_where)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select * from (select * from %s where %s %s order by ts)" %(table,q_where,q_in_where)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select * from (select * from %s) where %s %s order by ts" %(table,q_where,q_in_where)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select * from %s where %s %s %s  order by ts" %(table,q_where,q_in_where,time_window )
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select * from (select * from %s where %s %s %s  order by ts)" %(table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select * from (select * from %s) where %s %s %s  order by ts" %(table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from %s where %s %s  order by ts" %(hanshu_column,table,q_where,q_in_where )
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s where %s %s %s  order by ts)" %(hanshu_column,table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s where %s %s %s ) order by ts" %(hanshu_column,table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s) where %s %s %s  order by ts" %(hanshu_column,table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)
                            
                            sql = "select %s from (select * from %s) where %s %s %s  order by ts" %(hanshu_column,table,q_where,q_in_where,time_window )
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s) where %s %s %s  order by ts" %(hanshu_column,table,q_where,q_in_where,time_window )
                            tdWhere_makesql.execution_sql(sql)
                    
                    regular_where = tdWhere_makesql.regular_where()
                    column = regular_where[0]
                    hanshu_column = regular_where[1]
                    table = regular_where[2]
                    for i in range(2,len(regular_where[3])+1):
                        q_where = list(combinations(regular_where[3],i))
                        for q_where in q_where:
                            q_where = str(q_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                            q_in_where = regular_where[4]
                            time_window = regular_where[5]
                            sql = "select * from %s where %s %s order by ts desc" %(table,q_where,q_in_where)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select * from (select * from %s where %s %s order by ts desc)" %(table,q_where,q_in_where)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select * from (select * from %s) where %s %s order by ts desc" %(table,q_where,q_in_where)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select * from %s where %s %s %s  order by ts desc" %(table,q_where,q_in_where,time_window )
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select * from (select * from %s where %s %s %s  order by ts desc)" %(table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select * from (select * from %s) where %s %s %s  order by ts desc" %(table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from %s where %s %s  order by ts desc" %(hanshu_column,table,q_where,q_in_where )
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s where %s %s %s  order by ts desc)" %(hanshu_column,table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s where %s %s %s ) order by ts desc" %(hanshu_column,table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s) where %s %s %s  order by ts desc" %(hanshu_column,table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)
                            
                            sql = "select %s from (select * from %s) where %s %s %s  order by ts desc" %(hanshu_column,table,q_where,q_in_where,time_window )
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s) where %s %s %s  order by ts desc" %(hanshu_column,table,q_where,q_in_where,time_window )
                            tdWhere_makesql.execution_sql(sql)

                    print("case3:select * from regular_table where condition order by ts limit && select * from ( select front )")
                    print("=========================================case3=========================================")

                    regular_where = tdWhere_makesql.regular_where()
                    column = regular_where[0]
                    hanshu_column = regular_where[1]
                    table = regular_where[2]
                    for i in range(2,len(regular_where[3])+1):
                        q_where = list(combinations(regular_where[3],i))
                        for q_where in q_where:
                            q_where = str(q_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                            q_in_where = regular_where[4]
                            time_window = regular_where[5]
                            sql = "select * from %s where %s %s order by ts limit 10" %(table,q_where,q_in_where)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select * from (select * from %s where %s %s order by ts limit 10)" %(table,q_where,q_in_where)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select * from (select * from %s) where %s %s order by ts limit 10" %(table,q_where,q_in_where)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select * from %s where %s %s %s  order by ts limit 10" %(table,q_where,q_in_where,time_window )
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select * from (select * from %s where %s %s %s  order by ts limit 10)" %(table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select * from (select * from %s) where %s %s %s  order by ts limit 10" %(table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from %s where %s %s  order by ts limit 10" %(hanshu_column,table,q_where,q_in_where )
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s where %s %s %s  order by ts limit 10)" %(hanshu_column,table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s where %s %s %s  order by ts ) limit 10" %(hanshu_column,table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s where %s %s %s ) order by ts limit 10" %(hanshu_column,table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s) where %s %s %s  order by ts limit 10" %(hanshu_column,table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)
                            
                            sql = "select %s from (select * from %s) where %s %s %s  order by ts limit 10" %(hanshu_column,table,q_where,q_in_where,time_window )
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s) where %s %s %s  order by ts limit 10" %(hanshu_column,table,q_where,q_in_where,time_window )
                            tdWhere_makesql.execution_sql(sql)

                    print("case4:select * from regular_table where condition order by ts limit offset && select * from ( select front )")
                    print("=========================================case4=========================================")

                    regular_where = tdWhere_makesql.regular_where()
                    column = regular_where[0]
                    hanshu_column = regular_where[1]
                    table = regular_where[2]
                    for i in range(2,len(regular_where[3])+1):
                        q_where = list(combinations(regular_where[3],i))
                        for q_where in q_where:
                            q_where = str(q_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                            q_in_where = regular_where[4]
                            time_window = regular_where[5]
                            sql = "select * from %s where %s %s order by ts limit 10 offset 5" %(table,q_where,q_in_where)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select * from (select * from %s where %s %s order by ts limit 10 offset 5)" %(table,q_where,q_in_where)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select * from (select * from %s) where %s %s order by ts limit 10 offset 5" %(table,q_where,q_in_where)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select * from %s where %s %s %s  order by ts limit 10 offset 5" %(table,q_where,q_in_where,time_window )
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select * from (select * from %s where %s %s %s  order by ts limit 10 offset 5)" %(table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select * from (select * from %s) where %s %s %s  order by ts limit 10 offset 5" %(table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from %s where %s %s  order by ts limit 10 offset 5" %(hanshu_column,table,q_where,q_in_where )
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s where %s %s %s  order by ts limit 10 offset 5)" %(hanshu_column,table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s where %s %s %s  order by ts ) limit 10 offset 5" %(hanshu_column,table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s where %s %s %s ) order by ts limit 10 offset 5" %(hanshu_column,table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s) where %s %s %s  order by ts limit 10 offset 5" %(hanshu_column,table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)
                            
                            sql = "select %s from (select * from %s) where %s %s %s  order by ts limit 10 offset 5" %(hanshu_column,table,q_where,q_in_where,time_window )
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s) where %s %s %s  order by ts limit 10 offset 5" %(hanshu_column,table,q_where,q_in_where,time_window )
                            tdWhere_makesql.execution_sql(sql)

                    print("case5:select * from regular_table where condition order by ts limit offset && select * from ( select front )")
                    print("=========================================case4=========================================")

                    regular_where = tdWhere_makesql.regular_where()
                    column = regular_where[0]
                    hanshu_column = regular_where[1]
                    table = regular_where[2]
                    for i in range(2,len(regular_where[3])+1):
                        q_where = list(combinations(regular_where[3],i))
                        for q_where in q_where:
                            q_where = str(q_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                            q_in_where = regular_where[4]
                            time_window = regular_where[5]
                            sql = "select * from %s where %s %s order by ts limit 10 offset 5" %(table,q_where,q_in_where)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select * from (select * from %s where %s %s order by ts limit 10 offset 5)" %(table,q_where,q_in_where)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select * from (select * from %s) where %s %s order by ts limit 10 offset 5" %(table,q_where,q_in_where)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select * from %s where %s %s %s  order by ts limit 10 offset 5" %(table,q_where,q_in_where,time_window )
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select * from (select * from %s where %s %s %s  order by ts limit 10 offset 5)" %(table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select * from (select * from %s) where %s %s %s  order by ts limit 10 offset 5" %(table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from %s where %s %s  order by ts limit 10 offset 5" %(hanshu_column,table,q_where,q_in_where )
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s where %s %s %s  order by ts limit 10 offset 5)" %(hanshu_column,table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s where %s %s %s  order by ts ) limit 10 offset 5" %(hanshu_column,table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s where %s %s %s ) order by ts limit 10 offset 5" %(hanshu_column,table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s) where %s %s %s  order by ts limit 10 offset 5" %(hanshu_column,table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)
                            
                            sql = "select %s from (select * from %s) where %s %s %s  order by ts limit 10 offset 5" %(hanshu_column,table,q_where,q_in_where,time_window )
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s) where %s %s %s  order by ts limit 10 offset 5" %(hanshu_column,table,q_where,q_in_where,time_window )
                            tdWhere_makesql.execution_sql(sql)

                    print("=======================================error case=======================================")
                    print("case1:select * from regular_table where condition interval | sliding | Fill && select * from ( select front )")
                    print("=========================================case1=========================================")

                    regular_where = tdWhere_makesql.regular_where()
                    column = regular_where[0]
                    hanshu_column = regular_where[1]
                    table = regular_where[2]
                    for i in range(2,len(regular_where[3])+1):
                        q_where = list(combinations(regular_where[3],i))
                        for q_where in q_where:
                            q_where = str(q_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                            q_in_where = regular_where[4]  
                            time_window = regular_where[5]
                            #sql2 = "select * from %s where %s %s %s" %(table,q_where,q_in_where,time_window)
                            sql="select count(*) in (\"a\",\"b\") from stable_1 dd where %s %s group by tbname;" %(q_where,q_in_where)
                            #tdWhere_makesql.execution_sql(sql)

                            sql = "select * from (select * from %s where %s %s %s)" %(table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select * from (select * from %s) where %s %s %s" %(table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)

                    print("case1:select * from regular_table where condition && select * from ( select front )")
                    print("=========================================case1=========================================")

                    regular_where = tdWhere_makesql.regular_where()
                    column = regular_where[0]
                    hanshu_column = regular_where[1]
                    table = regular_where[2]
                    for i in range(2,len(regular_where[3])+1):
                        q_where = list(combinations(regular_where[3],i))
                        for q_where in q_where:
                            q_where = str(q_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                            q_in_where = regular_where[4]
                            time_window = regular_where[5]
                            sql = "select %s from %s where %s %s %s" %(hanshu_column,table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select * from (select %s from %s where %s %s %s)" %(hanshu_column,table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select * from (select %s from %s) where %s %s %s" %(hanshu_column,table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select * from %s) where %s %s %s" %(hanshu_column,table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)

                            sql = "select %s from (select %s from %s) where %s %s %s" %(hanshu_column,hanshu_column,table,q_where,q_in_where,time_window)
                            tdWhere_makesql.execution_sql(sql)
                            

            except Exception as e:
                raise e   

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())