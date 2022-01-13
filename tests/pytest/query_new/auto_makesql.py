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

    def execution_sql(self,sql):
        expectErrNotOccured = True
        try:
            self.cursor.execute(sql)
            tdLog.info("sql:%s, no error occured" % (sql))
        except BaseException:            
            expectErrNotOccured = False
            tdLog.info("sql:%s, error occured" % (sql))

    def run(self):
        tdSql.prepare()

        db = "regular_db"
        tdCreateData.dropandcreateDB_random("%s" %db,1) 

        table_list = ['regular_table_1','table_1','regular_table_2','table_2','table_null','regular_table_null','stable_1','stable_2']
        table = str(random.sample(table_list,1)).replace("[","").replace("]","")
        table_null_list = ['table_null','regular_table_null','stable_2']
        table_null = str(random.sample(table_null_list,1)).replace("[","").replace("]","")

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
                # testcaseFilename = os.path.split(__file__)[-1]
                # taos_cmd1 = "taos -f query_new/%s.sql" % testcaseFilename
                # _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                # print(conn1)

                for i in range(2):
                    print(db)
                    cur1.execute('use "%s";' %db)                 

                    print("case1:select * from regular_table where condition && select * from ( select front )")
                    print("=========================================case1=========================================")

                    regular_where = tdWhere.regular_where()
                    for i in range(2,len(regular_where[0])+1):
                        q_where_new = list(combinations(regular_where[0],i))
                        for q_where_new in q_where_new:
                            q_where_new = str(q_where_new).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                            q_in_where_new = str(regular_where[1]).replace("[","").replace("]","").replace("'","")
                            sql = "select * from %s where %s %s " %(table,q_where_new,q_in_where_new)
                            #cur1.execute(sql2)
                            #tdSql.query(sql2)
                            #self.execution_sql(sql)
                            tdSql.error(sql)

                            # sql2 = "select * from (select * from %s where %s %s )" %(table,q_where_new,q_in_where_new)
                            # #tdSql.error(sql2)
                            # self.execute(sql2)

                            # sql2 = "select * from (select * from %s) where %s %s " %(table,q_where_new,q_in_where_new)
                            # #tdSql.error(sql2)
                            # self.execute(sql2)

                    print("case2:select * from regular_table where condition order by ts asc | desc && select * from ( select front )")
                    print("=========================================case2=========================================")

                    regular_where = tdWhere.regular_where()
                    sql1 = 'select * from %s;'  % table
                    for i in range(2,len(regular_where[0])+1):
                        q_where_new = list(combinations(regular_where[0],i))
                        for q_where_new in q_where_new:
                            q_where_new = str(q_where_new).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                            q_in_where_new = str(regular_where[1]).replace("[","").replace("]","").replace("'","")
                            sql2 = "select * from %s where %s %s order by ts" %(table,q_where_new,q_in_where_new)
                            cur1.execute(sql2)

                            sql2 = "select * from (select * from %s where %s %s order by ts)" %(table,q_where_new,q_in_where_new)
                            cur1.execute(sql2)

                            sql2 = "select * from (select * from %s) where %s %s order by ts" %(table,q_where_new,q_in_where_new)
                            cur1.execute(sql2)
                    
                    regular_where = tdWhere.regular_where()
                    sql1 = 'select * from %s order by ts desc;'  % table
                    for i in range(2,len(regular_where[0])+1):
                        q_where_new = list(combinations(regular_where[0],i))
                        for q_where_new in q_where_new:
                            q_where_new = str(q_where_new).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                            q_in_where_new = str(regular_where[1]).replace("[","").replace("]","").replace("'","")
                            sql2 = "select * from %s where %s %s order by ts desc" %(table,q_where_new,q_in_where_new)
                            cur1.execute(sql2)

                            sql2 = "select * from (select * from %s where %s %s order by ts desc)" %(table,q_where_new,q_in_where_new)
                            cur1.execute(sql2)

                            sql2 = "select * from (select * from %s) where %s %s order by ts desc" %(table,q_where_new,q_in_where_new)
                            cur1.execute(sql2)

                    print("case3:select * from regular_table where condition order by ts limit && select * from ( select front )")
                    print("=========================================case3=========================================")

                    # regular_where = tdWhere.regular_where()
                    # sql1 = 'select * from %s;'  % table
                    # for i in range(2,len(regular_where[0])+1):
                    #     q_where_new = list(combinations(regular_where[0],i))
                    #     for q_where_new in q_where_new:
                    #         q_where_new = str(q_where_new).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                    #         q_in_where_new = str(regular_where[1]).replace("[","").replace("]","").replace("'","")
                    #         sql2 = "select * from %s where %s %s order by ts limit 10" %(table,q_where_new,q_in_where_new)
                    #         tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                    #         cur1.execute(sql2)

                    #         sql2 = "select * from (select * from %s where %s %s order by ts limit 10)" %(table,q_where_new,q_in_where_new)
                    #         tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                    #         cur1.execute(sql2)

                    #         sql2 = "select * from (select * from %s) where %s %s order by ts limit 10" %(table,q_where_new,q_in_where_new)
                    #         tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                    #         cur1.execute(sql2)

                    # print("case4:select * from regular_table where condition order by ts limit offset && select * from ( select front )")
                    # print("=========================================case4=========================================")

                    # regular_where = tdWhere.regular_where()
                    # sql1 = 'select * from %s limit 10 offset 5;'  % table
                    # for i in range(2,len(regular_where[0])+1):
                    #     q_where_new = list(combinations(regular_where[0],i))
                    #     for q_where_new in q_where_new:
                    #         q_where_new = str(q_where_new).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                    #         q_in_where_new = str(regular_where[1]).replace("[","").replace("]","").replace("'","")
                    #         sql2 = "select * from %s where %s %s order by ts limit 10 offset 5" %(table,q_where_new,q_in_where_new)
                    #         tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                    #         cur1.execute(sql2)

                    #         sql2 = "select * from (select * from %s where %s %s order by ts limit 10 offset 5)" %(table,q_where_new,q_in_where_new)
                    #         tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                    #         cur1.execute(sql2)

                    #         sql2 = "select * from (select * from %s) where %s %s order by ts limit 10 offset 5" %(table,q_where_new,q_in_where_new)
                    #         tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                    #         cur1.execute(sql2)

                    print("=======================================error case=======================================")
                    print("case1:select * from regular_table where condition interval | sliding | Fill && select * from ( select front )")
                    print("=========================================case1=========================================")

                    regular_where = tdWhere.regular_where()
                    sql1 = 'select * from %s interval(3s) sliding(3n) Fill(NEXT);'  % table
                    for i in range(2,len(regular_where[0])+1):
                        q_where_new = list(combinations(regular_where[0],i))
                        for q_where_new in q_where_new:
                            q_where_new = str(q_where_new).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                            q_in_where_new = str(regular_where[1]).replace("[","").replace("]","").replace("'","")
                            time_window = regular_where[3]
                            #sql2 = "select * from %s where %s %s %s" %(table,q_where_new,q_in_where_new,time_window)
                            #sql2="select count(*) in (\"a\",\"b\") from stable_1 dd where q_int>0 group by tbname;"
                            sql2="select count(*) in (\"a\",\"b\") from stable_1 dd where %s %s group by tbname;" %(q_where_new,q_in_where_new)
                            tdSql.error(sql2)
                            #cur1.execute(sql2)
                            #tdSql.execute_new(sql2)
                            #tdSql.execute(sql2)

                            # sql2 = "select * from (select * from %s where %s %s %s)" %(table,q_where_new,q_in_where_new,time_window)
                            # tdSql.error(sql2)

                            # sql2 = "select * from (select * from %s) where %s %s %s" %(table,q_where_new,q_in_where_new,time_window)
                            # tdSql.error(sql2)
                            

            except Exception as e:
                raise e   

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())