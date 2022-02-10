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
        case1<xyguo>:select * from regular_table where condition [null data] && select * from ( select front )
        case2<xyguo>:select * from regular_table where condition [null data] order by ts asc | desc && select * from ( select front )
        case3<xyguo>:select * from regular_table where condition [null data] order by ts limit && select * from ( select front )
        case4<xyguo>:select * from regular_table where condition [null data] order by ts limit offset && select * from ( select front )
        case5<xyguo>:
        case6<xyguo>:
        case7<xyguo>:
        case8<xyguo>:
        case9<xyguo>:
        case10<xyguo>:
        ''' 
        return

    #basic_param
    db = "regular_db"
    table_list = ['regular_table_1','stable_1_1','regular_table_2','stable_1_2','stable_2_1']
    table = str(random.sample(table_list,1)).replace("[","").replace("]","").replace("'","")
    table_null_list = ['regular_table_null','stable_1_3','stable_1_4','stable_2_2','stable_null_data_1']
    table_null = str(random.sample(table_null_list,1)).replace("[","").replace("]","").replace("'","")
    testcaseFilename = os.path.split(__file__)[-1]

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def case_common(self):
        os.system("rm -rf query_new/%s.sql" % self.testcaseFilename )    
        tdCreateData.dropandcreateDB_random("%s" %self.db,1) 

        conn1 = taos.connect(host="127.0.0.1", user="root", password="taosdata", config="/etc/taos/")
        cur1 = conn1.cursor()
        tdSql.init(cur1, True)        
        cur1.execute('use "%s";' %self.db)
        sql = 'select * from regular_table_1 limit 5;'
        cur1.execute(sql)

        return(conn1,cur1)

    def right_case1(self):
        print("case1:select * from regular_table where condition[null data] && select * from ( select front )")
        print("\n\n\n=========================================case1=========================================\n\n\n")
        
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]

        for i in range(2):
            try:
                taos_cmd1 = "taos -f query_new/%s.sql" % self.testcaseFilename
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                 

                regular_where_null = tdWhere.regular_where_null()
                sql1 = 'select * from %s;'  % self.table
                for i in range(2,len(regular_where_null[2])+1):
                    q_where = list(combinations(regular_where_null[2],i))
                    for q_where in q_where:
                        q_where = str(q_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        q_like_match = regular_where_null[3]
                        q_in_where = regular_where_null[4]
                        
                        sql2 = "select * from %s where %s %s %s " %(self.table,q_where,q_like_match,q_in_where)
                        tdCreateData.result_0(sql2)
                        cur1.execute(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s )" %(self.table,q_where,q_like_match,q_in_where)
                        tdCreateData.result_0(sql2)
                        cur1.execute(sql2)

                        sql2 = "select * from (select * from %s) where %s %s %s " %(self.table,q_where,q_like_match,q_in_where)
                        tdCreateData.result_0(sql2)
                        cur1.execute(sql2)
            
            except Exception as e:
                raise e   

    def right_case2(self):
        print("case2:select * from regular_table where condition[null data] order by ts asc | desc && select * from ( select front )")
        print("\n\n\n=========================================case2=========================================\n\n\n")
        
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]

        for i in range(2):
            try:
                taos_cmd1 = "taos -f query_new/%s.sql" % self.testcaseFilename
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                               

                regular_where_null = tdWhere.regular_where_null()
                sql1 = 'select * from %s ;' % self.table
                for i in range(2,len(regular_where_null[2])+1):
                    q_where = list(combinations(regular_where_null[2],i))
                    for q_where in q_where:
                        q_where = str(q_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        q_like_match = regular_where_null[3]
                        q_in_where = regular_where_null[4]

                        sql2 = "select * from %s where %s %s %s order by ts" %(self.table,q_where,q_like_match,q_in_where)
                        tdCreateData.result_0(sql2)
                        cur1.execute(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts)" %(self.table,q_where,q_like_match,q_in_where)
                        tdCreateData.result_0(sql2)
                        cur1.execute(sql2)

                        sql2 = "select * from (select * from %s) where %s %s %s order by ts" %(self.table,q_where,q_like_match,q_in_where)
                        tdCreateData.result_0(sql2)
                        cur1.execute(sql2)
                
                regular_where_null = tdWhere.regular_where_null()
                sql1 = 'select * from %s order by ts desc;' % self.table
                for i in range(2,len(regular_where_null[2])+1):
                    q_where = list(combinations(regular_where_null[2],i))
                    for q_where in q_where:
                        q_where = str(q_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        q_like_match = regular_where_null[3]
                        q_in_where = regular_where_null[4]

                        sql2 = "select * from %s where %s %s %s order by ts desc" %(self.table,q_where,q_like_match,q_in_where)
                        tdCreateData.result_0(sql2)
                        cur1.execute(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts desc)" %(self.table,q_where,q_like_match,q_in_where)
                        tdCreateData.result_0(sql2)
                        cur1.execute(sql2)

                        sql2 = "select * from (select * from %s) where %s %s %s order by ts desc" %(self.table,q_where,q_like_match,q_in_where)
                        tdCreateData.result_0(sql2)
                        cur1.execute(sql2)
            
            except Exception as e:
                raise e  

    def right_case3(self):
        print("case3:select * from regular_table where condition[null data] order by ts limit && select * from ( select front )")
        print("\n\n\n=========================================case3=========================================\n\n\n")
        
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]

        for i in range(2):
            try:
                taos_cmd1 = "taos -f query_new/%s.sql" % self.testcaseFilename
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                 

                regular_where_null = tdWhere.regular_where_null()
                sql1 = 'select * from %s;' % self.table
                for i in range(2,len(regular_where_null[2])+1):
                    q_where = list(combinations(regular_where_null[2],i))
                    for q_where in q_where:
                        q_where = str(q_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        q_like_match = regular_where_null[3]
                        q_in_where = regular_where_null[4]

                        sql2 = "select * from %s where %s %s %s order by ts limit 10" %(self.table,q_where,q_like_match,q_in_where)
                        tdCreateData.result_0(sql2)
                        cur1.execute(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts limit 10)" %(self.table,q_where,q_like_match,q_in_where)
                        tdCreateData.result_0(sql2)
                        cur1.execute(sql2)

                        sql2 = "select * from (select * from %s) where %s %s %s order by ts limit 10" %(self.table,q_where,q_like_match,q_in_where)
                        tdCreateData.result_0(sql2)
                        cur1.execute(sql2)
            
            except Exception as e:
                raise e  

    def right_case4(self):
        print("case4:select * from regular_table where condition[null data] order by ts limit offset && select * from ( select front )")
        print("\n\n\n=========================================case4=========================================\n\n\n")
        
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]

        for i in range(2):
            try:
                taos_cmd1 = "taos -f query_new/%s.sql" % self.testcaseFilename
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                 

                regular_where_null = tdWhere.regular_where_null()
                sql1 = 'select * from %s limit 10 offset 5;' % self.table
                for i in range(2,len(regular_where_null[2])+1):
                    q_where = list(combinations(regular_where_null[2],i))
                    for q_where in q_where:
                        q_where = str(q_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        q_like_match = regular_where_null[3]
                        q_in_where = regular_where_null[4]

                        sql2 = "select * from %s where %s %s %s order by ts limit 10 offset 5" %(self.table,q_where,q_like_match,q_in_where)
                        tdCreateData.result_0(sql2)
                        cur1.execute(sql2)

                        sql2 = "select * from (select * from %s where %s %s %s order by ts limit 10 offset 5)" %(self.table,q_where,q_like_match,q_in_where)
                        tdCreateData.result_0(sql2)
                        cur1.execute(sql2)

                        sql2 = "select * from (select * from %s) where %s %s %s order by ts limit 10 offset 5" %(self.table,q_where,q_like_match,q_in_where)
                        tdCreateData.result_0(sql2)
                        cur1.execute(sql2)
            
            except Exception as e:
                raise e  


    def run(self):
        tdSql.prepare()
        startTime = time.time() 

        self.right_case1()
        self.right_case2()
        self.right_case3()
        self.right_case4()

        endTime = time.time()
        print("total time %ds" % (endTime - startTime))

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())