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
import subprocess

class TDTestCase:
    def caseDescription(self):
        '''
        case1<xyguo>:select * from stable_1 where condition union all select * from stable_2[null data] where condition && select * from ( union all )
        case1.1<xyguo>:select * from stable_1 where condition union all select * from stable_1[null data] where condition && select * from ( union all )
        case2<xyguo>:select * from stable_1 where condition order by ts asc | desc union all select * from stable_2[null data] where condition && select * from ( union all )
        case2.1<xyguo>:select * from stable_1 where condition order by ts asc | desc union all select * from stable_1[null data] where condition && select * from ( union all )
        case3<xyguo>:select * from stable_1 where condition order by ts limit union all select * from stable_2[null data] where condition && select * from ( union all )
        case3.1<xyguo>:select * from stable_1 where condition order by ts limit union all select * from stable_1[null data] where condition && select * from ( union all )")
        case4<xyguo>:select * from stable_1 where condition order by ts limit offset union all select * from stable_2[null data] where condition && select * from ( union all )
        case4.1<xyguo>:select * from stable_1 where condition order by ts limit offset union all select * from stable_1[null data] where condition && select * from ( union all )
        case5<xyguo>:
        case6<xyguo>:
        case7<xyguo>:
        case8<xyguo>:
        case9<xyguo>:
        case10<xyguo>:
        ''' 
        return

    #basic_param
    db = "stable_union"
    table_list = ['stable_1','stable_2',]
    table = str(random.sample(table_list,1)).replace("[","").replace("]","").replace("'","")
    table_null_list = ['stable_null_data','stable_null_childtable']
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
        sql = 'select * from stable_1 limit 5;'
        cur1.execute(sql)

        return(conn1,cur1)

    def right_case1(self):
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]

        for i in range(2):
            try:
                taos_cmd1 = "taos -f query_new/%s.sql" % self.testcaseFilename
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                 

                print("case1:select * from stable_1 where condition union all select * from stable_2[null data] where condition && select * from ( union all )")
                print("\n\n\n=========================================case1=========================================\n\n\n")
                stable_where = tdWhere.stable_where()
                sql1 = 'select * from %s;' % self.table
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select * from %s where %s %s %s " %(self.table,qt_where,qt_like_match,qt_in_where)
                        sql2 += " union all select * from %s where %s %s %s " %(self.table_null,qt_where,qt_like_match,qt_in_where)
                        tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)

                        sql2 = "select * from %s where %s %s %s " %(self.table_null,qt_where,qt_like_match,qt_in_where)
                        sql2 += " union all select * from %s where %s %s %s " %(self.table,qt_where,qt_like_match,qt_in_where)
                        tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)

                        sql2 = "select * from ( %s )" %sql2 
                        tdSql.error(sql2)

                print("case1.1:select * from stable_1 where condition union all select * from stable_1[null data] where condition && select * from ( union all )")
                print("\n\n\n=========================================case1.1=========================================\n\n\n")
                stable_where_all_and_null = tdWhere.stable_where_all_and_null()
                sql1 = 'select * from %s;' % self.table
                for i in range(2,len(stable_where_all_and_null[2])+1):
                    qt_where = list(combinations(stable_where_all_and_null[2],i))                        
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where_all_and_null[3]
                        qt_in_where = stable_where_all_and_null[4]
                        sql2 = ""
                for i in range(2,len(stable_where_all_and_null[5])+1):   
                    qt_where_null = list(combinations(stable_where_all_and_null[5],i))     
                    for qt_where_null in qt_where_null:
                        qt_where_null = str(qt_where_null).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","") 
                        qt_like_match_null = stable_where_all_and_null[6]

                        sql2 = "select * from %s where %s %s %s " %(self.table,qt_where,qt_like_match,qt_in_where)
                        sql2 += " union all select * from %s where %s %s %s " %(self.table,qt_where_null,qt_like_match_null,qt_in_where)
                        tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)

                        sql2 = "select * from %s where %s %s %s " %(self.table,qt_where_null,qt_like_match_null,qt_in_where)
                        sql2 += " union all select * from %s where %s %s %s " %(self.table,qt_where,qt_like_match,qt_in_where)
                        tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)

                        sql2 = "select * from ( %s )" %sql2 
                        tdSql.error(sql2)
            
            except Exception as e:
                raise e   

    def right_case2(self):
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]

        for i in range(2):
            try:
                taos_cmd1 = "taos -f query_new/%s.sql" % self.testcaseFilename
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                 

                print("case2:select * from stable_1 where condition order by ts asc | desc union all select * from stable_2[null data] where condition && select * from ( union all )")
                print("\n\n\n=========================================case2=========================================\n\n\n")
                stable_where = tdWhere.stable_where()
                sql1 = 'select * from %s ;' % self.table
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select * from %s where tbname in ('%s_1') and %s %s %s order by ts" %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        sql2 += " union all select * from %s where %s %s %s " %(self.table_null,qt_where,qt_like_match,qt_in_where)
                        tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)

                        sql2 = "select * from ( %s )" %sql2 
                        tdSql.error(sql2)

                        sql2 = "select * from %s where %s %s %s order by ts" %(self.table_null,qt_where,qt_like_match,qt_in_where)
                        sql2 += " union all select * from %s where tbname in ('%s_1') and %s %s %s order by ts" %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)

                        sql2 = "select * from ( %s )" %sql2 
                        tdSql.error(sql2)

                stable_where = tdWhere.stable_where()
                sql1 = 'select * from %s order by ts desc;' % self.table
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select * from %s where %s %s %s order by ts desc" %(self.table,qt_where,qt_like_match,qt_in_where)
                        sql2 += " union all select * from %s where %s %s %s " %(self.table_null,qt_where,qt_like_match,qt_in_where)
                        tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)

                        sql2 = "select * from ( %s )" %sql2 
                        tdSql.error(sql2)

                        sql2 = "select * from %s where %s %s %s order by ts desc" %(self.table_null,qt_where,qt_like_match,qt_in_where)
                        sql2 += " union all select * from %s where %s %s %s order by ts desc" %(self.table,qt_where,qt_like_match,qt_in_where)
                        tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)

                        sql2 = "select * from ( %s )" %sql2 
                        tdSql.error(sql2)

                print("case2.1:select * from stable_1 where condition order by ts asc | desc union all select * from stable_1[null data] where condition && select * from ( union all )")
                print("\n\n\n=========================================case2.1=========================================\n\n\n")
                stable_where_all_and_null = tdWhere.stable_where_all_and_null()
                sql1 = 'select * from %s order by ts ;' % self.table
                for i in range(2,len(stable_where_all_and_null[2])+1):
                    qt_where = list(combinations(stable_where_all_and_null[2],i))                        
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where_all_and_null[3]
                        qt_in_where = stable_where_all_and_null[4]
                        sql2 = ""
                for i in range(2,len(stable_where_all_and_null[5])+1):   
                    qt_where_null = list(combinations(stable_where_all_and_null[5],i))     
                    for qt_where_null in qt_where_null:
                        qt_where_null = str(qt_where_null).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")  
                        qt_like_match_null = stable_where_all_and_null[6]

                        sql2 = "select * from %s where %s %s %s order by ts" %(self.table,qt_where,qt_like_match,qt_in_where)
                        sql2 += " union all select * from %s where %s %s %s " %(self.table,qt_where_null,qt_like_match_null,qt_in_where)
                        tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)

                        sql2 = "select * from %s where %s %s %s order by ts " %(self.table,qt_where_null,qt_like_match_null,qt_in_where)
                        sql2 += " union all select * from %s where %s %s %s order by ts " %(self.table,qt_where,qt_like_match,qt_in_where)
                        tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)

                        sql2 = "select * from ( %s )" %sql2 
                        tdSql.error(sql2)

                stable_where_all_and_null = tdWhere.stable_where_all_and_null()
                sql1 = 'select * from %s order by ts desc;' % self.table
                for i in range(2,len(stable_where_all_and_null[2])+1):
                    qt_where = list(combinations(stable_where_all_and_null[2],i))                        
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where_all_and_null[3]
                        qt_in_where = stable_where_all_and_null[4]
                        sql2 = ""
                for i in range(2,len(stable_where_all_and_null[5])+1):   
                    qt_where_null = list(combinations(stable_where_all_and_null[5],i))     
                    for qt_where_null in qt_where_null:
                        qt_where_null = str(qt_where_null).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","") 
                        qt_like_match_null = stable_where_all_and_null[6]

                        sql2 = "select * from %s where %s %s %s order by ts desc" %(self.table,qt_where,qt_like_match,qt_in_where)
                        sql2 += " union all select * from %s where %s %s %s order by ts desc" %(self.table,qt_where_null,qt_like_match_null,qt_in_where)
                        tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)

                        sql2 = "select * from %s where %s %s %s order by ts desc " %(self.table,qt_where_null,qt_like_match_null,qt_in_where)
                        sql2 += " union all select * from %s where %s %s %s order by ts desc " %(self.table,qt_where,qt_like_match,qt_in_where)
                        tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)

                        sql2 = "select * from ( %s )" %sql2 
                        tdSql.error(sql2)
            
            except Exception as e:
                raise e   

    def right_case3(self):
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]

        for i in range(2):
            try:
                taos_cmd1 = "taos -f query_new/%s.sql" % self.testcaseFilename
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                 

                print("case3:select * from stable_1 where condition order by ts limit union all select * from stable_2[null data] where condition && select * from ( union all )")
                print("\n\n\n=========================================case3=========================================\n\n\n")
                stable_where = tdWhere.stable_where()
                sql1 = 'select * from %s;' % self.table
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select * from %s where   tbname in ('%s_1') and %s %s %s order by ts limit 10" %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        sql2 += " union all select * from %s where %s %s %s " %(self.table_null,qt_where,qt_like_match,qt_in_where)
                        tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)

                        sql2 = "select * from ( %s )" %sql2 
                        tdSql.error(sql2)

                        sql2 = "select * from %s where %s %s %s order by ts limit 10" %(self.table_null,qt_where,qt_like_match,qt_in_where)
                        sql2 += " union all select * from %s where tbname in ('%s_1') and %s %s %s order by ts limit 10" %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)

                        sql2 = "select * from ( %s )" %sql2 
                        tdSql.error(sql2)

                print("case3.1:select * from stable_1 where condition order by ts limit union all select * from stable_1[null data] where condition && select * from ( union all )")
                print("\n\n\n=========================================case3.1=========================================\n\n\n")
                stable_where_all_and_null = tdWhere.stable_where_all_and_null()
                sql1 = 'select * from %s;' % self.table
                for i in range(2,len(stable_where_all_and_null[2])+1):
                    qt_where = list(combinations(stable_where_all_and_null[2],i))                        
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where_all_and_null[3]
                        qt_in_where = stable_where_all_and_null[4]
                        sql2 = ""
                for i in range(2,len(stable_where_all_and_null[5])+1):   
                    qt_where_null = list(combinations(stable_where_all_and_null[5],i))     
                    for qt_where_null in qt_where_null:
                        qt_where_null = str(qt_where_null).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","") 
                        qt_like_match_null = stable_where_all_and_null[6]

                        sql2 = "select * from %s where tbname in ('%s_1') and %s %s %s order by ts limit 10" %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        sql2 += " union all select * from %s where %s %s %s " %(self.table,qt_where_null,qt_like_match_null,qt_in_where)
                        tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)

                        sql2 = "select * from %s where %s %s %s order by ts limit 10" %(self.table,qt_where_null,qt_like_match_null,qt_in_where)
                        sql2 += " union all select * from %s where tbname in ('%s_1') and %s %s %s order by ts limit 10" %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)

                        sql2 = "select * from ( %s )" %sql2 
                        tdSql.error(sql2)
            
            except Exception as e:
                raise e   

    def right_case4(self):
        case_common = self.case_common()
        conn1 = case_common[0]
        cur1 = case_common[1]

        for i in range(2):
            try:
                taos_cmd1 = "taos -f query_new/%s.sql" % self.testcaseFilename
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")
                print(conn1)
                cur1.execute('use "%s";' %self.db)                 

                print("case4:select * from stable_1 where condition order by ts limit offset union all select * from stable_2[null data] where condition && select * from ( union all )")
                print("\n\n\n=========================================case4=========================================\n\n\n")
                stable_where = tdWhere.stable_where()
                sql1 = 'select * from %s limit 10 offset 5;' % self.table
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where[3]
                        qt_in_where = stable_where[4]

                        sql2 = "select * from %s where tbname in ('%s_1') and %s %s %s order by ts limit 10 offset 5" %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        sql2 += " union all select * from %s where %s %s %s order by ts limit 10 offset 5" %(self.table_null,qt_where,qt_like_match,qt_in_where)
                        tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)

                        sql2 = "select * from ( %s )" %sql2 
                        tdSql.error(sql2)

                        sql2 = "select * from %s where %s %s %s order by ts limit 10 offset 5" %(self.table_null,qt_where,qt_like_match,qt_in_where)
                        sql2 += " union all select * from %s where tbname in ('%s_1') and  %s %s %s order by ts limit 10 offset 5" %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)

                        sql2 = "select * from ( %s )" %sql2 
                        tdSql.error(sql2)

                print("case4.1:select * from stable_1 where condition order by ts limit offset union all select * from stable_1[null data] where condition && select * from ( union all )")
                print("\n\n\n=========================================case4.1=========================================\n\n\n")
                stable_where_all_and_null = tdWhere.stable_where_all_and_null()
                sql1 = 'select * from %s limit 10 offset 5;' % self.table
                for i in range(2,len(stable_where_all_and_null[2])+1):
                    qt_where = list(combinations(stable_where_all_and_null[2],i))                        
                    for qt_where in qt_where:
                        qt_where = str(qt_where).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                        qt_like_match = stable_where_all_and_null[3]
                        qt_in_where = stable_where_all_and_null[4]
                        sql2 = ""
                for i in range(2,len(stable_where_all_and_null[5])+1):   
                    qt_where_null = list(combinations(stable_where_all_and_null[5],i))     
                    for qt_where_null in qt_where_null:
                        qt_where_null = str(qt_where_null).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")  
                        qt_like_match_null = stable_where_all_and_null[6]

                        sql2 = "select * from %s where tbname in ('%s_1') and  %s %s %s order by ts limit 10 offset 5 " %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        sql2 += " union all select * from %s where %s %s %s order by ts limit 10 offset 5" %(self.table,qt_where_null,qt_like_match_null,qt_in_where)
                        tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)

                        sql2 = "select * from %s where %s %s %s order by ts limit 10  offset 5" %(self.table,qt_where_null,qt_like_match_null,qt_in_where)
                        sql2 += " union all select * from %s where tbname in ('%s_1') and  %s %s %s order by ts limit 10  offset 5" %(self.table,self.table,qt_where,qt_like_match,qt_in_where)
                        tdCreateData.dataequal('%s' %sql1 ,10,10,'%s' %sql2 ,10,10)
                        cur1.execute(sql2)

                        sql2 = "select * from ( %s )" %sql2 
                        tdSql.error(sql2)
            
            except Exception as e:
                raise e   

    def run(self):
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