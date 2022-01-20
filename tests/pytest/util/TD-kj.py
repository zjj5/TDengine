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
        case1<xyguo>:
        ''' 
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        #os.system("rm -rf 2-query/1-select/TD-kj.py.sql")
        os.system("rm -rf util/TD-kj.py.sql")

    def result_0(self,sql):
        tdLog.info(sql)       
        tdSql.query(sql)
        tdSql.checkRows(0)

    # def regular_where(self):       
    #     q_int_where = ['q_bigint >= -9223372036854775807 and ' , 'q_bigint <= 9223372036854775807 and ','q_smallint >= -32767 and ', 'q_smallint <= 32767 and ',
    #     'q_tinyint >= -127 and ' , 'q_tinyint <= 127 and ' , 'q_int <= 2147483647 and ' , 'q_int >= -2147483647 and ',
    #     'q_tinyint != 128 and ',
    #     'q_bigint between  -9223372036854775807 and 9223372036854775807 and ',' q_int between -2147483647 and 2147483647 and ',
    #     'q_smallint between -32767 and 32767 and ', 'q_tinyint between -127 and 127  and ',
    #     'q_bigint is not null and ' , 'q_int is not null and ' , 'q_smallint is not null and ' , 'q_tinyint is not null and ' ,]

    #     q_fl_do_where = ['q_float >= -3.4E38 and ','q_float <= 3.4E38 and ', 'q_double >= -1.7E308 and ','q_double <= 1.7E308 and ', 
    #     'q_float between -3.4E38 and 3.4E38 and ','q_double between -1.7E308 and 1.7E308 and ' ,
    #     'q_float is not null and ' ,'q_double is not null and ' ,]

    #     q_nc_bi_bo_ts_where = [ 'q_bool is not null and ' ,'q_binary is not null and ' ,'q_nchar is not null and ' ,'q_ts is not null and ' ,]

    #     q_where = random.sample(q_int_where,2) + random.sample(q_fl_do_where,1) + random.sample(q_nc_bi_bo_ts_where,1)
    #     print(q_where)
    #     return q_where
        

    # def regular_where_all(self):       
    #     q_int_where_add = ['q_bigint >= 0 and ' , 'q_smallint >= 0 and ', 'q_tinyint >= 0 and ' ,  'q_int >= 0 and ',
    #     'q_bigint between  0 and 9223372036854775807 and ',' q_int between 0 and 2147483647 and ',
    #     'q_smallint between 0 and 32767 and ', 'q_tinyint between 0 and 127  and ',
    #     'q_bigint is not null and ' , 'q_int is not null and ' ,]

    #     q_fl_do_where_add = ['q_float >= 0 and ', 'q_double >= 0 and ' , 'q_float between 0 and 3.4E38 and ','q_double between 0 and 1.7E308 and ' ,
    #     'q_float is not null and ' ,]

    #     q_nc_bi_bo_ts_where_add = ['q_nchar is not null and ' ,'q_ts is not null and ' ,]

    #     q_where_add = random.sample(q_int_where_add,2) + random.sample(q_fl_do_where_add,1) + random.sample(q_nc_bi_bo_ts_where_add,1)
        
    #     q_int_where_sub = ['q_bigint <= 0 and ' , 'q_smallint <= 0 and ', 'q_tinyint <= 0 and ' ,  'q_int <= 0 and ',
    #     'q_bigint between -9223372036854775807 and 0 and ',' q_int between -2147483647 and 0 and ',
    #     'q_smallint between -32767 and 0 and ', 'q_tinyint between -127 and 0 and ',
    #     'q_smallint is not null and ' , 'q_tinyint is not null and ' ,]

    #     q_fl_do_where_sub = ['q_float <= 0 and ', 'q_double <= 0 and ' , 'q_float between -3.4E38 and 0 and ','q_double between -1.7E308 and 0 and ' ,
    #     'q_double is not null and ' ,]

    #     q_nc_bi_bo_ts_where_sub = ['q_bool is not null and ' ,'q_binary is not null and ' ,]

    #     q_where_sub = random.sample(q_int_where_sub,2) + random.sample(q_fl_do_where_sub,1) + random.sample(q_nc_bi_bo_ts_where_sub,1)

    #     return(q_where_add,q_where_sub)

    # def stable_where(self):       
    #     q_where = self.regular_where()

    #     t_int_where = ['t_bigint >= -9223372036854775807 and ' , 't_bigint <= 9223372036854775807 and ','t_smallint >= -32767 and ', 't_smallint <= 32767 and ',
    #     't_tinyint >= -127 and ' , 't_tinyint <= 127 and ' , 't_int <= 2147483647 and ' , 't_int >= -2147483647 and ',
    #     't_tinyint != 128 and ',
    #     't_bigint between  -9223372036854775807 and 9223372036854775807 and ',' t_int between -2147483647 and 2147483647 and ',
    #     't_smallint between -32767 and 32767 and ', 't_tinyint between -127 and 127  and ',
    #     't_bigint is not null and ' , 't_int is not null and ' , 't_smallint is not null and ' , 't_tinyint is not null and ' ,]

    #     t_fl_do_where = ['t_float >= -3.4E38 and ','t_float <= 3.4E38 and ', 't_double >= -1.7E308 and ','t_double <= 1.7E308 and ', 
    #     't_float between -3.4E38 and 3.4E38 and ','t_double between -1.7E308 and 1.7E308 and ' ,
    #     't_float is not null and ' ,'t_double is not null and ' ,]

    #     t_nc_bi_bo_ts_where = [ 't_bool is not null and ' ,'t_binary is not null and ' ,'t_nchar is not null and ' ,'t_ts is not null and ' ,]

    #     t_where = random.sample(t_int_where,2) + random.sample(t_fl_do_where,1) + random.sample(t_nc_bi_bo_ts_where,1)
        
    #     qt_where = q_where + t_where
    #     print(qt_where)
    #     return qt_where


    # def stable_where_all(self):  
        # regular_where_all = self.regular_where_all()

        # t_int_where_add = ['t_bigint >= 0 and ' , 't_smallint >= 0 and ', 't_tinyint >= 0 and ' ,  't_int >= 0 and ',
        # 't_bigint between  1 and 9223372036854775807 and ',' t_int between 1 and 2147483647 and ',
        # 't_smallint between 1 and 32767 and ', 't_tinyint between 1 and 127  and ',
        # 't_bigint is not null and ' , 't_int is not null and ' ,]

        # t_fl_do_where_add = ['t_float >= 0 and ', 't_double >= 0 and ' , 't_float between 1 and 3.4E38 and ','t_double between 1 and 1.7E308 and ' ,
        # 't_float is not null and ' ,]

        # t_nc_bi_bo_ts_where_add = ['t_nchar is not null and ' ,'t_ts is not null and ' ,]

        # qt_where_add = random.sample(t_int_where_add,1) + random.sample(t_fl_do_where_add,1) + random.sample(t_nc_bi_bo_ts_where_add,1) + random.sample(regular_where_all[0],2)
        
        # t_int_where_sub = ['t_bigint <= 0 and ' , 't_smallint <= 0 and ', 't_tinyint <= 0 and ' ,  't_int <= 0 and ',
        # 't_bigint between -9223372036854775807 and -1 and ',' t_int between -2147483647 and -1 and ',
        # 't_smallint between -32767 and -1 and ', 't_tinyint between -127 and -1 and ',
        # 't_smallint is not null and ' , 't_tinyint is not null and ' ,]

        # t_fl_do_where_sub = ['t_float <= 0 and ', 't_double <= 0 and ' , 't_float between -3.4E38 and -1 and ','t_double between -1.7E308 and -1 and ' ,
        # 't_double is not null and ' ,]

        # t_nc_bi_bo_ts_where_sub = ['t_bool is not null and ' ,'t_binary is not null and ' ,]

        # qt_where_sub = random.sample(t_int_where_sub,1) + random.sample(t_fl_do_where_sub,1) + random.sample(t_nc_bi_bo_ts_where_sub,1) + random.sample(regular_where_all[1],2)

        # qt_in = ['q_bool in (0 , 1) ' ,  'q_bool in ( true , false) ' ,' (q_bool = true or  q_bool = false)' , '(q_bool = 0 or q_bool = 1)',]

        # hanshu = ['MIN','AVG','MAX','COUNT','SUM','STDDEV','FIRST','LAST','LAST_ROW','','SPREAD','CEIL','FLOOR','ROUND']
        # column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double_null)','(q_bigint_null)','(q_smallint_null)','(q_tinyint_null)','(q_int_null)','(q_float_null)','(q_double_null)']
        # hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        # return(qt_where_add,qt_where_sub,qt_in,hanshu_column)


    def run(self):
        tdSql.prepare()

        dcDB = tdCreateData.dropandcreateDB_random(1)
        
       
        stable_where_all = tdWhere.stable_where_all()
        print(stable_where_all)
        for i in range(2,len(stable_where_all[0])+1):
            qt_where_add_new = list(combinations(stable_where_all[0],i))
            for qt_where_add_new in qt_where_add_new:
                qt_where_add_new = str(qt_where_add_new).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","").replace("=","")
    
        for j in range(2,len(stable_where_all[1])+1):
            qt_where_sub_new = list(combinations(stable_where_all[1],j))
            for qt_where_sub_new in qt_where_sub_new:
                qt_where_sub_new = str(qt_where_sub_new).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","").replace("=","")
                sql = "select * from stable_1 where %s %s ts < now +1s order by ts " %(qt_where_add_new,qt_where_sub_new)

                tdSql.query(sql)

        # dcCK = tdCreateData.data2in1('select  * from (select  * from stable_1 where ts < now +1h) ',3000,20,\
        #     '(select  * from stable_1 where ts < now +1h and loc in (\'table_1\')) union all \
        #     (select  * from stable_1 where ts < now +1h and loc in (\'table_3\')) union all \
        #     (select  * from stable_2 where ts < now +1h and loc in (\'table_21\')) union all \
        #     (select  * from stable_1 where ts < now +1h and loc in (\'table_2\')) ',3000,20)  

        conn1 = taos.connect(host="127.0.0.1", user="root", password="taosdata", config="/etc/taos/")
        print(conn1)
        cur1 = conn1.cursor()
        tdSql.init(cur1, True)        
        cur1.execute('use db ;')
        sql = 'select * from stable_1 limit 10;'
        cur1.execute(sql)
        for data in cur1:
            print("ts = %s" %data[0])
        
        print(conn1)

        for i in range(10000):
            try:
                #taos_cmd1 = "taos -f 2-query/1-select/TD-kj.py.sql"
                taos_cmd1 = "taos -f util/TD-kj.py.sql"
                _ = subprocess.check_output(taos_cmd1, shell=True).decode("utf-8")

                print(i)
                print(conn1)

                for i in range(10):
                    cur1.execute('use db ;')
                    # 方法1，直接复现具体sql
                    #sql = 'select * from stable_1 where t_smallint between 0 and 32767 and  t_float between 0 and 3.4E38 and  t_nchar is not null and  q_smallint between 0 and 32767 and  q_nchar is not null and  t_binary is not null and  q_tinyint is not null and  ts < now +1s order by ts ;;;'
                    #sql = 'select count(*) from stable_1 '
                    #sql = 'select elapsed(ts,10s) from regular_table_1  interval(10s)  union all select elapsed(ts,10s) from regular_table_2 interval(10s);'
                    #sql = 'select * from stable_1 where t_smallint between 0 and 32767 and  t_float between 0 and 3.4E38 and  t_nchar is not null and  q_smallint between 0 and 32767 and  q_nchar is not null and  t_binary is not null and  q_tinyint is not null and  ts < now +1s order by ts ;'
                    # sql2 = ' select  q_int from stable_1 where q_ts is not null and  q_float between -3.4E38 and 3.4E38 and  q_bool in (0 , 1)  order by ts limit 2'
                    # cur1.execute(sql2)
                    # for data in cur1:
                    #     print("ts = %s" %data[0])

                    # sql1 = 'select * from stable_1;'   
                    # dcCK = tdCreateData.data2in1('%s' %sql1 ,20,10,'%s' %sql2 ,1,1)
                    
                    # sql = 'select * from stable_1 where t_tinyint >= 0 and  t_float >= 0 and  t_ts is not null and  q_float >= 0 and  q_int >= 0 and  t_bigint <= 0 and  t_double is not null and  ts < now +1s order by ts limit 2;'
                    
                    # cur1.execute(sql)
                    # for data in cur1:
                    #     print("ts = %s" %data[0])

                    # sql = 'select * from stable_1 where t_smallint > 0 and  t_double > 0 and  t_nchar is not null and  q_tinyint > 0 and  q_float is not null and  t_smallint is not null and  t_double is not null and  ts < now +1s order by ts limit 2;'
                    
                    # #dcRD = tdCreateData.restartDnodes()
                    
                    # cur1.execute(sql)
                    # for data in cur1:
                    #     print("ts = %s" %data[0])

                    #方法2，直接复现一组sql
                    stable_where = tdWhere.stable_where()
                    #print(stable_where_all)      
                    sql1 = 'select * from stable_1;'         
                    for i in range(2,len(stable_where[0])+1):
                        qt_where_new = list(combinations(stable_where[0],i))
                        for qt_where_new in qt_where_new:
                            qt_where_new = str(qt_where_new).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                            qt_in_where_new = str(random.sample(stable_where[1],1)).replace("[","").replace("]","").replace("'","")
                            hanshu_column_new = str(stable_where[2]).replace("[","").replace("]","").replace("'","").replace(",","")
                            #sql = "select * from stable_1 where %s %s ts < now +1s order by ts limit 2" %(qt_where_add_new,qt_where_sub_new)
                            #sql = "select * from stable_1 where %s %s %s order by ts limit 2" %(qt_where_add_new,qt_where_sub_new,qt_in_new)
                            sql = "select * from stable_1 where %s %s order by ts limit 2" %(qt_where_new,qt_in_where_new)
                            print(sql)
                            cur1.execute(sql)
                            for data in cur1:
                                print("ts = %s" %data[0])
                            sql = "select %s from stable_1 where %s %s order by ts limit 2" %(hanshu_column_new,qt_where_new,qt_in_where_new)
                            dcCK = tdCreateData.data2in1('%s' %sql1 ,20,20,'%s' %sql ,1,20)
                            print(sql)
                            cur1.execute(sql)
                            for data in cur1:
                                print("ts = %s" %data[0])
                            

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