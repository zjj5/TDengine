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
from util.log import tdLog
from util.cases import tdCases
from util.sql import tdSql
from util.dnodes import tdDnodes
from util.dnodes import *
import itertools
from itertools import product
from itertools import combinations
from faker import Faker
from util.createdata import tdCreateData

class TDWhere:
    updatecfgDict={'maxSQLLength':1048576}

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

        os.system("rm -rf util/where.py.sql")
 
    # def regular1_checkall(self,sql):
    #     tdLog.info(sql)      
    #     tdSql.query(sql)
    #     for i in range(1,4):
    #         for j in range(0,99):
	#             tdSql.checkData(j,i,j)
    #     for i in range(8,9):
    #         for j in range(0,99):
	#             tdSql.checkData(j,i,j)        
    #     for j in range(0,99):
    #         tdSql.checkData(j,6,'binary.'+str(j)) 

    #     for j in range(0,99):
    #         tdSql.checkData(j,7,'nchar.'+str(j)) 
        
    #     for j in range(0,99):
    #         tdSql.checkData(j,10,'2021-08-27 01:46:40.0'+str(j)) 

    # column and tag query
    # **int + floot_dou + other
    # q_where + t_where | q_where_null + t_where_null [前2组union] | q_where_all + t_where_all[自己union]
    def q_where(self):       
        # q_where = ['q_binary match \'binary%\'  'q_binary like \'binary%\'  or q_nchar = \'0\' ']

        q_int_where = ['q_bigint >= -9223372036854775807 and ' , 'q_bigint <= 9223372036854775807 and ','q_smallint >= -32767 and ', 'q_smallint <= 32767 and ',
        'q_tinyint >= -127 and ' , 'q_tinyint <= 127 and ' , 'q_int <= 2147483647 and ' , 'q_int >= -2147483647 and ',
        'q_tinyint != 128 and ',
        'q_bigint between  -9223372036854775807 and 9223372036854775807 and ',' q_int between -2147483647 and 2147483647 and ',
        'q_smallint between -32767 and 32767 and ', 'q_tinyint between -127 and 127  and ',
        'q_bigint is not null and ' , 'q_int is not null and ' , 'q_smallint is not null and ' , 'q_tinyint is not null and ' ,]

        q_fl_do_where = ['q_float >= -3.4E38 and ','q_float <= 3.4E38 and ', 'q_double >= -1.7E308 and ','q_double <= 1.7E308 and ', 
        'q_float between -3.4E38 and 3.4E38 and ','q_double between -1.7E308 and 1.7E308 and ' ,
        'q_float is not null and ' ,'q_double is not null and ' ,]

        q_nc_bi_bo_ts_where = [ 'q_bool is not null and ' ,'q_binary is not null and ' ,'q_nchar is not null and ' ,'q_ts is not null and ' ,]
        
        q_where = random.sample(q_int_where,4) + random.sample(q_fl_do_where,2) + random.sample(q_nc_bi_bo_ts_where,2)

        q_in_where = ['q_bool in (0 , 1) ' ,  'q_bool in ( true , false) ' ,' (q_bool = true or  q_bool = false)' , '(q_bool = 0 or q_bool = 1)',]
        q_in = random.sample(q_in_where,1)
        
        return(q_where,q_in)

    def q_where_null(self):  

        q_int_where = ['q_bigint < -9223372036854775807 and ' , 'q_bigint > 9223372036854775807 and ','q_smallint < -32767 and ', 'q_smallint > 32767 and ',
        'q_tinyint < -127 and ' , 'q_tinyint > 127 and ' , 'q_int > 2147483647 and ' , 'q_int < -2147483647 and ',
        'q_bigint between  9223372036854775807 and -9223372036854775807 and ',' q_int between 2147483647 and -2147483647 and ',
        'q_smallint between 32767 and -32767 and ', 'q_tinyint between 127 and -127  and ',
        'q_bigint is null and ' , 'q_int is null and ' , 'q_smallint is null and ' , 'q_tinyint is null and ' ,]

        q_fl_do_where = ['q_float < -3.4E38 and ','q_float > 3.4E38 and ', 'q_double < -1.7E308 and ','q_double > 1.7E308 and ', 
        'q_float between 3.4E38 and -3.4E38 and ','q_double between 1.7E308 and -1.7E308 and ' ,
        'q_float is null and ' ,'q_double is null and ' ,]

        q_nc_bi_bo_ts_where = [ 'q_bool is null and ' ,'q_binary is null and ' ,'q_nchar is null and ' ,'q_ts is null and ' ,]
        
        q_where_null = random.sample(q_int_where,4) + random.sample(q_fl_do_where,2) + random.sample(q_nc_bi_bo_ts_where,2)

        q_in_where = ['q_bool in (0 , 1) ' ,  'q_bool in ( true , false) ' ,' (q_bool = true or  q_bool = false)' , '(q_bool = 0 or q_bool = 1)',]
        q_in_null = random.sample(q_in_where,1)

        return(q_where_null,q_in_null)

    def t_where(self):   
        t_int_where = ['t_bigint >= -9223372036854775807 and ' , 't_bigint <= 9223372036854775807 and ','t_smallint >= -32767 and ', 't_smallint <= 32767 and ',
        't_tinyint >= -127 and ' , 't_tinyint <= 127 and ' , 't_int <= 2147483647 and ' , 't_int >= -2147483647 and ',
        't_tinyint != 128 and ',
        't_bigint between  -9223372036854775807 and 9223372036854775807 and ',' t_int between -2147483647 and 2147483647 and ',
        't_smallint between -32767 and 32767 and ', 't_tinyint between -127 and 127  and ',
        't_bigint is not null and ' , 't_int is not null and ' , 't_smallint is not null and ' , 't_tinyint is not null and ' ,]

        t_fl_do_where = ['t_float >= -3.4E38 and ','t_float <= 3.4E38 and ', 't_double >= -1.7E308 and ','t_double <= 1.7E308 and ', 
        't_float between -3.4E38 and 3.4E38 and ','t_double between -1.7E308 and 1.7E308 and ' ,
        't_float is not null and ' ,'t_double is not null and ' ,]

        t_nc_bi_bo_ts_where = [ 't_bool is not null and ' ,'t_binary is not null and ' ,'t_nchar is not null and ' ,'t_ts is not null and ' ,]

        t_where = random.sample(t_int_where,4) + random.sample(t_fl_do_where,2) + random.sample(t_nc_bi_bo_ts_where,2)
        
        t_in = ['t_bool in (0 , 1) ' ,  't_bool in ( true , false) ' ,' (t_bool = true or  t_bool = false)' , '(t_bool = 0 or t_bool = 1)',]
        
        return(t_where,t_in)

    def t_where_null(self):   
        t_int_where = ['t_bigint < -9223372036854775807 and ' , 't_bigint > 9223372036854775807 and ','t_smallint < -32767 and ', 't_smallint > 32767 and ',
        't_tinyint < -127 and ' , 't_tinyint > 127 and ' , 't_int > 2147483647 and ' , 't_int < -2147483647 and ',
        't_bigint between  9223372036854775807 and -9223372036854775807 and ',' t_int between 2147483647 and -2147483647 and ',
        't_smallint between 32767 and -32767 and ', 't_tinyint between 127 and -127  and ',
        't_bigint is null and ' , 't_int is null and ' , 't_smallint is null and ' , 't_tinyint is null and ' ,]

        t_fl_do_where = ['t_float < -3.4E38 and ','t_float > 3.4E38 and ', 't_double < -1.7E308 and ','t_double > 1.7E308 and ', 
        't_float between 3.4E38 and -3.4E38 and ','t_double between 1.7E308 and -1.7E308 and ' ,
        't_float is null and ' ,'t_double is null and ' ,]

        t_nc_bi_bo_ts_where = [ 't_bool is null and ' ,'t_binary is null and ' ,'t_nchar is null and ' ,'t_ts is null and ' ,]

        t_where_null = random.sample(t_int_where,4) + random.sample(t_fl_do_where,2) + random.sample(t_nc_bi_bo_ts_where,2)
        
        t_in_where = ['t_bool in (0 , 1) ' ,  't_bool in ( true , false) ' ,' (t_bool = true or  t_bool = false)' , '(t_bool = 0 or t_bool = 1)',]
        t_in_null = random.sample(t_in_where,1)

        return(t_where_null,t_in_null)

    def hanshu_int(self):       
        hanshu = ['MIN','AVG','MAX','COUNT','SUM','STDDEV','FIRST','LAST','LAST_ROW','','SPREAD','CEIL','FLOOR','ROUND']
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double_null)','(q_bigint_null)','(q_smallint_null)','(q_tinyint_null)','(q_int_null)','(q_float_null)','(q_double_null)']        
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        return hanshu_column

    def hanshu_all(self):       
        hanshu = ['COUNT','SUM','STDDEV','FIRST','LAST','LAST_ROW','','SPREAD','CEIL','FLOOR','ROUND']
        column = ['(*)','(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double_null)','(q_bigint_null)','(q_smallint_null)','(q_tinyint_null)','(q_int_null)','(q_float_null)','(q_double_null)']        
        hanshu_column_all = random.sample(hanshu,1)+random.sample(column,1)
        return hanshu_column_all

    # stable_group by.  table_ok
    def hanshu_stable(self):       
        hanshu = ['TWA','IRATE','STDDEV','INTERP','DIFF']
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double_null)','(q_bigint_null)','(q_smallint_null)','(q_tinyint_null)','(q_int_null)','(q_float_null)','(q_double_null)']        
        hanshu_column_stable = random.sample(hanshu,1)+random.sample(column,1)
        return hanshu_column_stable

    def regular_where(self):       
        regular_q_where = self.q_where()
        
        q_where = random.sample(regular_q_where[0],5) 
        q_in_where = regular_q_where[1]

        hanshu_column = self.hanshu_int()

        return(q_where,q_in_where,hanshu_column)

    def regular_where_null(self):       
        regular_q_where_null = self.q_where_null()
        
        q_where_null = random.sample(regular_q_where_null[0],5) 
        q_in_where_null = regular_q_where_null[1]

        hanshu_column = self.hanshu_int()

        return(q_where_null,q_in_where_null,hanshu_column)

    def stable_where(self):       
        stable_q_where = self.q_where()
        stable_t_where = self.t_where()

        qt_where = random.sample(stable_q_where[0],3) + random.sample(stable_t_where[0],3)
        print(qt_where)
        qt_in_where = random.sample((stable_q_where[1] + stable_t_where[1]),1)
        print(qt_in_where)

        hanshu_column = self.hanshu_int()

        return(qt_where,qt_in_where,hanshu_column)

    # test >=0 <=0
    def regular_where_all_null(self):   
        q_where = self.q_where()
        
        q_where = random.sample(q_where[0],5) 
        q_in_where = random.sample(q_where[1],1)

        q_where_null = self.q_where()
        
        q_where = random.sample(q_where[0],5) 
        q_in_where = random.sample(q_where[1],1)

        q_int_where_add = ['q_bigint >= 0 and ' , 'q_smallint >= 0 and ', 'q_tinyint >= 0 and ' ,  'q_int >= 0 and ',
        'q_bigint between  0 and 9223372036854775807 and ',' q_int between 0 and 2147483647 and ',
        'q_smallint between 0 and 32767 and ', 'q_tinyint between 0 and 127  and ',
        'q_bigint is not null and ' , 'q_int is not null and ' ,]

        q_fl_do_where_add = ['q_float >= 0 and ', 'q_double >= 0 and ' , 'q_float between 0 and 3.4E38 and ','q_double between 0 and 1.7E308 and ' ,
        'q_float is not null and ' ,]

        q_nc_bi_bo_ts_where_add = ['q_nchar is not null and ' ,'q_ts is not null and ' ,]

        q_where_add = random.sample(q_int_where_add,2) + random.sample(q_fl_do_where_add,1) + random.sample(q_nc_bi_bo_ts_where_add,1)
        
        q_int_where_sub = ['q_bigint <= 0 and ' , 'q_smallint <= 0 and ', 'q_tinyint <= 0 and ' ,  'q_int <= 0 and ',
        'q_bigint between -9223372036854775807 and 0 and ',' q_int between -2147483647 and 0 and ',
        'q_smallint between -32767 and 0 and ', 'q_tinyint between -127 and 0 and ',
        'q_smallint is not null and ' , 'q_tinyint is not null and ' ,]

        q_fl_do_where_sub = ['q_float <= 0 and ', 'q_double <= 0 and ' , 'q_float between -3.4E38 and 0 and ','q_double between -1.7E308 and 0 and ' ,
        'q_double is not null and ' ,]

        q_nc_bi_bo_ts_where_sub = ['q_bool is not null and ' ,'q_binary is not null and ' ,]

        q_where_sub = random.sample(q_int_where_sub,2) + random.sample(q_fl_do_where_sub,1) + random.sample(q_nc_bi_bo_ts_where_sub,1)

        return(q_where_add,q_where_sub)

    def stable_where_all(self):  
        regular_where_all = self.regular_where_all()

        t_int_where_add = ['t_bigint >= 0 and ' , 't_smallint >= 0 and ', 't_tinyint >= 0 and ' ,  't_int >= 0 and ',
        't_bigint between  0 and 9223372036854775807 and ',' t_int between 0 and 2147483647 and ',
        't_smallint between 0 and 32767 and ', 't_tinyint between 0 and 127  and ',
        't_bigint is not null and ' , 't_int is not null and ' ,]

        t_fl_do_where_add = ['t_float >= 0 and ', 't_double >= 0 and ' , 't_float between 0 and 3.4E38 and ','t_double between 0 and 1.7E308 and ' ,
        't_float is not null and ' ,]

        t_nc_bi_bo_ts_where_add = ['t_nchar is not null and ' ,'t_ts is not null and ' ,]

        qt_where_add = random.sample(t_int_where_add,1) + random.sample(t_fl_do_where_add,1) + random.sample(t_nc_bi_bo_ts_where_add,1) + random.sample(regular_where_all[0],2)
        
        t_int_where_sub = ['t_bigint <= 0 and ' , 't_smallint <= 0 and ', 't_tinyint <= 0 and ' ,  't_int <= 0 and ',
        't_bigint between -9223372036854775807 and 0 and ',' t_int between -2147483647 and 0 and ',
        't_smallint between -32767 and 0 and ', 't_tinyint between -127 and 0 and ',
        't_smallint is not null and ' , 't_tinyint is not null and ' ,]

        t_fl_do_where_sub = ['t_float <= 0 and ', 't_double <= 0 and ' , 't_float between -3.4E38 and -1 and ','t_double between -1.7E308 and -1 and ' ,
        't_double is not null and ' ,]

        t_nc_bi_bo_ts_where_sub = ['t_bool is not null and ' ,'t_binary is not null and ' ,]

        qt_where_sub = random.sample(t_int_where_sub,1) + random.sample(t_fl_do_where_sub,1) + random.sample(t_nc_bi_bo_ts_where_sub,1) + random.sample(regular_where_all[1],2)

        qt_in = ['q_bool in (0 , 1) ' ,  'q_bool in ( true , false) ' ,' (q_bool = true or  q_bool = false)' , '(q_bool = 0 or q_bool = 1)', 't_bool in (0 , 1) ' ,  't_bool in ( true , false) ' ,' (t_bool = true or  t_bool = false)' , '(t_bool = 0 or t_bool = 1)',]

        hanshu_column = self.hanshu_int()

        return(qt_where_add,qt_where_sub,qt_in,hanshu_column)

    # test all and null
    def regular_where_all(self):     
        q_where = self.q_where()  

        q_where = random.sample(q_where[0],5) 
        q_in_where = random.sample(q_where[1],1)

        q_int_where_add = ['q_bigint >= 0 and ' , 'q_smallint >= 0 and ', 'q_tinyint >= 0 and ' ,  'q_int >= 0 and ',
        'q_bigint between  0 and 9223372036854775807 and ',' q_int between 0 and 2147483647 and ',
        'q_smallint between 0 and 32767 and ', 'q_tinyint between 0 and 127  and ',
        'q_bigint is not null and ' , 'q_int is not null and ' ,]

        q_fl_do_where_add = ['q_float >= 0 and ', 'q_double >= 0 and ' , 'q_float between 0 and 3.4E38 and ','q_double between 0 and 1.7E308 and ' ,
        'q_float is not null and ' ,]

        q_nc_bi_bo_ts_where_add = ['q_nchar is not null and ' ,'q_ts is not null and ' ,]

        q_where_add = random.sample(q_int_where_add,2) + random.sample(q_fl_do_where_add,1) + random.sample(q_nc_bi_bo_ts_where_add,1)
        
        q_int_where_sub = ['q_bigint <= 0 and ' , 'q_smallint <= 0 and ', 'q_tinyint <= 0 and ' ,  'q_int <= 0 and ',
        'q_bigint between -9223372036854775807 and 0 and ',' q_int between -2147483647 and 0 and ',
        'q_smallint between -32767 and 0 and ', 'q_tinyint between -127 and 0 and ',
        'q_smallint is not null and ' , 'q_tinyint is not null and ' ,]

        q_fl_do_where_sub = ['q_float <= 0 and ', 'q_double <= 0 and ' , 'q_float between -3.4E38 and 0 and ','q_double between -1.7E308 and 0 and ' ,
        'q_double is not null and ' ,]

        q_nc_bi_bo_ts_where_sub = ['q_bool is not null and ' ,'q_binary is not null and ' ,]

        q_where_sub = random.sample(q_int_where_sub,2) + random.sample(q_fl_do_where_sub,1) + random.sample(q_nc_bi_bo_ts_where_sub,1)

        return(q_where_add,q_where_sub)

    def stable_where_all(self):  
        regular_where_all = self.regular_where_all()

        t_int_where_add = ['t_bigint >= 0 and ' , 't_smallint >= 0 and ', 't_tinyint >= 0 and ' ,  't_int >= 0 and ',
        't_bigint between  0 and 9223372036854775807 and ',' t_int between 0 and 2147483647 and ',
        't_smallint between 0 and 32767 and ', 't_tinyint between 0 and 127  and ',
        't_bigint is not null and ' , 't_int is not null and ' ,]

        t_fl_do_where_add = ['t_float >= 0 and ', 't_double >= 0 and ' , 't_float between 0 and 3.4E38 and ','t_double between 0 and 1.7E308 and ' ,
        't_float is not null and ' ,]

        t_nc_bi_bo_ts_where_add = ['t_nchar is not null and ' ,'t_ts is not null and ' ,]

        qt_where_add = random.sample(t_int_where_add,1) + random.sample(t_fl_do_where_add,1) + random.sample(t_nc_bi_bo_ts_where_add,1) + random.sample(regular_where_all[0],2)
        
        t_int_where_sub = ['t_bigint <= 0 and ' , 't_smallint <= 0 and ', 't_tinyint <= 0 and ' ,  't_int <= 0 and ',
        't_bigint between -9223372036854775807 and 0 and ',' t_int between -2147483647 and 0 and ',
        't_smallint between -32767 and 0 and ', 't_tinyint between -127 and 0 and ',
        't_smallint is not null and ' , 't_tinyint is not null and ' ,]

        t_fl_do_where_sub = ['t_float <= 0 and ', 't_double <= 0 and ' , 't_float between -3.4E38 and -1 and ','t_double between -1.7E308 and -1 and ' ,
        't_double is not null and ' ,]

        t_nc_bi_bo_ts_where_sub = ['t_bool is not null and ' ,'t_binary is not null and ' ,]

        qt_where_sub = random.sample(t_int_where_sub,1) + random.sample(t_fl_do_where_sub,1) + random.sample(t_nc_bi_bo_ts_where_sub,1) + random.sample(regular_where_all[1],2)

        qt_in = ['q_bool in (0 , 1) ' ,  'q_bool in ( true , false) ' ,' (q_bool = true or  q_bool = false)' , '(q_bool = 0 or q_bool = 1)', 't_bool in (0 , 1) ' ,  't_bool in ( true , false) ' ,' (t_bool = true or  t_bool = false)' , '(t_bool = 0 or t_bool = 1)',]

        hanshu_column = self.hanshu_int()

        return(qt_where_add,qt_where_sub,qt_in,hanshu_column)

    def run(self):
        tdSql.prepare()
        #创建库，最普通的
        dcDB = tdCreateData.dropandcreateDB_null()
        #获取where
        regular_where = self.regular_where()
        #combinations遍历where组合
        for i in range(len(regular_where)+1):
            q_where_new = list(combinations(regular_where,i))
            sql1 = 'select  * from regular_table_1;'
            #print(q_where_new)
            for q_where_new in q_where_new:
                #特殊处理
                q_where_new = str(q_where_new).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                #print(q_where_new)
                #拼接sql
                sql2 = "select * from regular_table_1 where %s ts < now +1s;" %(q_where_new)
                dcCK = tdCreateData.dataequal('%s' %sql1 ,100,10,'%s' %sql2 ,100,10) 
                #可靠性落盘
                #dcRD = self.restartDnodes()

        dcDB = tdCreateData.dropandcreateDB_null()
        #获取where,超级表的
        stable_where = self.stable_where()
        #combinations遍历where组合
        for i in range(len(stable_where)+1):
            qt_where_new = list(combinations(stable_where,i))
            sql1 = 'select  * from stable_1;'
            #print(q_where_new)
            for qt_where_new in qt_where_new:
                #特殊处理
                qt_where_new = str(qt_where_new).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                #print(q_where_new)
                #拼接sql
                sql2 = "select * from stable_1 where %s ts < now +1s;" %(qt_where_new)
                dcCK = tdCreateData.dataequal('%s' %sql1 ,100,10,'%s' %sql2 ,100,10) 
                #可靠性落盘
                #dcRD = self.restartDnodes()

        # 创建库，随机数的
        dcDB = tdCreateData.dropandcreateDB_random(1)
        #获取where
        regular_where = self.regular_where()
        #combinations遍历where组合
        for i in range(len(regular_where)+1):
            q_where_new = list(combinations(regular_where,i))
            sql1 = 'select  * from regular_table_1;'
            #print(q_where_new)
            for q_where_new in q_where_new:
                #特殊处理
                q_where_new = str(q_where_new).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                #print(q_where_new)
                #拼接sql
                sql2 = "select * from regular_table_1 where %s ts < now +1s;" %(q_where_new)
                dcCK = tdCreateData.dataequal('%s' %sql1 ,100,10,'%s' %sql2 ,100,10) 
                #可靠性落盘
                #dcRD = self.restartDnodes()

        #创建库，可以选择多个数据表
        dcDB = tdCreateData.dropandcreateDB_random(1)
        #获取where
        regular_where = self.regular_where()
        table_select = ['table_1','table_2','table_3']
        sql1 = 'select  * from stable_1;'
        #遍历where组合
        for i in range(len(regular_where)+1):
            q_where_new = list(combinations(regular_where,i))
            #print(q_where_new)
            for q_where_new in q_where_new:
                #特殊处理
                q_where_new = str(q_where_new).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                for j in itertools.product(table_select):
                    #拼接sql
                    #sql1 = "select * from stable_1 where %s ts < now +1s;" %(q_where_new)
                    sql2 = "select * from %s where %s ts < now +1s;" %(j[0],q_where_new)
                    dcCK = tdCreateData.data2in1('%s' %sql1 ,3000,10,'%s' %sql2 ,1000,10) 
              


        dcDB = tdCreateData.dropandcreateDB_random(1)
        #获取where,返回2组值不带tag，方便union拼接
        
        #table_select = ['table_1','table_2','table_3']
        sql1 = 'select  * from stable_1;'
        #遍历where组合
        regular_where_all = self.regular_where_all()
        print(regular_where_all)
        for i in range(len(regular_where_all[0])+1):
            q_where_add_new = list(combinations(regular_where_all[0],i))
            for q_where_add_new in q_where_add_new:
                q_where_add_new = str(q_where_add_new).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
    
            for j in range(len(regular_where_all[1])+1):
                q_where_sub_new = list(combinations(regular_where_all[1],j))
                for q_where_sub_new in q_where_sub_new:
                    q_where_sub_new = str(q_where_sub_new).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                    sql2 = "(select * from stable_1 where %s ts < now +1s) union all " %(q_where_add_new)
                    sql2 += " (select * from stable_1 where %s ts < now +1s);" %(q_where_sub_new)
            
                    dcCK = tdCreateData.data2in1('%s' %sql1 ,3000,10,'%s' %sql2 ,1000,10) 

        dcDB = tdCreateData.dropandcreateDB_random(1)
        #获取where,返回2组值带tag，方便union拼接
        
        #table_select = ['table_1','table_2','table_3']
        sql1 = 'select  * from stable_1;'
        #遍历where组合
        stable_where_all = self.stable_where_all()
        print(stable_where_all)
        for i in range(len(stable_where_all[0])+1):
            qt_where_add_new = list(combinations(stable_where_all[0],i))
            for qt_where_add_new in qt_where_add_new:
                qt_where_add_new = str(qt_where_add_new).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
    
            for j in range(len(stable_where_all[1])+1):
                qt_where_sub_new = list(combinations(stable_where_all[1],j))
                for qt_where_sub_new in qt_where_sub_new:
                    qt_where_sub_new = str(qt_where_sub_new).replace("(","").replace(")","").replace("'","").replace("\"","").replace(",","")
                    sql2 = "(select * from stable_1 where %s ts < now +1s order by ts) union all " %(qt_where_add_new)
                    sql2 += " (select * from stable_1 where %s ts < now +1s order by ts asc);" %(qt_where_sub_new)
            
                    dcCK = tdCreateData.data2in1('%s' %sql1 ,3000,10,'%s' %sql2 ,2000,10) 


        dcDB = tdCreateData.dropandcreateDB_random(1)
        
        #遍历where组合,checkrows=0
        stable_where_all = self.stable_where_all()
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
        
                #dcCK = tdCreateData.result_0(sql) 
                    
                    
        # night
        dcDB = tdCreateData.dropandcreateDB_null()
    
        dcCK = tdCreateData.dataequal('select  * from regular_table_1 where ts < now +1s;',10,1,'select  * from table_1 where ts < now +1s;',10,1)  
        dcCK = tdCreateData.data2in1('select  * from regular_table_2',100,10,'select  * from table_2',10,10) 
        dcCK = tdCreateData.dataequal('select  * from regular_table_1',100,10,'select  * from stable_1',100,10) 
        
        #dcRD = tdCreateData.restartDnodes()
        dcCK = tdCreateData.dataequal('select  * from (select  * from stable_1 where ts < now +1h limit 1000) ',100,10,\
            '(select  * from stable_1 where ts < now +1h and loc in (\'table_1\') limit 1000 ) union all \
                select  * from stable_1 where ts < now +1h and loc in (\'table_2\') limit 1000',100,10)  

        dcDB = tdCreateData.dropandcreateDB_random(1)
        dcCK = tdCreateData.dataequal('select  * from regular_table_1',100,10,'select  * from regular_table_1 where ts < now +1h',100,10)  


        dcCK = tdCreateData.dataequal('select  * from regular_table_2',100,10,'select  * from regular_table_2 where ts < now +1h',100,10)
        dcCK = tdCreateData.dataequal('select  * from regular_table_3',100,10,'select  * from regular_table_3 where ts < now +1h',100,10)
        dcCK = tdCreateData.data2in1('select  * from stable_1',3000,20,'select  * from table_1 where ts < now +1h',300,10)
        dcCK = tdCreateData.data2in1('select  * from stable_1',3000,20,'select  * from table_2 where ts < now +1h',1000,20)
        dcCK = tdCreateData.data2in1('select  * from stable_1',3000,20,'select  * from table_3 where ts < now +1h',1000,20) 

        dcDB = tdCreateData.dropandcreateDB_random(1)
        dcRD = tdCreateData.restartDnodes()
        dcCK = tdCreateData.data2in1('select  * from (select  * from stable_1 where ts < now +1h) ',3000,10,\
            '(select  * from stable_1 where ts < now +1h and loc in (\'table_1\')) union all \
             (select  * from stable_1 where ts < now +1h and loc in (\'table_3\')) union all \
             (select  * from stable_2 where ts < now +1h and loc in (\'table_21\')) union all \
             (select  * from stable_1 where ts < now +1h and loc in (\'table_2\')) ',3000,10)  
        dcCK = tdCreateData.data2in1('select  * from (select  * from stable_1 where ts < now +1h) ',3000,20,\
            '(select  * from stable_1 where ts < now +1h and loc in (\'table_1\')) union all \
             (select  * from stable_1 where ts < now +1h and loc in (\'table_3\')) union all \
             (select  * from stable_2 where ts < now +1h and loc in (\'table_21\')) union all \
             (select  * from stable_1 where ts < now +1h and loc in (\'table_2\')) ',3000,20)  

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdWhere = TDWhere()