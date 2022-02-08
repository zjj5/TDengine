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
from util.createdata import *
from util.where import *
import subprocess

class TDFunction:
    def caseDescription(self):
        '''
        case1<xyguo>:
        case2<xyguo>:
        case3<xyguo>:
        case4<xyguo>:
        ''' 
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def all_column(self):  
        # support all table, support all data type     
        hanshu = ['COUNT','FIRST','LAST','LAST_ROW']
        column = ['(*)','(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)','(_c0)','(_C0)','(q_ts)','(q_bool)','(q_binary)','(q_nchar)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        all_column = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return all_column
    
    def int_ts_cloumn(self):  
        # support all int type \ double type \ ts type        
        hanshu = ['SPREAD']       
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)','(_c0)','(_C0)','(q_ts)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_ts_cloumn = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return int_ts_cloumn
    
    def int_cloumn(self):  
        # support all int type \ double type      
        hanshu = ['STDDEV','']
        hanshu = ['AVG','SUM','MIN','MAX','SPREAD','CEIL','FLOOR','ROUND']        
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_cloumn = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return int_cloumn

    def int_cloumn_regular_only(self):  
        # diff 不能和order by使用   'DIFF',  
        # not support stable, if support should together with groupby tbname.  support all int type \ double type \ 
        hanshu = ['TWA','IRATE','SPREAD','CEIL','FLOOR','ROUND']        
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_cloumn_regular_only = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return int_cloumn_regular_only

    def int_cloumn_stable_groupby(self):    
        # not support stable, if support should together with groupby tbname.  support all int type \ double type \    
        hanshu = ['TWA','IRATE','DIFF','SPREAD','CEIL','FLOOR','ROUND']        
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_cloumn_stable_groupby = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return int_cloumn_stable_groupby    

    def int1_cloumn_other(self):   
        #   not support stddev/percentile/interp in the outer query 
        # # order by not supported in nested interp query   ,'INTERP'
        hanshu = ['LEASTSQUARES','STDDEV','INTERP','DERIVATIVE','top-bottom',' PERCENTILE- APERCENTILE']
        column = ['(q_bigint)','(q_smallint)','(q_tinyint)','(q_int)','(q_float)','(q_double)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_cloumn = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return int_cloumn  

    def int_cloumn_error(self):  
        # not support all int type \ double type \        
        hanshu = ['AVG','SUM','MIN','MAX','CEIL','FLOOR','ROUND']      
        column = ['(*)','(_c0)','(_C0)','(q_ts)','(q_bool)','(q_binary)','(q_nchar)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_cloumn_error = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return int_cloumn_error  

    def int_ts_cloumn_error(self):  
        # not support all int type \ double type \ ts type        
        hanshu = ['SPREAD']       
        column = ['(*)','(q_bool)','(q_binary)','(q_nchar)'] 
        hanshu_column = random.sample(hanshu,1)+random.sample(column,1)
        int_ts_cloumn = str(hanshu_column).replace("[","").replace("]","").replace("'","").replace(", ","")
        return int_ts_cloumn

    def func_regular_all(self,i):   
        func_regular_all = ''
        if i == 1:    
            func_regular_all = self.all_column()
        elif i == 2:
            func_regular_all = self.int_cloumn()
        elif i == 3:
            func_regular_all = self.int_cloumn_regular_only()
        elif i == 4:
            func_regular_all = self.int_ts_cloumn()

        return func_regular_all
    
    def func_regular_error_all(self,i):   
        func_regular_error_all = ''
        if i == 1:    
            func_regular_error_all = self.int_cloumn_error()
        elif i == 2:
            func_regular_error_all = self.int_ts_cloumn_error()

        return func_regular_error_all

    def func_stable_all(self,i):   
        func_stable_all = ''
        if i == 1:    
            func_stable_all = self.all_column()
        elif i == 2:
            func_stable_all = self.int_cloumn()
        elif i == 3:
            func_stable_all = self.int_cloumn_regular_only()
        elif i == 4:
            func_stable_all = self.int_ts_cloumn()

        return func_stable_all
    
    def func_stable_error_all(self,i):   
        func_stable_error_all = ''
        if i == 1:    
            func_stable_error_all = self.int_cloumn_error()
        elif i == 2:
            func_stable_error_all = self.int_cloumn_error()

        return func_stable_error_all

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)



tdFunction = TDFunction()