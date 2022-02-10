###################################################################
#           Copyright (c) 2020 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

from ast import arg
from distutils import core
import sys, getopt
import os
import shutil

from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *
import random
import argparse
import subprocess
import time

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        
        
    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        return buildPath

    def caseDescription(self):

        '''
        case1 <wenzhouwww>: this is an test case for auto run crash_gen use different arguments to work : 

        ''' 
        return

    def start(self):   # also we can use deploy with test framework
        buildPath = self.getBuildPath()
        binPath = buildPath + "/build/bin/taosd"
        cfg_path = buildPath.replace("debug","sim")+"/dnode1/cfg"
        cmd = "nohup %s -c %s > /dev/null 2>&1 &" %(binPath, cfg_path)
        os.system(cmd)
        tdLog.debug("dnode is running with : %s"%cmd)

    def limit(self,args_list,error_arguments_mix):
        flag = False
        if args_list["--use-shadow-db"]==True:
            if args_list["--max-dbs"] > 1:
                flag = True
                tdLog.debug("Cannot combine use-shadow-db with max-dbs of more than 1")
                error_arguments_mix+=1
            else:
                pass

        elif args_list["--num-replicas"]==0:
            tdLog.notice(" make sure num-replicas is at least 1 ")
            flag = True
            error_arguments_mix+=1
        elif args_list["--num-replicas"]==1:
            pass

        elif args_list["--num-replicas"]>1:
            if not args_list["--auto-start-service"]:
                tdLog.debug("it should be deployed by crash_gen auto-start-service for multi replicas")
                flag = True
                error_arguments_mix+=1
                
            else:
                if args_list["--num-replicas"] > args_list["--num-dnodes"]:
                    flag = True
                    tdLog.debug("this envs is create by crash_gen auto-start-service ,but replicas is large than num_dnodes , error occured  ")
                    error_arguments_mix+=1
                else:
                    pass 
        print(error_arguments_mix)
        return   error_arguments_mix , flag

    def set_random_value(self,args_list,run_tdengine = False):

        nums_args_list = ["--max-dbs","--num-replicas","--num-dnodes","--max-steps","--num-threads",] # record int type arguments
        bools_args_list = ["--auto-start-service" , "--debug","--run-tdengine","--ignore-errors","--track-memory-leaks","--larger-data","--mix-oos-data","--dynamic-db-table-names",
        "--per-thread-db-connection","--record-ops","--verify-data","--use-shadow-db","--continue-on-exception"
        ]  # record bool type arguments
        strs_args_list = ["--connector-type"]  # record str type arguments

        # connect_types=['native','rest','mixed'] # restful interface has change ,we should trans dbnames to connection or change sql such as "db.test"
        connect_types=['native']
        # args_list["--connector-type"]=connect_types[random.randint(0,2)]
        args_list["--connector-type"]=connect_types[0]

        args_list["--max-dbs"]=random.randint(1,10)
        # args_list["--num-replicas"]=random.randint(1,3)
        # args_list["--num-dnodes"]=random.randint(max(1,args_list["--num-replicas"]-1),args_list["--num-replicas"]+1)
        args_list["--num-replicas"]=1
        args_list["--num-dnodes"]=1
        args_list["--max-steps"]=random.randint(500,3000)
        args_list["--num-threads"]=random.randint(10,50)

        # args_list["--max-steps"]=random.randint(5,30)
        # args_list["--num-threads"]=random.randint(1,5) #$ debug
        args_list["--ignore-errors"]=[]   ## can add error codes for detail

    
        args_list["--auto-start-service"]= True

        if run_tdengine :
            args_list["--run-tdengine"]= True

        for key in bools_args_list:
            set_bool_value = [True,False]
            if key == "--auto-start-service" :
                continue
            elif key =="--run-tdengine":
                continue
            elif key == "--ignore-errors":
                continue
            else:
                args_list[key]=set_bool_value[random.randint(0,1)]
        return args_list

    def get_cmds(self, args_list):

        build_path = self.getBuildPath()
        crash_gen_path = build_path[:-5]+"tests/pytest/"
        bools_args_list = ["--auto-start-service" , "--debug","--run-tdengine","--ignore-errors","--track-memory-leaks","--larger-data","--mix-oos-data","--dynamic-db-table-names",
        "--per-thread-db-connection","--record-ops","--verify-data","--use-shadow-db","--continue-on-exception"]
        arguments = ""
        for k ,v in args_list.items():
            if k == "--ignore-errors":
                if v:
                    arguments+=(k+"="+str(v)+" ")
                else:
                    arguments+=""
            elif  k in bools_args_list and v==True:
                arguments+=(k+" ")
            elif k in bools_args_list and v==False:
                arguments+=""
            else:
                arguments+=(k+"="+str(v)+" ")
        crash_gen_cmd = 'cd %s && ./crash_gen.sh %s '%(crash_gen_path ,arguments)
        return crash_gen_cmd

    def run_crash_gen(self, crash_cmds,result_file):
        os.system("%s>%s 2>&1"%(crash_cmds,result_file))
        
    
    def check_error_of_result(self,result_file):

        run_code = subprocess.Popen("tail -n 10 %s"%result_file, shell=True, stdout=subprocess.PIPE,stderr=subprocess.STDOUT).stdout.read().decode("utf-8")
        if "Crash_Gen is now exiting with status code: 1" in run_code:
            return 1
        elif "Crash_Gen is now exiting with status code: 0" in run_code:
            return 0
        else:
            return 2 
    def check_coredump_exist(self, start_time ,end_time,core_dump_path):
        time_gap = int((end_time-start_time).seconds/60)  # limit to miniute
        before = subprocess.Popen("find %s -type f -mmin +%d -exec ls  {} \;"%(core_dump_path,time_gap), shell=True, stdout=subprocess.PIPE,stderr=subprocess.STDOUT).stdout.read().decode("utf-8")
        after = subprocess.Popen("find %s -type f -mmin +0 -exec ls  {} \;"%(core_dump_path), shell=True, stdout=subprocess.PIPE,stderr=subprocess.STDOUT).stdout.read().decode("utf-8")
        # str_2_list
        before_list = set()
        after_list = set()

        for line in before:
            before_list.add(line)
        for line in after:
            after_list.add(line)
        new_core_set = after_list - before_list
        return new_core_set

    def run(self):
        
        args_list = {"--auto-start-service":False ,"--max-dbs":0,"--connector-type":"native","--debug":False,"--run-tdengine":False,"--ignore-errors":[],
        "--num-replicas":1 ,"--track-memory-leaks":False , "--larger-data":False, "--mix-oos-data":False, "--dynamic-db-table-names":False,"--num-dnodes":1,
        "--per-thread-db-connection":False , "--record-ops":False , "--max-steps":100, "--num-threads":10, "--verify-data":False,"--use-shadow-db":False , 
        "--continue-on-exception":False }
        # this is an dir to back up crash_gen error ,it will back logs and data
        back_data_dir = "/data/wz/crash_gen_back/" 
        # please set coredump path  and the same with system core path
        core_dump_dir = "/data/coredump" 
        
        build_path = self.getBuildPath()
        crash_gen_path = build_path[:-5]+"tests/pytest/"
        taospy =  build_path[:-5]+"/src/connector/python"
        try:
            tdLog.notice("install new release taos connector for python! ")
            os.system("pip3 install %s " %(taospy))
        except Exception as e:
            print(" ======install taospy error occured===== ")
            print(e)


        if os.path.exists(crash_gen_path+"crash_gen.sh"):
            tdLog.info (" make sure crash_gen.sh is now ready")
        else:
            tdLog.notice( " crash_gen.sh is not exists ")
            sys.exit(1)
         
        # set random value:
        
        error_arguments_mix = 0
        if os.path.exists("./crash_gen_result.log"):
            os.remove("./crash_gen_result.log")
        with open("./crash_gen_result.log","a+") as logfiles:
            logfiles.write("*********************************** start auto run crash_gen and records results **********************************\n")
            logfiles.write("\n"*2)
            logfiles.write("*******************************************************************************************************************\n")
            logfiles.write("\n"*2)
            
            for i in range(100):
                start_time = datetime.datetime.now()
                print("now running %dth task: "%i)
                logfiles.write(" ====================================================== the %sth runing ======================================= \n"%i)

                args_list = self.set_random_value(args_list,run_tdengine = False)

                if args_list["--auto-start-service"]:
                    print(" auto prepare run envs ,it will clear envs such as stop taosd service")
                        # clear envs
                    os.system("ps -aux |grep 'taosd'  |awk '{print $2}'|xargs kill -9 >/dev/null 2>&1")
                    auto_start_path = build_path[:-5] +"debug/"
                    os.system("rm -rf %s/test*"%auto_start_path)
                    os.system("rm -rf %s/cluster*"%auto_start_path)
                    os.system("rm -rf %s/sim"%build_path[:-5])
                else:
                    auto_start_path = build_path[:-5] +"debug/"
                    os.system("ps -aux |grep 'taosd'  |awk '{print $2}'|xargs kill -9 >/dev/null 2>&1")
                    os.system("rm -rf %s/sim"%build_path[:-5])
                    os.system("rm -rf %s/test*"%auto_start_path)
                    os.system("rm -rf %s/cluster*"%auto_start_path)
                    tdDnodes.deploy(1,{})
                    tdLog.notice(" envs has read for single taosd service ")
                    self.start()
                    pass
                
                error_arguments_mix,flag = self.limit(args_list,error_arguments_mix)
                crash_gen_cmd = self.get_cmds(args_list)
                print(crash_gen_cmd)
                if flag :
                    dateString = time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime())
                    logfiles.write(" crash_gen is runing at time: %s \n"%dateString)
                    logfiles.write(" aruments now is : %s \n"%crash_gen_cmd)
                    logfiles.write(" arguments with some errors to run crash_gen , it will stop runing  \n")
                    continue
                
                dateString = time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime())
                logfiles.write(" crash_gen is runing at time: %s \n"%dateString)
                logfiles.write(" aruments now is : %s \n"%crash_gen_cmd)
                # ps_ef = subprocess.Popen("ps -ef | grep taosd", shell=True, stdout=subprocess.PIPE,stderr=subprocess.STDOUT).stdout.read().decode("utf-8")
                # print(ps_ef)
                log_path = build_path[:-5] +"debug/crash_gen_logs"
                if not os.path.exists(log_path):
                    os.mkdir(log_path)
                else:
                    shutil.rmtree(log_path, ignore_errors=True)
                    os.mkdir(log_path)

                result_file = "%s/result_%dth.log"%(log_path,i)
                if os.path.exists(result_file):
                    os.remove(result_file)
                self.run_crash_gen(crash_gen_cmd,result_file)
                run_code = self.check_error_of_result(result_file)
                end_time = datetime.datetime.now()
                core_set = self.check_coredump_exist(start_time ,end_time,core_dump_dir)
                
                if run_code>0 or core_set: # unexpected exit or coredump

                    if core_set:
                        tdLog.notice("  coredump occured as follow paths :" , core_set)

                    if args_list["--auto-start-service"]:
                            # set single taosd 
                        back_path = build_path[:-5] +"debug/test/"
                        
                        back_path_set = back_data_dir + dateString
                        if not os.path.exists(back_path_set):
                            os.mkdir(back_path_set)
                        # back files taosdlog and database data

                        if not os.path.exists(back_path_set):
                            os.mkdir(back_path_set)
                        # back files taosdlog and database data
                        os.system(" cp -r  %s %s"%(back_path,back_path_set))
                        if not os.path.exists(result_file):
                            tdLog.notice(" %s not exists " % result_file )
                        try:
                            tdLog.notice(" execute shell cmds : 'cp %s %s '  "%(result_file,back_path_set))
                            # shutil.copyfile(result_file,back_path_set)
                            os.system("cp -r %s %s")%(log_path,back_path_set)

                        except Exception as e :
                            pass
                        if core_set :
                            for core_file in core_set:
                                os.system(" cp -r  %s %s"%(core_file,back_path_set))
                                tdLog.notice(" coredump when run crash gen ,core  has back at %s "%back_path_set)
                                logfiles.write("============="*5)
                                logfiles.write(" coredump when run crash gen ,core  has back at %s \n"%back_path_set)
                                logfiles.write("============="*5)
                        else:
                            pass
                        tdLog.notice(" error expected run crash gen ,data and log  has back at %s "%back_path_set)
                        logfiles.write('================'*5)
                        logfiles.write(" error expected run crash gen ,data and log  has back at %s "%back_path_set)
                        logfiles.write('================'*5)
                        with open(back_path_set+"/crash_gen_cmds.txt","a+") as fcmds:
                            fcmds.write("=====the unexpected crash occured with follow crash_gen_cmds====== \n")
                            fcmds.write(crash_gen_cmd+"\n")
                            fcmds.write("==========="*5)
                        fcmds.close()

                        os.system("ps -aux |grep 'taosd'  |awk '{print $2}'|xargs kill -9 >/dev/null 2>&1")
                        auto_start_path = build_path[:-5] +"debug/"
                        os.system("rm -rf %s/test*"%auto_start_path)
                        os.system("rm -rf %s/cluster*"%auto_start_path)
                        os.system("rm %s"%result_file)
                    else:
                        back_path = build_path[:-5] +"sim/"
                        
                        back_path_set = back_data_dir + dateString+str(i)+"_th/"
                        if not os.path.exists(back_path_set):
                            os.mkdir(back_path_set)
                        # back files taosdlog and database data
                        os.system(" cp -r  %s %s"%(back_path,back_path_set))
                        if not os.path.exists(result_file):
                            tdLog.notice(" %s not exists " % result_file )
                        try:
                            tdLog.notice(" execute shell cmds : 'cp %s %s '  "%(result_file,back_path_set))
                            # shutil.copyfile(result_file,back_path_set)
                            os.system("cp -r %s %s")%(log_path,back_path_set)
                        except Exception as e :
                            pass
                        if core_set :
                            for core_file in core_set:
                                os.system(" cp -r  %s %s"%(core_file,back_path_set))
                                tdLog.notice(" coredump when run crash gen ,core  has back at %s "%back_path_set)
                                logfiles.write("============="*5)
                                logfiles.write(" coredump when run crash gen ,core  has back at %s \n"%back_path_set)
                                logfiles.write("============="*5)
                        else:
                            pass
                        tdLog.notice(" error expected run crash gen ,data and log  has back at %s "%back_path_set)
                        logfiles.write('================'*5)
                        logfiles.write(" error expected run crash gen ,data and log  has back at %s "%back_path_set)
                        logfiles.write('================'*5)
                        with open(back_path_set+"/crash_gen_cmds.txt","a+") as fcmds:
                            fcmds.write("=====the unexpected crash occured with follow crash_gen_cmds======\n ")
                            fcmds.write(crash_gen_cmd+"\n")
                            fcmds.write("==========="*5)
                        fcmds.close()

                        os.system("ps -aux |grep 'taosd'  |awk '{print $2}'|xargs kill -9 >/dev/null 2>&1")
                        auto_start_path = build_path[:-5] +"debug/"
                        os.system("rm -rf %s/test*"%auto_start_path)
                        os.system("rm -rf %s/cluster*"%auto_start_path)
                        os.system("rm %s"%result_file)
                else:
                    logfiles.write('================'*5)
                    logfiles.write(" crash_gen run done, and exit sucess as expected  ")
                    tdLog.notice(" crash_gen run done, and exit sucess as expected  ")
                    logfiles.write('================'*5)
                    os.system("ps -aux |grep 'taosd'  |awk '{print $2}'|xargs kill -9 >/dev/null 2>&1")
                    auto_start_path = build_path[:-5] +"debug/"
                    os.system("rm -rf %s/test*"%auto_start_path)
                    os.system("rm -rf %s/cluster*"%auto_start_path)
                    os.system("rm %s"%result_file)
                logfiles.flush()
                
            print("unrunning task number : ",error_arguments_mix)
        logfiles.close()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())

