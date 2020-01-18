#!/usr/bin/env python
# -*- coding:utf-8 -*-
import re
import os
from class_jt import Fileinfo
import jt
import Helper,time
from mylog import spider_say
from demo_record import write2log_m
from datetime import datetime
#回执文件的查找
def checkRPT(path,*dM):   
    msg=''
    msg+=(str(datetime.now())+"现在开始检查"+dM[0]+"回执文件......\n")
    spider_say(msg)
    
    narmal={}
    files=os.listdir(path)
    if len(files)!=0:
        for file in files:
            absPath=path+file
            if re.match("\w*([.]\d+)+([.]\d+)+([.]\w+)+\.(RPT|ERR)$",file)is not None: #只搜索rpt和err文件
                if dM[0]=='day':
                    if Helper.dateOper(Helper.getFileCreateTime(absPath),Helper.dateNow(0))==0:#只搜索当前日期的回执文件
                        if file.split(".")[0] in jt.getDAPX("day"):#只搜索与属于配置文件的
                            if file.split(".")[0] not in narmal.keys():
                                if os.path.splitext(file)[1]==".RPT":
                                    narmal[file.split(".")[0]]="正常"
                                else:
                                    narmal[file.split(".")[0]]="异常"
                elif dM[0]=='month':
                        if file.split(".")[2]==Helper.monthNow(-1):#只搜索M-1的回执文件
                            if file.split(".")[0] in jt.getDAPX("month"):#只搜索与属于配置文件的
                                if file.split(".")[0] not in narmal.keys():
                                    if os.path.splitext(file)[1]==".RPT":
                                        narmal[file]="正常"
                                    else:
                                        narmal[file]="异常"
    for (key,value) in narmal.items():
        str1="{key}:{value}".format(key=key,value=value)
        spider_say(str1)
        msg+=str1+'\n'

    ncount=0
    fcount=0
    for v in narmal.values():
        if v=="正常":
            ncount=ncount+1
        if v=="异常":
            fcount=fcount+1
    str2=str(datetime.now())+"当前收到%d个回执文件，其中正常[%d],异常[%d]"%(len(narmal),ncount,fcount)
    spider_say(str2)
    msg+=str2+'\n'
    write2log_m(msg,"check.log")

def check_day():
    checkRPT('E:/PUT_JT_DATA/The_BWT_Data/DayRPT/','day')
def check_month():
    checkRPT('E:/PUT_JT_DATA/The_BWT_Data/MonthRPT/','month')

if __name__=='__main__':
    try:
        while True:
            time.sleep(10*60)
            check_day()
            check_month()
    except Exception  as e:
        spider_say(e)

