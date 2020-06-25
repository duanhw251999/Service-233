#!/usr/bin/env python
# -*-coding:utf-8 -*-

'''
日期: 2019-04-21
作者: 段宏伟
'''

import os,shutil
import jt
from datetime import datetime,timedelta
import time
import re
from mylog import spider_say
from dateutil.relativedelta import relativedelta

'''
   查找文件是否存在
'''
def isExist(path):
    boolean=True
    [dirname,filename]=os.path.split(path)
    if os.path.exists(path)==True:
        spider_say("%s文件存在" % (filename))
    else:
        spider_say("%s文件不存在" % (filename))
        boolean=False
    return boolean

'''
@file 文件名称 带路径
@toPath 将要移动的目录
'''
def move(file,toPath):
    [dirname,filename]=os.path.split(file)

    if not os.path.isfile(file):
        spider_say("%s 不是一个文件" %(file))
    else:
        if not os.path.exists(toPath):
            os.mkdir(toPath)
        if os.path.exists(toPath+filename)==False:
            spider_say("%s-->%s" %(file,shutil.move(file,toPath)))
        else:
            spider_say("%s 文件已经存在，不能再传" % (toPath+filename))


# 字节数
def get_FileSize(filePath):
    fsize = os.path.getsize(filePath)
    return round(fsize,3)


# 获取文件大小
def getDocSize(path):
    try:
        size = os.path.getsize(path)
        spider_say( formatSize(size))
    except Exception as err:
        spider_say(err)

# 字节bytes转化kb\m\g
def formatSize(bytes):
    try:
        bytes = float(bytes)
        kb = bytes / 1024
    except:
        spider_say("传入的字节格式不对")
        return "Error"

    if kb >= 1024:
        M = kb / 1024
        if M >= 1024:
            G = M / 1024
            return "%fG" % (G)
        else:
            return "%fM" % (M)
    else:
        return "%fkb" % (kb)


'''
日期加减操作
cdate 创建日期
ddate  数据账期
'''
def dateOper(cdate,ddate):
    d1 = datetime.strptime(cdate, '%Y%m%d')
    d2 = datetime.strptime(ddate, '%Y%m%d')
    day = d1 - d2
    return day.days

'''
 获得当前日期的账期
 offset -1 前一天  0 当天  1 后一天
'''
def dateNow(offset):
    now=(datetime.now()+timedelta(days=offset)).strftime('%Y%m%d')
    return now

'''
  获取月份
'''
def monthNow(offset):
    now=(datetime.today() + relativedelta(months=offset)).strftime('%Y%m')
    return now

# |*日文件共 19 个 当前已上传 2 个
# |---DAPD_DIM_MAPPING_PD 已上传 共(3)个文件 CHECK(1); VAL(1);DAT(1);
# |------DAPD_DIM_MAPPING_PD.20190318.20190317.00.000.000.862.CHECK
# |------DAPD_DIM_MAPPING_PD.20190318.20190317.00.001.001.862.VAL
# |------DAPD_DIM_MAPPING_PD.20190318.20190317.00.001.001.862.DAT.gz
def log():
    day=jt.getDAPX("day")
    spider_say("|*日文件共%d个 当前已上传%d" %(len(day),2))

'''
  dirName 需要检查的目录
  errorDir 错误目录
'''
def deleteEmpty(dirName,errorDir):
    files=os.listdir(dirName)
    for file in files:
        absPath=dirName+file
        spider_say("Helperl.deleteEmpty>>>%s" % absPath)
        if isExist(absPath)==True:
            if get_FileSize(absPath)==0:
                spider_say("%s 是空文件，文件大小为0，被移动到错误目录..."%file)
                move(absPath,errorDir)

def notConf(dirName,errorDir):
    files=os.listdir(dirName)
    for file in files:
        absPath=dirName+file
        datName=file.split(".")[0]
        spider_say("Helperl.notConf>>>%s"%absPath)
        if datName not in jt.getDAPX("day"):
        	  if datName not in jt.getDAPX("hs_data"):
            		spider_say("%s 并不属于配置中的文件，可能是错误文件，将被移动到错误目录..."%file)
            		move(absPath,errorDir)

def formatError(dirName,errorDir):
    files=os.listdir(dirName)
    for file in files:
        absPath=dirName+file
        spider_say("Helperl.formatError>>>%s" % absPath)
        dn=file.split(".")[0]
        if re.match("^"+dn+"([.]\d{8}){2}([.]\d{2,3})+\d+\.(CHECK|VAL|DAT)(\.gz)?$",file) is None:
            move(absPath,errorDir)

'''打印对象的属性值'''
def prn_obj(obj):
  spider_say("----------------------------------------------------------------")
  spider_say ('\n'.join(['%s:%s' % item for item in obj.__dict__.items()]))
  spider_say("----------------------------------------------------------------")

#返回文件创建时间
def getFileCreateTime(absPath):
    t = os.path.getctime(absPath)
    return TimeStampToTime(t)

#返回文件最后访问时间
def getFileCreateTime(absPath):
    t = os.path.getatime(absPath)
    return TimeStampToTime(t)

#返回文件最后修改时间
def get_FileModifyTime(absPath):
    t = os.path.getmtime(absPath)
    return TimeStampToTime(t) #

'''把时间戳转化为时间: 1479264792 to 2016-11-16 10:53:12'''
def TimeStampToTime(timestamp):
    timeStruct = time.localtime(timestamp)
    return time.strftime('%Y%m%d',timeStruct)#return time.strftime('%Y-%m-%d %H:%M:%S',timeStruct)

#时间点相等
def timeEque(t2):
    t1=time.strftime('%H%M',time.localtime())
    return t1==t2

#获取文件后缀名
def suffix(file):
    sname=os.path.splitext(file)
    return sname

if __name__ == '__main__':
    pass
