#!/usr/bin/env python
# -*-coding:utf-8 -*-

import ftplib,os,Helper
import re
import time
import logging
from datetime import datetime
from mylog import spider_say
import security0 as sec0

pd=sec0.decryption("de7093ef1f23cd55d6694826e4307fe9","bigdata_jk")
conStr={
"host":"135.148.9.50",
"user":"bigdata_jk",
"password":pd
}


paths={'day':'/data02/jkfiles/ods_jk/bwt_day/',
       'mon':'/data02/jkfiles/ods_jk/bwt_mon/',
       'local_day':'E:/PUT_JT_DATA/The_BWT_Data/DayData_BAK/',
       'local_mon':'E:/PUT_JT_DATA/The_BWT_Data/MonthData_BAK/'
       }

def ftpconnect():
    ftpobj=None
    try:
        ftpobj=ftplib.FTP(conStr['host'])
        ftpobj.login(conStr['user'],conStr['password'])
        msg=ftpobj.getwelcome()
        spider_say(msg)
    except Exception as e:
        spider_say(e)
    return ftpobj

'''
ftpobj FTP对象
file_remote 远程名称
file_local 本地文件绝对路径
'''
def ftp_upload(ftpobj,file_remote,file_local,dtype):
    '''以二进制形式上传文件'''
    if ftpobj is not None:
        #获取当前路径
        if dtype=='local_day':
            ftpobj.cwd(paths["day"])
        elif dtype=='local_mon':
            ftpobj.cwd(paths["mon"])
        bufsize = 1024  # 设置缓冲器大小
        fp = open(file_local, 'rb')
        ftpobj.storbinary('STOR ' + file_remote, fp, bufsize)
        ftpobj.set_debuglevel(0)
        fp.close()

#扫描生成目录
def scan_file(path,ftpobj,dtype):
    log="bigdata_day"+Helper.dateNow(0)+".log"
    files=fileValid(path,dtype)
    for file in files:
        absPath=path+file
        spider_say(absPath)
        if read_log(file,log)==True:
            write_log(file,log)
            ftp_upload(ftpobj,file,absPath,dtype)

'''
#在扫描临时文件夹，将文件上传后放入备份文件
def backup_file(path,ftpobj):
    log="local_backup_"+Helper.dateNow(0)+".log"
    files=fileValid(path)
    for file in files:
        absPath=path+file
        if read_log(file,log)==True:
            write_log(file,log)
            bakDir=paths["backup"]+Helper.dateNow(0)
            if os.path.exists(bakDir)==False:
                os.mkdir(bakDir)
            ftp_upload(ftpobj,file,absPath)
            Helper.move(absPath,bakDir)
'''

#文件检查
# path 目录
# dtype: local_day and local_mon
def fileValid(path,dtype):
    fileStr=[]
    files=os.listdir(path)
    if len(files)!=0:
        for file in files:
            absPath=path+file
            if os.path.isfile(absPath)==True:
                if Helper.get_FileSize(absPath)>0:
                    if re.match('^.*?\.(CHECK|VAL|gz)$',file) is not None:
                        if dtype=='local_day':
                            if dateValid(file)==True:
                                fileStr.append(file.strip())
                        elif dtype=='local_mon':
                            if datetime.now().day==12:
                                if dateValid2(file)==True:
                                    fileStr.append(file.strip())
    return fileStr


#写入日志
def write_log(file,log):
    with open(log,'a+') as f:
        f.write(file+"\n")
#读取日志
def read_log(file,log):
    flag=True
    readStr=[]
    if Helper.isExist(log)==False:
        f=open(log,"w")
        f.close()

    with open(log,'r') as f:
        line=f.readline()
        while line:
            readStr.append(line.strip())
            line=f.readline()
    if len(readStr)>0:
        for x in readStr:
            if file==x:
                flag=False
                break
    return flag

#对日表校验
def dateValid(file):
    d1=file.split(".")[1]
    d2=file.split(".")[2]
    d3=Helper.dateNow(0)

    flag=False
    if Helper.dateOper(d3,d1)==0 and Helper.dateOper(d3,d2)==1:
        flag=True
    elif Helper.dateOper(d3,d1)==1 and Helper.dateOper(d3,d2)==1:
        flag=True
    elif Helper.dateOper(d3,d1)==0 and Helper.dateOper(d3,d2)==2:
        flag=True
    return flag

#对月表校验
def dateValid2(file):
    flag=False
    m1=file.split(".")[2]
    m2=Helper.monthNow(-1)
    if m1==m2:
        flag=True    
    return flag

def spider_say(msg):
    LOG_FORMAT = "%(process)d - %(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(filename="bigdata.log",filemode='a', level=logging.DEBUG, format=LOG_FORMAT)
    logging.info(msg)

if __name__ == '__main__':   
    try:
        while True:
            print('%s 进程号 %s ... ' % ("代号:蜘蛛", os.getpid()))
            print("==================================================开始执行大数据日/月表上传")
            ftpobj=ftpconnect()
            scan_file(paths["local_day"],ftpobj,'local_day')
            scan_file(paths["local_mon"],ftpobj,'local_mon')
            ftpobj.quit()
            time.sleep(2*60*60)
            print("==================================================结束大数据日/月表上传")
    except Exception as e:
        spider_say(e)

