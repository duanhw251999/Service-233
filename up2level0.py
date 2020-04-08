#!/usr/bin/env python
# -*-coding:utf-8 -*-
import os,Helper
import datetime
import logging
import gzip

paths={'BONC':'E:/PUT_JT_DATA/The_Temp_Data/The_BONC_Data/DAY/'
      ,'HJBD':'E:/PUT_JT_DATA/The_Temp_Data/The_HJBD_Data/DAY/'
      ,'SQMS':'E:/PUT_JT_DATA/The_Temp_Data/The_SQMS_Data/DAY/'
      }

errdir='E:/PUT_JT_DATA/The_Temp_Data/The_TEST_Data/err/'
remotedir='E:/PUT_JT_DATA/The_Temp_Data/The_TEST_Data/bak/'
recordfile='E:/PUT_JT_DATA/The_Temp_Data/The_TEST_Data/python0/'
#recordfile='D:/pythongram/'
uploaded='E:/PUT_JT_DATA/The_Temp_Data/The_TEST_Data/uploaded/'

#各单位约定数组
sqms=['DAPD_ENTITY_CLS'
,'DAPD_INST_MBL_MNTNCE'
,'DAPD_10000_NBR_SVC'
,'DAPD_CMPLN_DEAL_SVC'
,'DAPD_WEB_OFFICE'
,'DAPD_PALM_OFFICE'
,'DAPD_EXEC_SINGLE'
,'DAPD_NO_STSFTN_SINGLE']

bonc=['DAPD_CLS_SANDTABLE_SYS'
,'DAPD_WORKNG_SBT_OPERATION_MNTR'
,'DAPD_POS_OPERATION_MNTR'
,'DAPD_CREDIT_PHONE_HARASSMENT_V1'
,'DAPD_CREDIT_PHONE_CHEAT_V2']

hjbd=['DAPD_SVC_TRAIL'
,'DAPD_SVC_USER'
,'DAPD_SVC_ORD'
,'DAPD_CHNL_DCTNRY']


'''
循环遍历目录
'''
def loopdir():
    for k,v in paths.items():
        readdir(k,v)

'''
读取目录中所有文件
如果匹配到，说明是约定的文件,进行验证文件是否全
如果匹配不到就不是约定文件，移动到err目录
'''
def readdir(key,path):
    '''
        剔除干扰文件
        1.tmp文件
        2.已经上传过得文件
        3.行数为0的文件
    '''
    moveTmpFile(path)
    move_uploaded(path)
    moveZero(path)


    files = os.listdir(path)
    for f in files:
        suffix=os.path.splitext(f)[1]
        if suffix=='.CHECK':
            tname=f.split('.')[0]
            if isUnit(tname,key)==1:
                check=path+f
                if valid_file_isAll(check)==1:
                    upload(check)

'''
挪走已经上传的文件
'''
def move_uploaded(path):
    files = os.listdir(path)
    #2.挪走
    for f in files:
        if check_uploaded(f)==1:
            Helper.move(path+f,uploaded)
            spider_say(f + "-->" + uploaded)

'''
挪走tmp文件
'''
def moveTmpFile(path):
    files=os.listdir(path)
    for f in files:
        suffix = os.path.splitext(f)[1]
        if suffix == '.TMP':
            Helper.move(path+f,errdir)
            spider_say(f+"-->"+errdir)

'''
挪走文件记录数为0的文件
'''
def moveZero(path):
    count=0
    files = os.listdir(path)
    for f in files:
        suffix = os.path.splitext(f)[1]
        if suffix == '.gz':
            count = read_gz_file(path + f)
            if count==0:
                Helper.move(path + f, errdir)
                spider_say(f + "-->" + errdir)
        elif suffix=='.CHECK' or suffix=='.VAL':
            count = read_check_val_line(path + f)
            if count==0:
                Helper.move(path + f, errdir)
                spider_say(f + "-->" + errdir)
'''
上传
1.读取check
2.根据check找到gz和VAL文件
3.优先挪动gz和VAL文件
4.最后挪动check文件
'''
def upload(check):
    [dirname, filename] = os.path.split(check)
    with open(check, 'r') as fx:
        for line in fx:
            dat = dirname + '/' + line.rstrip() + '.gz'
            Helper.move(dat, remotedir)
            write_upload(os.path.split(dat)[1])

            val = dirname + '/' + (line.rstrip()).replace('.DAT', '.VAL')
            Helper.move(val, remotedir)
            write_upload(os.path.split(val)[1])

    Helper.move(check, remotedir)
    write_upload(os.path.split(filename)[1])

'''
验证文件是否全
'''
def valid_file_isAll(file):
    signal=1
    [dirname, filename] = os.path.split(file)
    suffix=os.path.splitext(file)[1]
    if suffix=='.CHECK':
        with open(file, 'r') as f:
            for line in f:
                dat=dirname+'/'+line.rstrip()+'.gz'
                val=dirname+'/'+(line.rstrip()).replace('.DAT','.VAL')
                if Helper.isExist(dat)==False:
                    signal=0
                    break
                if Helper.isExist(val)==False:
                    signal=0
                    break
    return signal

'''
 写入文件
'''
def write_upload(filename):
    record_path = recordfile + "record_" + current_month() + "_upload.log"
    with open(record_path, 'a+') as f:
        f.write(filename+"\n")


'''
  检查文件是否已经上传 
  0 未上传过，可以上传 
  1 上传过，不能上传
'''
def check_uploaded(filename):
    flag=0
    record_path=recordfile+"record_"+current_month()+"_upload.log"
    if Helper.isExist(record_path)==False:
        with open(record_path, 'a+') as f:
            pass
    with open(record_path, 'r') as f:
        for line in f:
            if filename == line.rstrip():
                flag = 1
                break
    return flag

'''
  获取当前月份
'''
def current_month():
    now_time = datetime.datetime.now()
    cur_mon=now_time.strftime('%Y%m')
    return cur_mon

'''
根据文件的名称到各厂家数组当中匹配，
'''
def isUnit(tablename,key):
    signal=0
    if key=='BONC':
        if tablename in bonc:
            signal=1
    elif key=='HJBD' :
        if tablename in hjbd:
            signal=1
    elif key == 'SQMS' :
        if tablename in sqms:
            signal = 1
    else:
        signal=0
    return signal

'''
日志
'''
def spider_say(msg):
    LOG_FORMAT = "%(process)d - %(asctime)s - %(levelname)s - %(message)s"
    logname='./up2level0.log'
    logging.basicConfig(filename=logname,filemode='a', level=logging.DEBUG, format=LOG_FORMAT)
    logging.info(msg)
'''
读取gz的行数
'''
def read_gz_file(path):
    count=0
    if os.path.exists(path):
        with gzip.open(path, 'r') as pf:
            for line in pf:
                count+=1
    else:
        print('the path [{}] is not exist!'.format(path))
    return count

'''
读取check val文件行数
'''
def read_check_val_line(path):
    count = 0
    with open(path, 'r') as fx:
        for line in fx:
            count += 1
    return count

if __name__ == '__main__':
    loopdir()
