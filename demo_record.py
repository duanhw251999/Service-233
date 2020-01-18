#/usr/bin/env python
# -*- coding:utf-8 -*-

import os

#写入记录文件
def write2log(str,abspath):
     if read2log(str,abspath)==0:
        with open(abspath,"a+") as f:
            f.write(str+"\n")


#读取记录文件中的记录
def read2log(str,abspath):
    signal=0
    if os.path.exists(abspath)==False:
        open(abspath,"w")

    with open(abspath,"r") as f:
        for l in f.readlines():
            if str==l.strip():
                signal=1
                break
    return signal

def write2log_m(str,abspath,mode='a+'):
     with open(abspath,mode) as f:
          f.write(str+'\n')


if __name__ == '__main__':
    pass


