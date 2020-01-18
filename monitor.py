#!/usr/bin/env python
#-*- coding=utf-8 -*-
#filename: monitor_dir.py
import os
import time
from mylog import spider_say

#monitor_dir = "E:/PUT_JT_DATA/The_BWT_Data/DayData/"
monitor_dir = "E:/PUT_JT_DATA/The_BWT_Data/DayRPT/"
now_file = dict([(f,None)for  f in os.listdir(monitor_dir)])
while True:
    new_file = dict([(f,None)for  f in os.listdir(monitor_dir)])
    added = [f for f in new_file if not f in now_file]
    removed = [f for f in now_file if not f in new_file]
    if added:
        spider_say ("\n Added: %s" %(",".join(added)))
    if removed:
        spider_say ("\n Removed:  %s" %(",".join(removed)))
    now_file = new_file
