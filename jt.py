#!/usr/bin/env python
# -*-coding:utf-8 -*-

'''
日期: 2019-04-21
作者: 段宏伟
'''

import json
path="conf.json"
def getDAPX(key):
     with open(path,"r") as f:
         jsonObj=json.load(f)
         return jsonObj[key]

if __name__ == '__main__':
    day=getDAPX("hs_data")
    for d in day:
        print(d)