#!/usr/bin/env python
# -*-coding:utf-8 -*-

'''
日期: 2019-04-21
作者: 段宏伟
'''

class Fileinfo(object):
    __datName=""#数据文件名称
    __size=""#数据文件大小
    __row=""#数据文件行数
    __datDate="" #数据文件日期
    __time="" #数据文件时间戳

    def getDatName(self):
        return self.__datName
    def setDatName(self,datName):
        self.__datName=datName

    def getSize(self):
        return self.__size
    def setSize(self,size):
        self.__size=size

    def getRow(self):
        return self.__row
    def setRow(self,row):
        self.__row=row

    def getDatDate(self):
        return self.__datDate
    def setDatDate(self,datDate):
        self.__datDate=datDate

    def getTime(self):
        return self.__time
    def setTime(self,time):
        self.__time=time