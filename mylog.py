#!/usr/bin/env python
# -*-coding:utf-8 -*-

import logging
import sys

def spider_say(msg):
    LOG_FORMAT = "%(process)d - %(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(filename='my.log',filemode='a', level=logging.DEBUG, format=LOG_FORMAT)
    logging.info(msg)


if __name__ == '__main__':
    spider_say("你好")