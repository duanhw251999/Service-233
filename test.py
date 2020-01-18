#!/usr/bin/env python
# -*-coding:utf-8 -*-

import hashlib

str0="duanhw"

h1=hashlib.md5()

h1.update(str0.encode(encoding='utf-8'))

print(h1.hexdigest())
