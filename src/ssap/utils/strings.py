# -*- coding: utf8 -*-
'''
 Python SSAP API
 Version 1.5
 
 © Indra Sistemas, S.A.
 2014  SPAIN
  
 All rights reserved
'''

import sys

def bytes2String(data):
    '''
    Converts a Python 3 bytes object to a string.
    '''
    if sys.version_info[0] < 3:
        return data
    else:
        return data.decode("utf-8")
        