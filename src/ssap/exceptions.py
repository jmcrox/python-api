# -*- coding: utf8 -*-
'''
 Python SSAP API
 Version 1.5
 
 © Indra Sistemas, S.A.
 2014  SPAIN
  
 All rights reserved
'''
class InvalidSSAPOperation(Exception):
    '''
    Exception class for invalid SSAP operations (for example, an INSERT without a previous JOIN).
    '''
    pass
        
class SSAPConnectionError(Exception):
    '''
    Exception class for connection errors.
    '''
    pass

class InvalidSSAPCallback(Exception):
    '''
    Exception class for SSAP callback configuration errors.
    '''
    pass
        