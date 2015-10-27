# -*- coding: utf8 -*-
'''
 Python SSAP API
 Version 1.5
 
 © Indra Sistemas, S.A.
 2014  SPAIN
  
 All rights reserved
'''
import logging    
import sys

class LogFactory(object):
    '''
    This class configures a Python logger.
    '''
    __line_pattern = '%(asctime)s %(name)s [%(levelname)s] - %(message)s'       

    DEFAULT_LOG_FILE = ""
   
    @staticmethod
    def __getLogger(obj, level):
        '''
        Creates and returns a logger. The logger will be named after the class that uses it.
        
        Keyword arguments:
        obj     -- an instance of the class that will use the logger.
        level   -- the minimum logging level (i.e. "DEBUG", "INFO",...).
        '''
        logger = logging.getLogger(type(obj).__name__)
        logger.setLevel(level)
        return logger

    @staticmethod
    def __getHandler(output_file=""):
        '''
        Configures a logging handler.
        
        Keyword arguments:
        output_file     -- the output file that the logger will use.
        '''
        if (len(output_file) != 0) :
            handler = logging.FileHandler(output_file)
        else:
            handler = logging.StreamHandler(sys.stdout)    
        handler.setFormatter(logging.Formatter(LogFactory.__line_pattern))
        return handler
    
    @staticmethod
    def configureLogger(obj, level, logfile):
        '''
        Configures a logger
        
        Keyword arguments:
        obj         -- the object that will use the logger.
        level       -- the minimum logging level (i.e. "DEBUG", "INFO",...).
        logfile     -- the output file that the logger will use.
        '''
        logger = LogFactory.__getLogger(obj, level)
        logger.addHandler(LogFactory.__getHandler(logfile))
        return logger