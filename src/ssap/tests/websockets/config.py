# -*- coding: utf8 -*-
'''
 Python SSAP API
 Version 1.5
 
 © Indra Sistemas, S.A.
 2014  SPAIN
  
 All rights reserved
'''
import unittest
from ssap.factories import SSAPEndpointFactory
from ssap.tests.utils.callbacks import TestCallback

class TestConfig(unittest.TestCase):
    
    KP = "KPTestTemperatura"
    INSTANCE = "KPTestTemperatura01"
    TOKEN = "e5e8a005d0a248f1ad2cd60a821e6838"
    ASSET_SERVICE_PARAMS = {}
    ASSET_SERVICE = "testConfig"

    def setUp(self):
        self.__serverURL = 'ws://sofia2.com/sib/api_websocket'        
        self.__callback = TestCallback()
        self.__endpoint = SSAPEndpointFactory.buildWebsocketBasedSSAPEndpoint(self.__serverURL, self.__callback, True)
        
    def tearDown(self):
        pass

    def testConfig(self):        
        self.__callback.prepareToReceiveSsapResponse()
        self.__endpoint.config(TestConfig.KP, TestConfig.INSTANCE, TestConfig.TOKEN, TestConfig.ASSET_SERVICE, TestConfig.ASSET_SERVICE_PARAMS)
        self.__callback.waitForSsapResponse()
        self.assertFalse(self.__callback.isSsapResponseOk())

if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
