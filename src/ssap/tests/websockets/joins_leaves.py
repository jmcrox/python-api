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
from ssap.exceptions import InvalidSSAPOperation

class TestJoinsAndLeaves(unittest.TestCase):
    
    TOKEN = "3f8d3638215a4d59ad5cffad19c81379"
    INSTANCE = "KPTestTemperatura:KPTestTemperatura01"

    def setUp(self):
        self.__serverURL = 'ws://sofia2.com/sib/api_websocket'
        self.__callback = TestCallback()
        self.__endpoint = SSAPEndpointFactory.buildWebsocketBasedSSAPEndpoint(self.__serverURL, self.__callback, True)
        
    def tearDown(self):
        pass

    def testSuccessfulJoinAndLeave(self):
        self.__callback.prepareToReceiveSsapResponse()
        self.__endpoint.joinWithToken(TestJoinsAndLeaves.TOKEN, TestJoinsAndLeaves.INSTANCE)
        self.__callback.waitForSsapResponse()
        self.assertTrue(self.__callback.isSsapResponseOk())
        self.__callback.prepareToReceiveSsapResponse()
        self.__endpoint.leave()
        self.__callback.waitForSsapResponse()
        self.assertTrue(self.__callback.isSsapResponseOk())
    
    def testUnsuccessfulJoin(self):
        self.__callback.prepareToReceiveSsapResponse()
        self.__endpoint.joinWithToken("token inválido", TestJoinsAndLeaves.INSTANCE)
        self.__callback.waitForSsapResponse()
        self.assertFalse(self.__callback.isSsapResponseOk())
        
    def testInvalidLeave(self):
        self.__callback.prepareToReceiveSsapResponse()
        self.assertRaises(InvalidSSAPOperation, self.__endpoint.leave) 
        
    def testRenovateSessionKey(self):
        self.__callback.prepareToReceiveSsapResponse()
        self.__endpoint.joinWithToken(TestJoinsAndLeaves.TOKEN, TestJoinsAndLeaves.INSTANCE)
        self.__callback.waitForSsapResponse()
        self.assertTrue(self.__callback.isSsapResponseOk())
        self.__callback.prepareToReceiveSsapResponse()
        self.__endpoint.renovateSessionKey()
        self.__callback.waitForSsapResponse()
        self.assertTrue(self.__callback.isSsapResponseOk())
        self.__endpoint.leave()
        self.__callback.waitForSsapResponse()
        self.assertTrue(self.__callback.isSsapResponseOk())

if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
