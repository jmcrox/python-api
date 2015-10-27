# -*- coding: utf8 -*-
'''
 Python SSAP API
 Version 1.5
 
 © Indra Sistemas, S.A.
 2014  SPAIN
  
 All rights reserved
'''
import unittest
from ssap.core import SSAP_QUERY_TYPE
from ssap.factories import SSAPEndpointFactory
from ssap.tests.utils.callbacks import TestCallback

class TestSubscribes(unittest.TestCase):
    
    ONTOLOGY = "TestSensorTemperatura"
    TOKEN = "e5e8a005d0a248f1ad2cd60a821e6838"
    INSTANCE = "KPTestTemperatura:KPTestTemperatura01"
    SQLLIKE_SUBSCRIBE = "SELECT * FROM TestSensorTemperatura WHERE TestSensorTemperatura.assetId = \"S_Temperatura_00066\""
    NATIVE_SUBSCRIBE = "db.TestSensorTemperatura.find()"
    CEP_RULE = "API_CEP_EVENTS"
    
    def setUp(self):
        self.__serverURL = 'ws://sofia2.com/sib/api_websocket'
        self.__callback = TestCallback(True)
        self.__doJoin()

    def tearDown(self):
        self.__doLeave()
    
    def buildJsonObject(self):
        jsonObject = {}
        jsonObject["Sensor"] = {}
        jsonObject["Sensor"]["geometry"] = {}
        jsonObject["Sensor"]["geometry"]["coordinates"] = [ 40.512967, -3.67495 ]
        jsonObject["Sensor"]["geometry"]["type"] = "Point"
        jsonObject["Sensor"]["assetId"] = "S_Temperatura_00066"
        jsonObject["Sensor"]["measure"] = 10
        jsonObject["Sensor"]["timestamp"] = {"$date" : "2014-04-29T08:24:54.005Z"}
        return jsonObject
    
    def __doJoin(self):
        self.__endpoint = SSAPEndpointFactory.buildWebsocketBasedSSAPEndpoint(self.__serverURL, self.__callback, True)
        self.__callback.prepareToReceiveSsapResponse()
        self.__endpoint.joinWithToken(TestSubscribes.TOKEN, TestSubscribes.INSTANCE)
        self.__callback.waitForSsapResponse()
        self.assertTrue(self.__callback.isSsapResponseOk())
    
    def __doLeave(self):
        self.__callback.prepareToReceiveSsapResponse()
        self.__endpoint.leave()
        self.__callback.waitForSsapResponse()
        self.assertTrue(self.__callback.isSsapResponseOk())
        
    def testSuccessfulNativeSubscribeUnsubscribe(self):
        self.__callback.prepareToReceiveSsapResponse()
        self.__endpoint.subscribe(TestSubscribes.ONTOLOGY, TestSubscribes.NATIVE_SUBSCRIBE, SSAP_QUERY_TYPE.NATIVE, 100)
        self.__callback.waitForSsapResponse()
        self.assertTrue(self.__callback.isSsapResponseOk())
        self.__callback.prepareToReceiveSsapResponse()
        self.__endpoint.unsubscribe(self.__callback.getSubscriptionId())
        self.__callback.waitForSsapResponse()
        self.assertTrue(self.__callback.isSsapResponseOk())
          
    def testUnsuccessfulNativeSubscribeUnsubscribe(self):
        self.__callback.prepareToReceiveSsapResponse()
        self.__endpoint.subscribe(TestSubscribes.ONTOLOGY, "wrong query", SSAP_QUERY_TYPE.NATIVE, 100)
        self.__callback.waitForSsapResponse()
        self.assertFalse(self.__callback.isSsapResponseOk())
         
    def testIndication(self):
        self.__callback.prepareToReceiveSsapResponse()
        self.__endpoint.subscribe(TestSubscribes.ONTOLOGY, TestSubscribes.NATIVE_SUBSCRIBE, SSAP_QUERY_TYPE.NATIVE, 100)
        self.__callback.waitForSsapResponse()
        self.assertTrue(self.__callback.isSsapResponseOk())
        self.__callback.prepareToReceiveSsapResponse()
        self.__endpoint.insert(TestSubscribes.ONTOLOGY, self.buildJsonObject())
        self.__callback.waitForSsapIndication()
        self.assertTrue(self.__callback.wasIndicationReceived())
        self.__callback.prepareToReceiveSsapResponse()
        self.__endpoint.unsubscribe(self.__callback.getSubscriptionId())
        self.__callback.waitForSsapResponse()
        self.assertTrue(self.__callback.isSsapResponseOk())
  
    def testSuccessfulSqlLikeSubscribeUnsubscribe(self):
        self.__callback.prepareToReceiveSsapResponse()
        self.__endpoint.subscribe(TestSubscribes.ONTOLOGY, TestSubscribes.SQLLIKE_SUBSCRIBE, SSAP_QUERY_TYPE.SQLLIKE, 100)
        self.__callback.waitForSsapResponse()
        self.assertTrue(self.__callback.isSsapResponseOk())
        self.__callback.prepareToReceiveSsapResponse()
        self.__endpoint.unsubscribe(self.__callback.getSubscriptionId())
        self.__callback.waitForSsapResponse()
        self.assertTrue(self.__callback.isSsapResponseOk())
           
    def testUnsuccessfulSqlLikeSubscribeUnsubscribe(self):
        self.__callback.prepareToReceiveSsapResponse()
        self.__endpoint.subscribe(TestSubscribes.ONTOLOGY, "wrong query", SSAP_QUERY_TYPE.SQLLIKE, 100)
        self.__callback.waitForSsapResponse()
        self.assertFalse(self.__callback.isSsapResponseOk())
        
#     def testCepRuleSubscribe(self):
#         self.__callback.prepareToReceiveSsapResponse()
#         self.__endpoint.subscribe(TestSubscribes.ONTOLOGY, TestSubscribes.CEP_RULE, SSAP_QUERY_TYPE.CEP, 100)
#         self.__callback.waitForSsapResponse()
#         self.assertTrue(self.__callback.isSsapResponseOk())
#         self.__callback.prepareToReceiveSsapResponse()
#         self.__endpoint.insert(TestSubscribes.ONTOLOGY, self.buildJsonObject())
#         self.__callback.waitForSsapIndication()
#         self.assertTrue(self.__callback.wasIndicationReceived())
#         self.__callback.prepareToReceiveSsapResponse()
#         self.__endpoint.unsubscribe(self.__callback.getSubscriptionId())
#         self.__callback.waitForSsapResponse()
#         self.assertTrue(self.__callback.isSsapResponseOk())
        
if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
