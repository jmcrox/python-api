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

class TestUpdates(unittest.TestCase):
    
    ONTOLOGY = "TestSensorTemperatura"
    TOKEN = "e5e8a005d0a248f1ad2cd60a821e6838"
    INSTANCE = "KPTestTemperatura:KPTestTemperatura01"
    NATIVE_UPDATE = "{\"Sensor\": { \"geometry\": { \"coordinates\": [ 40.512967, -3.67495 ], \"type\": \"Point\" }, \"assetId\": \"S_Temperatura_00066\", \"measure\": 20, \"timestamp\": { \"$date\": \"2014-04-29T08:24:54.005Z\"}}}"
    NATIVE_UPDATE_QUERY = "{Sensor.assetId:\"S_Temperatura_00066\"}"
    SQLLIKE_UPDATE = "UPDATE TestSensorTemperatura SET TestSensorTemperatura.assetId=\"S_Temperatura_00066\" WHERE TestSensorTemperatura.assetId=\"S_Temperatura_00066\""

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
        self.__endpoint.joinWithToken(TestUpdates.TOKEN, TestUpdates.INSTANCE)
        self.__callback.waitForSsapResponse()
        self.assertTrue(self.__callback.isSsapResponseOk())
    
    def __doLeave(self):
        self.__callback.prepareToReceiveSsapResponse()
        self.__endpoint.leave()
        self.__callback.waitForSsapResponse()
        self.assertTrue(self.__callback.isSsapResponseOk())
        
    def testSuccessfulNativeUpdate(self):
        self.__callback.prepareToReceiveSsapResponse()
        self.__endpoint.update(TestUpdates.ONTOLOGY, "", TestUpdates.NATIVE_UPDATE)
        self.__callback.waitForSsapResponse()
        self.assertTrue(self.__callback.isSsapResponseOk())
        
    def testSuccessfulNativeUpdateWithQuery(self):
        self.__callback.prepareToReceiveSsapResponse()
        self.__endpoint.update(TestUpdates.ONTOLOGY, TestUpdates.NATIVE_UPDATE_QUERY, TestUpdates.NATIVE_UPDATE)
        self.__callback.waitForSsapResponse()
        self.assertTrue(self.__callback.isSsapResponseOk())
        
    def testSuccessfulSqlLikeUpdate(self):
        self.__callback.prepareToReceiveSsapResponse()
        self.__endpoint.update(TestUpdates.ONTOLOGY, TestUpdates.SQLLIKE_UPDATE, "", SSAP_QUERY_TYPE.SQLLIKE)
        self.__callback.waitForSsapResponse()
        self.assertTrue(self.__callback.isSsapResponseOk())
        
    def testUnsuccessfulSqlLikeUpdate(self):
        self.__callback.prepareToReceiveSsapResponse()
        self.__endpoint.update(TestUpdates.ONTOLOGY, "", "", SSAP_QUERY_TYPE.SQLLIKE)
        self.__callback.waitForSsapResponse()
        self.assertFalse(self.__callback.isSsapResponseOk())
    
if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
