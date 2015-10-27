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

class TestInserts(unittest.TestCase):
    
    ONTOLOGY = "TestSensorTemperatura"
    TOKEN = "e5e8a005d0a248f1ad2cd60a821e6838"
    INSTANCE = "KPTestTemperatura:KPTestTemperatura01"
    ONTOLOGY_INSERT_SQLLIKE = "INSERT INTO TestSensorTemperatura(geometry, assetId, measure, timestamp) values (\"{ 'coordinates': [ 40.512967, -3.67495 ], 'type': 'Point' }\", \"S_Temperatura_00066\", 15, \"{ '$date': '2014-04-29T08:24:54.005Z'}\")";

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
        self.__endpoint.joinWithToken(TestInserts.TOKEN, TestInserts.INSTANCE)
        self.__callback.waitForSsapResponse()
        self.assertTrue(self.__callback.isSsapResponseOk())
    
    def __doLeave(self):
        self.__callback.prepareToReceiveSsapResponse()
        self.__endpoint.leave()
        self.__callback.waitForSsapResponse()
        self.assertTrue(self.__callback.isSsapResponseOk())

    def testSuccessfulNativeInsert(self):
        self.__callback.prepareToReceiveSsapResponse()
        self.__endpoint.insert(TestInserts.ONTOLOGY, self.buildJsonObject())
        self.__callback.waitForSsapResponse()
        self.assertTrue(self.__callback.isSsapResponseOk())
        
    def testUnsuccessfulNativeInsert(self):
        self.__callback.prepareToReceiveSsapResponse()
        self.__endpoint.insert(TestInserts.ONTOLOGY, {})
        self.__callback.waitForSsapResponse()
        self.assertFalse(self.__callback.isSsapResponseOk())
        
    def testSuccessfulSqlLikeInsert(self):
        self.__callback.prepareToReceiveSsapResponse()
        self.__endpoint.insert(TestInserts.ONTOLOGY, TestInserts.ONTOLOGY_INSERT_SQLLIKE, SSAP_QUERY_TYPE.SQLLIKE)
        self.__callback.waitForSsapResponse()
        self.assertTrue(self.__callback.isSsapResponseOk())
    
    def testUnsuccessfulSqlLikeInsert(self):
        self.__callback.prepareToReceiveSsapResponse()
        self.__endpoint.insert(TestInserts.ONTOLOGY, "Insert into foo values ('moo')", SSAP_QUERY_TYPE.SQLLIKE)
        self.__callback.waitForSsapResponse()
        self.assertFalse(self.__callback.isSsapResponseOk())

if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
