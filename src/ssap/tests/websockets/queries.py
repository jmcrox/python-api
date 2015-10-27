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

class TestQueries(unittest.TestCase):
    
    ONTOLOGY = "TestSensorTemperatura"
    TOKEN = "e5e8a005d0a248f1ad2cd60a821e6838"
    INSTANCE = "KPTestTemperatura:KPTestTemperatura01"
    SQLLIKE_QUERY = "SELECT * FROM TestSensorTemperatura LIMIT 10"    
    NATIVE_QUERY = "db.TestSensorTemperatura.find().limit(10)"  
    SIB_DEFINED_QUERY = "MiConsulta"
    SIB_DEFINED_QUERY_WITH_PARAMS = "selectAllWithParam"

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
        self.__endpoint.joinWithToken(TestQueries.TOKEN, TestQueries.INSTANCE)
        self.__callback.waitForSsapResponse()
        self.assertTrue(self.__callback.isSsapResponseOk())
    
    def __doLeave(self):
        self.__callback.prepareToReceiveSsapResponse()
        self.__endpoint.leave()
        self.__callback.waitForSsapResponse()
        self.assertTrue(self.__callback.isSsapResponseOk())
        
    def testSuccessfulNativeQuery(self):
        self.__callback.prepareToReceiveSsapResponse()
        self.__endpoint.query(TestQueries.ONTOLOGY, TestQueries.NATIVE_QUERY)
        self.__callback.waitForSsapResponse()
        self.assertTrue(self.__callback.isSsapResponseOk())
        
    def testUnsuccessfulNativeQuery(self):
        self.__callback.prepareToReceiveSsapResponse()
        self.__endpoint.query(TestQueries.ONTOLOGY, "wrong query")
        self.__callback.waitForSsapResponse()
        self.assertFalse(self.__callback.isSsapResponseOk())
        
    def testSuccessfulSqlLikeQuery(self):
        self.__callback.prepareToReceiveSsapResponse()
        self.__endpoint.query(TestQueries.ONTOLOGY, TestQueries.SQLLIKE_QUERY, SSAP_QUERY_TYPE.SQLLIKE)
        self.__callback.waitForSsapResponse()
        self.assertTrue(self.__callback.isSsapResponseOk())
        
    def testUnsuccessfulSqlLikeQuery(self):
        self.__callback.prepareToReceiveSsapResponse()
        self.__endpoint.query(TestQueries.ONTOLOGY, "", SSAP_QUERY_TYPE.SQLLIKE)
        self.__callback.waitForSsapResponse()
        self.assertFalse(self.__callback.isSsapResponseOk())
        
    def testSibDefinedQuery(self):
        self.__callback.prepareToReceiveSsapResponse()
        self.__endpoint.query(TestQueries.ONTOLOGY, TestQueries.SIB_DEFINED_QUERY, SSAP_QUERY_TYPE.SIB_DEFINED)
        self.__callback.waitForSsapResponse()
        self.assertFalse(self.__callback.isSsapResponseOk())
     
    def testSibDefinedQueryWithParams(self):
        self.__callback.prepareToReceiveSsapResponse()
        queryParams = {}
        queryParams["PARAM1"] = "S_Temperatura_00001"
        self.__endpoint.query(TestQueries.ONTOLOGY, TestQueries.SIB_DEFINED_QUERY_WITH_PARAMS, SSAP_QUERY_TYPE.SIB_DEFINED, queryParams)
        self.__callback.waitForSsapResponse()
        self.assertFalse(self.__callback.isSsapResponseOk())    

if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
