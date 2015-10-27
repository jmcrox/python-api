# -*- coding: utf8 -*-
'''
 Python SSAP API
 Version 1.5
 
 © Indra Sistemas, S.A.
 2014  SPAIN
  
 All rights reserved
'''
import unittest
from ssap.core import SSAP_QUERY_TYPE, MultiHandlerSSAPCallback, SSAP_MESSAGE_TYPE
from ssap.factories import SSAPEndpointFactory
from time import sleep
import json

class TestMultihandlerCallback(unittest.TestCase):

    def setUp(self):
        self.__ontology = "TestSensorTemperatura"
        serverURL = 'ws://sofia2.com/sib/api_websocket'
        callback = MultiHandlerSSAPCallback()
        callback.registerHandler(SSAP_MESSAGE_TYPE.JOIN, self.__onJoin)
        callback.registerSubscriptionHandler(SSAP_MESSAGE_TYPE.INDICATION, self.__ontology, self.__onIndication)
        self.__endpoint = SSAPEndpointFactory.buildWebsocketBasedSSAPEndpoint(serverURL, callback, True)
        self.__endpoint.joinWithToken("e5e8a005d0a248f1ad2cd60a821e6838", "KPTestTemperatura:KPTestTemperatura01")
        sleep(5)
        
    def __onJoin(self, message):
        print("Join callback")
        print(json.dumps(message, sort_keys=True, indent=4, separators=(",", ":")))
        
    def __onIndication(self, message):
        print("Indication callback")
        print(json.dumps(message, sort_keys=True, indent=4, separators=(",", ":")))

    def tearDown(self):
        self.__endpoint.leave()
        sleep(5)

    def testSuccessfulSubscribe(self):
        query = "{SELECT * FROM TestSensorTemperatura}"
        self.__endpoint.subscribe(self.__ontology, query, SSAP_QUERY_TYPE.SQLLIKE, 100)
        sleep(5);
        jsonObject = {}
        jsonObject["Sensor"] = {}
        jsonObject["Sensor"]["geometry"] = {}
        jsonObject["Sensor"]["geometry"]["coordinates"] = [ 40.512967, -3.67495 ]
        jsonObject["Sensor"]["geometry"]["type"] = "Point"
        jsonObject["Sensor"]["assetId"] = "S_Temperatura_00066"
        jsonObject["Sensor"]["measure"] = 10
        jsonObject["Sensor"]["timestamp"] = {"$date" : "2014-04-29T08:24:54.005Z"}
        self.__endpoint.insert(self.__ontology, jsonObject, SSAP_QUERY_TYPE.NATIVE)
        sleep(5)
        
if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
