# -*- coding: utf8 -*-
'''
 Python SSAP API
 Version 1.5
 
 © Indra Sistemas, S.A.
 2014  SPAIN
  
 All rights reserved
'''

from ssap.core import BasicSSAPCallback, SSAP_MESSAGE_TYPE, SSAPEndpoint

import json
from time import sleep

class TestCallback(BasicSSAPCallback):
    
    def __init__(self, ignoreJoinsAndLeaves=False):
        BasicSSAPCallback.__init__(self)
        self.__isResponseOk = False
        self.__responseReceived = False
        self.__ignoreJoinsAndLeaves = ignoreJoinsAndLeaves
        self.__subscriptionId = None
        self.__indicationReceived = False
    
    def onSSAPMessageReceived(self, message):
        self.__prettyPrintMessage(message)
        self.__isResponseOk = SSAPEndpoint.hasOkField(message) and message["body"]["ok"]
        if (message["messageType"] == SSAP_MESSAGE_TYPE.SUBSCRIBE):
            self.__subscriptionId = message["body"]["data"]
        self.__indicationReceived = message["messageType"] == SSAP_MESSAGE_TYPE.INDICATION
        self.__responseReceived = True
        
    def isSsapResponseOk(self):
        return self.__isResponseOk
    
    def wasIndicationReceived(self):
        return self.__indicationReceived
    
    def prepareToReceiveSsapResponse(self):
        self.__responseReceived = False
        self.__indicationReceived = False
    
    def waitForSsapResponse(self):
        while(not self.__responseReceived):
            sleep(1)
            
    def waitForSsapIndication(self):
        while (not self.__indicationReceived):
            sleep(1)
            
    def getSubscriptionId(self):
        return self.__subscriptionId
    
    def __prettyPrintMessage(self, message):
        if (self.__ignoreJoinsAndLeaves and 
            (message["messageType"] == SSAP_MESSAGE_TYPE.JOIN or message["messageType"] == SSAP_MESSAGE_TYPE.LEAVE)):
            return
        print("A(n) " + SSAP_MESSAGE_TYPE.toString(message["messageType"]) + " message was received!")
        print(json.dumps(message, sort_keys=True, indent=4, separators=(",", ":")))