# -*- coding: utf8 -*-
'''
A websocket-based implementation of the SSAP API.

This module is part of the Python SSAP API, version 1.5
 
 © Indra Sistemas, S.A.
 2014  SPAIN
  
 All rights reserved
'''

from __future__ import print_function
from ssap.core import SSAPEndpoint, SSAP_MESSAGE_TYPE, SSAP_QUERY_TYPE
from ssap.messages.messages import _SSAPMessageFactory, _SSAPMessageParser
from ws4py.client.threadedclient import WebSocketClient
from ssap.utils.logs import LogFactory
from ssap.utils.datastructures import GenericThreadSafeList
from ssap.exceptions import InvalidSSAPOperation, SSAPConnectionError
from ssap.utils.enums import enum
import logging
from time import sleep

_CONNECTION_STATUS = enum("OPENED", "CLOSED")

class WebsocketConnectionData(object):
    '''
    These objects store the configuration data of a websocket-based connection.
    '''
    def __init__(self, server_url):
        '''
        Stores the websocket server URL in the configuration object.
        '''
        self.__server_url = server_url
    
    def getServerUrl(self):
        '''
        Returns the websocket server URL.
        '''
        return self.__server_url
    
    def getProtocols(self):
        '''
        Returns a list containing the supported websocket protocols.
        '''
        return ['http_only']

class WebsocketBasedSSAPEndpoint(SSAPEndpoint):    
    '''A websocket-based SSAP endpoint'''
    def __init__(self, callback, connectionData, debugMode=False):
        '''
        Initializes the state of the endpoint.
        
        Keyword arguments:
        callback          -- the object that will process the incoming SSAP messages.
        connectionData    -- the object that stores the configuration of the websocket connection.
        debugMode         -- a flag that enables additional debug messages.
        '''
        SSAPEndpoint.__init__(self, callback)
        if (debugMode) :
            logLevel = logging.DEBUG
        else:
            logLevel = logging.INFO
        self.__logger = LogFactory.configureLogger(self, logLevel, LogFactory.DEFAULT_LOG_FILE)      
        self.__queue = GenericThreadSafeList()
        self.__websocket = None
        self.__connectionData = connectionData
        self.__activeSubscriptions = 0
        
    def __sendSSAPRequest(self, messageType, ssapRequest, checkWebsocket=True):
        '''
        Prepares a SSAP message to be sent to the SIB.
        
        Keyworkd arguments: 
        messageType        -- the type of the SSAP message to send.
        ssapRequest        -- the serialized SSAP message to send.
        checkWebsocket     -- indicates if we must check wether the connection is ready or not.
        '''
        if checkWebsocket :
            self.__checkIfWebsocketIsInstantiated()
        self.__appendRequest(_SSAPRequest(messageType, ssapRequest))
        
    def joinWithToken(self, token, instance):
        self._token = token
        self._instance = instance        
        self.__sendSSAPRequest(SSAP_MESSAGE_TYPE.JOIN,
                               _SSAPMessageFactory.buildTokenBasedJoinMessage(token, instance), False)
        
    def leave(self):
        if (self.__activeSubscriptions != 0):
            self.__logger.warning("There are active subscriptions. You should cancel them before disconnecting from the SIB")
        self.__sendSSAPRequest(SSAP_MESSAGE_TYPE.LEAVE,
                               _SSAPMessageFactory.buildLeaveMessage(self._sessionKey))
        
    def renovateSessionKey(self):
        self.__sendSSAPRequest(SSAP_MESSAGE_TYPE.JOIN,
                               _SSAPMessageFactory.buildRenewSessionKeyJoinMessage(self._token, self._instance, self._sessionKey))
        
    def insert(self, ontology, data, queryType=SSAP_QUERY_TYPE.NATIVE):
        self.__sendSSAPRequest(SSAP_MESSAGE_TYPE.INSERT,
                               _SSAPMessageFactory.buildInsertMessage(ontology, data, queryType, self._sessionKey))
        
    def query(self, ontology, query, queryType=SSAP_QUERY_TYPE.NATIVE, queryParams = None):
        self.__sendSSAPRequest(SSAP_MESSAGE_TYPE.QUERY,
            _SSAPMessageFactory.buildQueryMessage(ontology, query, queryType, queryParams, self._sessionKey))
    

    def update(self, ontology, query, data, queryType=SSAP_QUERY_TYPE.NATIVE):
        self.__sendSSAPRequest(SSAP_MESSAGE_TYPE.UPDATE,
                               _SSAPMessageFactory.buildUpdateMessage(ontology, query, queryType, data, self._sessionKey))
    
    def delete(self, ontology, query, queryType=SSAP_QUERY_TYPE.NATIVE):
        self.__sendSSAPRequest(SSAP_MESSAGE_TYPE.DELETE,
                               _SSAPMessageFactory.buildDeleteMessage(ontology, query, queryType, self._sessionKey))
        
    def subscribe(self, ontology, query, queryType=SSAP_QUERY_TYPE.NATIVE, refreshTimeInMillis=1000):
        self.__sendSSAPRequest(SSAP_MESSAGE_TYPE.SUBSCRIBE,
                               _SSAPMessageFactory.buildSubscribeMessage(ontology, query, queryType, refreshTimeInMillis, self._sessionKey))
    
    def unsubscribe(self, subscriptionId):
        self.__sendSSAPRequest(SSAP_MESSAGE_TYPE.UNSUBSCRIBE,
                               _SSAPMessageFactory.buildUnsubscribeMessage(subscriptionId, self._sessionKey))
        
    def config(self, kpName, kpInstance, token, assetService, assetServiceParam):
        self.__sendSSAPRequest(SSAP_MESSAGE_TYPE.CONFIG, _SSAPMessageFactory.buildConfigMessage(kpName, kpInstance, token, assetService, assetServiceParam), False)

#     def bulk(self, ontology, ssapBulkRequest):
#         self.__sendSSAPRequest(SSAP_MESSAGE_TYPE.BULK,
#                                _SSAPMessageFactory.buildBulkMessage(ssapBulkRequest, ontology, self._sessionKey))

    def waitForever(self):
        if (self.__websocket is None) :
            raise InvalidSSAPOperation("The connection with the SIB is not established")
        self.__websocket.run_forever()
        
    def __appendRequest(self, request):
        '''
        Queues a send request in the output message queue.
        
        Keyword arguments:  
        request     -- the request to queue.
        '''
        
        self.__queue.append(request)
        if (self.__queue.getSize() == 1):
            self.__sendNextRequestToSib()
            
    def __checkIfWebsocketIsInstantiated(self):
        '''
        Checks if the websocket has been instantiated. This allows us to detect invalid API invocations.
        '''
        if (self.__websocket is None):
            raise InvalidSSAPOperation("The connection with the SIB has not been established yet")
    
    def __sendNextRequestToSib(self):
        '''
        Pops a SSAP message request from the output queue and sends it to the SIB.
        '''
        if (self.__websocket is None):
            self.__openConnection()
        request = self.__queue[0]
        self.__websocket.send(request.getQuery(), False)            
        
    def __openConnection(self):
        '''
        Establishes a websocket-based connection with the SIB
        '''
        if (not self.__websocket is None) :
            raise InvalidSSAPOperation("The connection with the SIB has already been established")
        try :
            self.__websocket = _SSAPWebsocketClient(self.__connectionData.getServerUrl(),
                                                    self.__connectionData.getProtocols(),
                                                    self.__onConnectionEvent,
                                                    self.__onDataReceived)
            self.__websocket.connect()
            self.__waitUntilConnectionEstablished()        
        except Exception as ws4pyException:
            self._clearStateData()
            raise SSAPConnectionError("Couldn't connect to the SIB: " + str(ws4pyException))        
        
    def __waitUntilConnectionEstablished(self):
        '''
        Waits for the websocket connection to the SIB to be established.
        '''
        self.__logger.info("Waiting for the websocket connection to be established")
        while not self.__connection_established :            
            sleep(1)
            
    def __onConnectionEvent(self):
        '''
        This method is invoked from the ws4py library when a websocket connection is established or closed.
        '''
        self.__connection_established = True
        
    def __onDataReceived(self, data):
        '''
        This method is invokef from the ws4py library when data is received from the websocket.
        
        Keyword arguments:
        data    -- the received data.
        '''
        if (len(data.data) == 1):
            return # We might receive some shit after closing the connection. We won't process it.
        self.__logger.debug("Data received: " + data.data)
        parsed_message = _SSAPMessageParser.parse(data.data)
        
        # The message content can be modified within the callback. We must copy
        # everything we need before invoking it.
        messageType = parsed_message["messageType"]
        noErrors = parsed_message["messageType"] != SSAP_MESSAGE_TYPE.INDICATION and parsed_message["body"]["ok"]
                
        if (noErrors and messageType == SSAP_MESSAGE_TYPE.JOIN):
            self._sessionKey = parsed_message["sessionKey"]
        self._callback.onSSAPMessageReceived(parsed_message)  
              
        if (messageType != SSAP_MESSAGE_TYPE.INDICATION) :
            self.__queue.pop()       
            
        if (noErrors) :             
        
            if (messageType == SSAP_MESSAGE_TYPE.LEAVE):
                self.__closeConnection()
                self._clearStateData()
                self.__websocket = None
            elif (messageType == SSAP_MESSAGE_TYPE.SUBSCRIBE):
                self.__activeSubscriptions = self.__activeSubscriptions + 1
            elif (messageType == SSAP_MESSAGE_TYPE.UNSUBSCRIBE):
                self.__activeSubscriptions = self.__activeSubscriptions - 1
            
        if (self.__queue.getSize() != 0) :
            self.__sendNextRequestToSib()
    
    def __closeConnection(self):
        '''
        Closes the connection.
        '''
        if (self.__websocket is None) :
            raise InvalidSSAPOperation("The connection with the SIB is not established")
        self._clearStateData()
        self.__websocket.close()
        self.__websocket = None
        
    def _clearStateData(self):
        '''
        Deletes all the state data stored in the endpoint (i.e. session keys, ...).
        '''
        SSAPEndpoint._clearStateData(self)
        self.__connection_established = False
        
        
class _SSAPRequest(object):
    '''
    These objects store the data of an outgoing SSAP request (i.e. one that will be sent to the SIB).
    '''
    
    def __init__(self, requestType, query):
        '''
        Initializes the state of the request.
        
        Keyword arguments:
        requestType    --    the SSAP message type of the request.
        query          --    the serialized SSAP message to be sent.
        '''
        self.__type = requestType
        self.__query = query
        
    def getType(self):
        '''
        Returns the SSAP message type of the request.
        '''
        return self.__type
    
    def getQuery(self):
        '''
        Returns the serialized SSAP message of the request.
        '''
        return self.__query
    
class _SSAPWebsocketClient(WebSocketClient):
    '''
    The ws4py websocket client that is used by the SSAP API.
    '''
    
    def __init__(self, serverUrl, protocols, connectionEstablishedHandler, dataReceivedEventHandler):
        '''
        Initializes the state of the client.
        
        Keyword arguments:
        serverURL                        -- the URL of the websocket server.
        protocols                        -- a list containing the websocket protocols supported by the websockets client.
        connectionEstablishedHandler     -- a function that will be invoked after establishing the websocket connection.
        dataReceivedHandler              -- a function that will be invoked after receiving data from the websocket.
        '''
        WebSocketClient.__init__(self, serverUrl, protocols)
        self.__logger = LogFactory.configureLogger(self, logging.INFO, LogFactory.DEFAULT_LOG_FILE)
        self.__connectionEstablishedHandler = connectionEstablishedHandler
        self.__dataReceivedEventHandler = dataReceivedEventHandler

    def opened(self):
        '''
        This function will be invoked from the ws4py library after establishing the websocket connection.
        '''
        self.__logger.info("Websocket connection established")
        self.__connectionEstablishedHandler()
    
    def closed(self, code, reason):
        '''
        This function will be invoked from the ws4py library after closing the websocket connection.
        
        Keyword arguments:
        code     -- a status code.
        reason   -- a string containing the disconnection reason.
        '''
        message = "Websocket connection closed. Code: {0}, Message: {1}".format(code, reason)
        self.__logger.info(message)
        
    def received_message(self, message):
        '''
        This function will be invoked from the ws4py library after receiving data from the websocket.
        
        Keyword arguments:
        message     -- An object containing the received data.
        '''
        self.__logger.debug("Data received: {0}".format(message))
        self.__dataReceivedEventHandler(message)
        
class _ConnectionStatus(object):
    '''
    These objects hold the status of a websocket connection.
    '''
    
    def __init__(self, status, message=""):
        '''
        Initializes the state of the object.
        
        Keyword arguments:        
        status     --    The status of the connection.
        message    --    A string that describes the status of the connection.
        '''
        self.__status = status
        self.__message = message
        
    def getStatus(self):
        return self.__status
    
    def getMessage(self):
        return self.__message
