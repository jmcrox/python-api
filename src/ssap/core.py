# -*- coding: utf8 -*-
'''
 Python SSAP API
 Version 1.5
 
 © Indra Sistemas, S.A.
 2014  SPAIN
  
 All rights reserved
'''
from ssap.utils.enums import enum
from inspect import getargspec
from ssap.exceptions import InvalidSSAPCallback

SSAP_MESSAGE_TYPE = enum("JOIN", "LEAVE", "INSERT", "UPDATE", "DELETE", "QUERY", "SUBSCRIBE", "UNSUBSCRIBE", "INDICATION", "CONFIG", "BULK")

SSAP_QUERY_TYPE = enum("SQLLIKE", "NATIVE", "SIB_DEFINED", "CEP", "HDB", "CDB")

SSAP_MESSAGE_DIRECTION = enum("REQUEST", "RESPONSE", "ERROR")

SSAP_ERROR_CODE = enum("AUTHENTICATION", "AUTHORIZATION", "PROCESSOR", "PERSISTENCE", "PARSE_SQL", "ONTOLOGY_NOT_FOUND",
    "SIB_DEFINED_QUERY_NOT_FOUND", "OTHER")

class BasicSSAPCallback(object):
    '''
    The simplest SSAP callback. It defines an unique handler for all the incoming SSAP messages.
    '''
    
    def onSSAPMessageReceived(self, message):
        '''
        This method will be invoked when a SSAP message is received.
        
        Keyword arguments:
        message     -- the received SSAP message. It has already been deserialized.
        '''
        raise NotImplementedError
    
class MultiHandlerSSAPCallback(BasicSSAPCallback):
    '''
    A SSAP callback that define one or more handlers for each SSAP message type.
    '''
    def __init__(self):
        '''
        Initializes the state of the handler. By default, nothing will be done after receiving a SSAP message.
        '''
        self.__handlers = {}
        def clearHandler(value):
            if (value != SSAP_MESSAGE_TYPE.SUBSCRIBE and value != SSAP_MESSAGE_TYPE.UNSUBSCRIBE
                and value != SSAP_MESSAGE_TYPE.INDICATION):
                self.__handlers[value] = []
            else:
                self.__handlers[value] = {}
        SSAP_MESSAGE_TYPE.iterateOverValues(clearHandler)
        
    @staticmethod
    def __checkCallable(handler):
        '''
        This function will only detect silly configuration errors. It's not very
        pythonic, but these errors can be too hard to find.
        
        Keyword arguments:
        handler -- a function that will handle a SSAP message.
        '''
        if not handler is None and hasattr(handler, '__call__') :
            (args, _varargs, _keywords, _locals) = getargspec(handler)
            if len(args) == 1 or (len(args) == 2 and 'self' in args) :
                return
        raise InvalidSSAPCallback("The given object is not a valid SSAP callback function")
    
    @staticmethod
    def __isNotSubscriptionMessage(messageType):
        '''
        Checks if a SSAP message type is associated to a subscription.
        
        Keyword arguments:
        messageType -- a SSAP message type.
        
        '''
        return messageType != SSAP_MESSAGE_TYPE.SUBSCRIBE and messageType != SSAP_MESSAGE_TYPE.UNSUBSCRIBE \
            and messageType != SSAP_MESSAGE_TYPE.INDICATION
    
    def registerHandler(self, messageType, handler):
        '''
        Registers a new SSAP message handler.
        
        Keyword arguments:
        messageType     -- the type of the SSAP message that the handler will process.
        handler         -- the function that will handle the messages.
        '''
        MultiHandlerSSAPCallback.__checkCallable(handler)
        if (not MultiHandlerSSAPCallback.__isNotSubscriptionMessage(messageType)):
            raise InvalidSSAPCallback("The given message type is associated to a subscription. Please use registerSubscriptionHandler() instead.")
        self.__handlers[messageType].append(handler)
    
    def registerSubscriptionHandler(self, messageType, ontology, handler):
        '''
        Registers a new SSAP subscription message handler.
        
        Keyword arguments:
        messageType    -- the type of the SSAP message that the handler will process.
        ontology       -- the target ontology of the SSAP subscription messages.
        handler        -- the function that will handle the subscription messages.
        '''
        MultiHandlerSSAPCallback.__checkCallable(handler)
        if (MultiHandlerSSAPCallback.__isNotSubscriptionMessage(messageType)):
            raise InvalidSSAPCallback("The given message type not is associated to a subscription. Please use registerHandler() instead.")
        
        if (ontology in self.__handlers[messageType]):
            self.__handlers[messageType][ontology].append(handler)
        else:
            self.__handlers[messageType][ontology] = [handler]
            
    def unregisterHandler(self, messageType, handler):
        '''
        Unregisters (i.e. disables) a handler.
        
        Keyword arguments:
        messageType     -- the SSAP message type that the handler is currently processing.
        handler         -- the handler that we intend to disable. It will only be disabled for the given SSAP message type.
        '''
        if (handler == None) :
            raise InvalidSSAPCallback("The given object is not a valid SSAP callback function")
        self.__handlers[messageType].remove(handler)
        
    def unregisterSubscriptionhandler(self, messageType, ontology, handler):
        '''
        Unregisters (i.e. disables) a subscription message handler.
        
        Keyword arguments:
        messageType     -- the SSAP message type that the handler is currently processing.
        ontology       -- the target ontology of the SSAP subscription messages.
        handler         -- the handler that we intend to disable. It will only be disabled for the given SSAP message type.
        '''
        if (handler == None) :
            raise InvalidSSAPCallback("The given object is not a valid SSAP callback function")
        if (ontology in self.__handlers[messageType]) :
            self.__handlers[messageType][ontology].remove(handler)
        
    def onSSAPMessageReceived(self, message):
        # Do not call this method from client code!!!
        if (MultiHandlerSSAPCallback.__isNotSubscriptionMessage(message["messageType"])) :
            callbacksToInvoke = self.__handlers[message["messageType"]]
        else:
            callbacksToInvoke = self.__handlers[message["messageType"]]
            if (message["ontology"] in callbacksToInvoke):
                callbacksToInvoke = callbacksToInvoke[message["ontology"]]
            else:
                callbacksToInvoke = []
        for callback in callbacksToInvoke:
            callback(message)
            
class SSAPEndpoint(object):
    '''
    This class defines the interface common to all the SSAP endpoint implementations.
    '''
    
    def __init__(self, callback):
        '''
        Initializes the state of the endpoint.
        
        Keyword arguments:
        callback     -- the callback that will process the incoming SSAP messages.
        '''
        self._callback = callback
        self._clearStateData()
    
    def joinWithCredentials(self, user, password, instance):
        '''
        Performs a user/password-based JOIN.
        
        Keyword arguments:
        user         -- a valid username.
        password     -- a valid password.
        instance     -- the KP instance ID to use.
        '''
        raise NotImplementedError
    
    def joinWithToken(self, token, instance):
        '''
        Performs a token-based JOIN.
        
        Keyword arguments:
        token     -- a valid token.
        instance  -- the KP instance ID to use.
        '''
        raise NotImplementedError
    
    def renovateSessionKey(self):
        '''
        Renews the current session.
        '''
        raise NotImplementedError
    
    def leave(self):
        '''
        Closes the current session.
        '''
        raise NotImplementedError
    
    def insert(self, ontology, data, queryType=SSAP_QUERY_TYPE.NATIVE):
        '''
        Sends data to be inserted in the RTDB.
        
        Keyword arguments:
        ontology         -- the target ontology of the INSERT operation.
        data             -- the data to insert in the RTDB.
        queryType        -- defines the format of the data (NATIVE or SQL-LIKE).
        '''
        raise NotImplementedError
    
    def update(self, ontology, query, data, queryType=SSAP_QUERY_TYPE.NATIVE):
        '''
        Updates data in the RTDB.
        
        Keyword arguments:
        ontology         -- the target ontology of the UPDATE operation.
        query            -- the query that selects the data that will be updated in the RTDB.
        data             -- defines what will be updated in the RTDB.
        queryType        -- the type of the query (NATIVE or SQL-LIKE).        
        '''
        raise NotImplementedError
    
    def delete(self, ontology, query, queryType=SSAP_QUERY_TYPE.NATIVE):
        '''
        Removes data from the RTDB.
        
        Keyword arguments:
        ontology         -- the target ontology of the DELETE operation.
        query            -- the query that selects the data that will be removed from the RTDB.
        queryType        -- the type of the query (NATIVE or SQL-LIKE).  
        '''
        raise NotImplementedError
    
    def query(self, ontology, query, queryType=SSAP_QUERY_TYPE.NATIVE):
        '''
        Retrieves data stored in the RTDB, the HDB, the CDB or the SIB.
        
        Keyword arguments:
        ontology         -- the target ontology of the QUERY operation.
        query            -- the query that selects the data that will be returned.
        queryType        -- the type of the query (NATIVE, SQL-LIKE, CDB, SIB-DEFINED).  
        '''
        raise NotImplementedError

    def subscribe(self, ontology, query, queryType=SSAP_QUERY_TYPE.NATIVE, refreshTimeInMillis=1000):
        '''
        Sends a SUBSCRIBE request to the SIB.
        
        Keyword arguments:
        ontology             -- the target ontology of the subscription operation.
        query                -- the query that selects the data that will generate subscription notifications.
        queryType            -- the type of the query (NATIVE, SQL-LIKE, CDB, SIB-DEFINED).  
        refreshTimeInMillis  -- the period of time that will separate two consecutive subscription notifications.
        '''
        raise NotImplementedError
    
    def unsubscribe(self, subscriptionId):
        '''
        Cancels a subscription.
        
        Keyword arguments:
        subscriptionID       -- the ID of the subscription that will be cancelled.
        '''
        raise NotImplementedError
    
    def bulk(self, ontology, ssapBulkRequest):
        '''
        Sends a BULK request to the SIB.
        
        Keyword arguments:
        ontology         -- the target ontology of the BULK request.
        ssapBulkRequest  -- an object containing the requests that will be processed together.
        '''
        raise NotImplementedError
    
    @staticmethod
    def hasOkField(jsonMessage):
        '''
        Checks if a SSAP message has an OK field to indicate errors.
        
        Keyword arguments:
        jsonMessage     -- the SSAP message to check.
        '''
        return jsonMessage["messageType"] != SSAP_MESSAGE_TYPE.INDICATION and \
            jsonMessage["messageType"] != SSAP_MESSAGE_TYPE.CONFIG
    
    def config(self, kpName, kpInstance, token, assetService, assetServiceParam):
        '''
        Sends a GET_CONFIG request to the SIB.
        
        Keyword arguments:
        kpName             -- the name of the KP.
        kpInstance         -- the instance ID to use. It does not start with the name of the KP.
        token              -- the token to use.
        assetService       -- the asset service that is affected by the CONFIG request.
        assetServiceParams -- the dictionary of parameters that will be passed to the asset service.
        '''
        raise NotImplementedError
        
    def waitForever(self):
        '''
        Waits until the SSAP endpoint stops.
        '''
        raise NotImplementedError
    
    def _clearStateData(self):
        '''
        Clears the state data stored in the endpoint object.
        '''
        self._sessionKey = None
        self._token = None
        self._instance = None