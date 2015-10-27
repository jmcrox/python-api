# -*- coding: utf8 -*-
'''
This module contains the data structures for the SSAP messages.

This module is part of the Python SSAP API, version 1.5
 
 © Indra Sistemas, S.A.
 2014  SPAIN
  
 All rights reserved
'''
 
import json
import six
from ssap.utils.logs import LogFactory
from ssap.utils.strings import bytes2String
from ssap.core import SSAP_QUERY_TYPE, SSAP_MESSAGE_DIRECTION, SSAP_MESSAGE_TYPE, \
    SSAP_ERROR_CODE, SSAPEndpoint
    
class SSAPBulkRequest(object):
    '''
    This object represents the body of a SSAP bulk message.
    '''
    def __init__(self):
        '''
        Initializes the state of the object. 
        '''
        self.__messages = []
    
    def addInsertMessage(self,ontology, data, queryType = SSAP_QUERY_TYPE.NATIVE):
        '''
        Adds an insert message to the bulk request.
        
        Keyword arguments:
        ontology         -- the ontology associated to the SSAP message.
        data             -- the data to insert.
        queryType        -- the query type that defines the content of the data (SQL or MongoDB string)
        '''
        self.__messages.append(_SSAPMessageFactory.buildInsertMessage(ontology, data, queryType, None, False))
    
    def addUpdateMessage(self, ontology, query, data, queryType = SSAP_QUERY_TYPE.NATIVE):
        '''
        Adds an update message to the bulk request.
        ontology         -- the ontology associated to the SSAP message.
        query            -- the query that filters the data to update.
        data             -- the data to update in the selected ontology instances.
        queryType        -- the query type that defines the format of the query (SQL or MongoDB-like)
        
        '''
        self.__messages.append(_SSAPMessageFactory.buildUpdateMessage(ontology, query, queryType, data, None, False))
        
    def addDeleteMessage(self,ontology, query, queryType = SSAP_QUERY_TYPE.NATIVE):
        '''
        Adds a delete message to the bulk request.
        ontology         -- the ontology associated to the SSAP message.
        query            -- the query that filters the data to delete.
        queryType        -- the query type that defines the format of the query (SQL or MongoDB-like)
        
        '''
        self.__messages.append(_SSAPMessageFactory.buildDeleteMessage(ontology, query, queryType, None, False))      
        
    def _getMessages(self):
        '''
        Returns a list containing the SSAP messages of the bulk request.
        '''
        return self.__messages  

class _SSAPMessageFactory(object):
    '''
    This class contains methods to build SSAP messages.
    '''
    
    @staticmethod
    def __buildMessageStructure(messageType, sessionKey):
        '''
        Includes the data common to all the SSAP message types.
        
        Keyword arguments:
        messageType     --    the type of the SSAP message.
        sessionKey      --    the session key to include in the SSAP message.
        '''
        jsonObj = {}
        jsonObj["body"] = {}
        jsonObj["direction"] = SSAP_MESSAGE_DIRECTION.toString(SSAP_MESSAGE_DIRECTION.REQUEST)
        jsonObj["messageType"] = SSAP_MESSAGE_TYPE.toString(messageType)
        jsonObj["sessionKey"] = sessionKey
        return jsonObj
    
    @staticmethod
    def __serializeMessage(jsonObj):
        '''
        Serializes the given SSAP message. The output JSON string will already be minimized.
        
        Keyword arguments:
        jsonObj: the SSAP message to serialize.
        '''
#         print(json.dumps(jsonObj, sort_keys=True, indent=4, separators=(",", ":")))
        return json.dumps(jsonObj, sort_keys=True, indent=None, separators=(",", ":"))
    
    @staticmethod
    def buildBulkMessage(ssapBulkRequest, ontology, sessionKey):
        '''
        Builds a SSAP BULK message.
        
        Keyword arguments:
        ssapBulkRequest     -- a SSAP bulk request.
        ontology            -- the target ontology of the BULK message.
        sessionKey          -- the session key that will be included in the BULK message.
        '''
        jsonObj = _SSAPMessageFactory.__buildMessageStructure(SSAP_MESSAGE_TYPE.BULK, sessionKey)
        jsonObj["body"] = []
        messages = ssapBulkRequest._getMessages()
        for message in messages:
            bulkItem = {}
            bulkItem["type"] = message["messageType"]
            bulkItem["body"] = message["body"]
            bulkItem["ontology"] = message["ontology"]
            jsonObj["body"].append(bulkItem)
        jsonObj["ontology"] = ontology
        return _SSAPMessageFactory.__serializeMessage(jsonObj)

    @staticmethod
    def buildConfigMessage(kpName, kpInstance, token, assetService, assetServiceParam):
        '''
        Builds a CONFIG SSAP message.
        
        Keyword arguments:
        kpName             -- the KP name that will be included in the CONFIG message.
        kpInstance         -- the KP instance that will be included in the CONFIG message. This string ONLY contains the instance identifier.
        assetService       -- the name of the asset service that is related to the CONFIG request.
        assetServiceParam  -- a dictionary containing the parameters that will be passed to the asset service.
        '''
        jsonObj = _SSAPMessageFactory.__buildMessageStructure(SSAP_MESSAGE_TYPE.CONFIG, None)
        jsonObj["body"] = {}
        jsonObj["body"]["kp"] = kpName
        jsonObj["body"]["instanciaKp"] = kpInstance
        jsonObj["body"]["token"] = token
        jsonObj["body"]["assetService"] = assetService
        jsonObj["body"]["assetServiceParam"] = assetServiceParam
        return _SSAPMessageFactory.__serializeMessage(jsonObj)
    
    @staticmethod
    def buildTokenBasedJoinMessage(token, instance):
        '''
        Builds a token-based JOIN SSAP message.
        
        Keyword arguments:
        token     -- the token that will be included in the JOIN message.
        instance  -- the KP instance that will be included in the JOIN message, with the format <KP ID>:<KP instance ID>
        '''
        jsonObj = _SSAPMessageFactory.__buildMessageStructure(SSAP_MESSAGE_TYPE.JOIN, None)        
        jsonObj["body"]["instance"] = instance
        jsonObj["body"]["token"] = token
        return _SSAPMessageFactory.__serializeMessage(jsonObj)
    
    @staticmethod
    def buildLeaveMessage(sessionKey):
        '''
        Builds a LEAVE SSAP message.
        
        Keyword arguments:
        sessionKey    -- the session key that will be included in the LEAVE request.
        '''
        jsonObj = _SSAPMessageFactory.__buildMessageStructure(SSAP_MESSAGE_TYPE.LEAVE, sessionKey)        
        return _SSAPMessageFactory.__serializeMessage(jsonObj)
    
    @staticmethod
    def buildRenewSessionKeyJoinMessage(token, instance, sessionKey):
        '''
        Builds a JOIN SSAP message that renews a session.
        
        Keyword arguments:
        token        -- the token of the KP
        instance     -- the KP instance that will be included in the JOIN message, with the format <KP ID>:<KP instance ID>.
        sessionKey   -- the identifier of the session that will be renewed.
        '''
        jsonObj = _SSAPMessageFactory.__buildMessageStructure(SSAP_MESSAGE_TYPE.JOIN, sessionKey)        
        jsonObj["body"]["instance"] = instance
        jsonObj["body"]["token"] = token
        return _SSAPMessageFactory.__serializeMessage(jsonObj)
    
    @staticmethod
    def buildInsertMessage(ontology, data, queryType, sessionKey, serialize = True):
        '''
        Builds an INSERT SSAP message.
        
        Keyword arguments:
        ontology         -- the target ontology of the INSERT operation.
        data             -- a string containing the JSON instance or a SQL-like INSERT statement.
        queryType        -- the type of the query (native or SQL-like)
        sessionKey       -- the identifier of the session.
        serialize        -- indicates if a JSON object or a serialized JSON object must be returned.
        '''
        jsonObj = _SSAPMessageFactory.__buildMessageStructure(SSAP_MESSAGE_TYPE.INSERT, sessionKey)  
        jsonObj["body"]["query"] = None
        jsonObj["ontology"] = ontology
        jsonObj["body"]["queryType"] = SSAP_QUERY_TYPE.toString(queryType)
        
        if (queryType == SSAP_QUERY_TYPE.NATIVE) :                  
            jsonObj["body"]["data"] = data
            jsonObj["body"]["query"] = None
        else :
            jsonObj["body"]["data"] = None
            jsonObj["body"]["query"] = data      
        if (serialize):     
            return _SSAPMessageFactory.__serializeMessage(jsonObj)
        else:
            return jsonObj
    
    @staticmethod
    def buildQueryMessage(ontology, query, queryType, queryParams, sessionKey):
        '''
        Builds a QUERY SSAP message.
        
        Keyword arguments:
        ontology         -- the target ontology of the QUERY operation.
        query            -- que query to perform.
        queryType        -- the type of the query (native, SQL-like, configuration database, historical database).
        queryParams      -- an object containing the parameters of the query.
        sessionKey       -- the identifier of the session.
        '''
        jsonObj = _SSAPMessageFactory.__buildMessageStructure(SSAP_MESSAGE_TYPE.QUERY, sessionKey)  
        jsonObj["body"]["query"] = query
        jsonObj["body"]["queryParams"] = queryParams        
        jsonObj["body"]["queryType"] = SSAP_QUERY_TYPE.toString(queryType)
        jsonObj["ontology"] = ontology
        return _SSAPMessageFactory.__serializeMessage(jsonObj)   
    
    @staticmethod
    def buildUpdateMessage(ontology, query, queryType, data, sessionKey, serialize = True):
        '''
        Builds an UPDATE ssap message.
        
        Keyword arguments:
        ontology         -- the target ontology of the UPDATE operation.
        query            -- the query that selects the instances that will be updated.
        queryType        -- the type of the query (native or SQL-like).
        data             -- an expression that updates parts of the selected instances.
        sessionKey       -- the identifier of the session.
        '''
        jsonObj = _SSAPMessageFactory.__buildMessageStructure(SSAP_MESSAGE_TYPE.UPDATE, sessionKey)  
        jsonObj["body"]["query"] = query
        jsonObj["body"]["data"] = data
        jsonObj["body"]["queryType"] = SSAP_QUERY_TYPE.toString(queryType)
        jsonObj["ontology"] = ontology
        if (serialize):
            return _SSAPMessageFactory.__serializeMessage(jsonObj)  
        else:
            return jsonObj
    
    @staticmethod
    def buildDeleteMessage(ontology, query, queryType, sessionKey, serialize = True):
        '''
        Builds a DELETE ssap message.
        
        Keyword arguments:
        ontology         -- the target ontology of the DELETE operation.
        query            -- the query that selects the instances that will be deleted.
        queryType        -- the type of the query (native or SQL-like).
        sessionKey       -- the identifier of the session.
        serialize        -- indicates if a JSON object or a serialized JSON object must be returned.
        '''
        jsonObj = _SSAPMessageFactory.__buildMessageStructure(SSAP_MESSAGE_TYPE.DELETE, sessionKey)  
        jsonObj["body"]["data"] = None
        jsonObj["body"]["query"] = query
        jsonObj["body"]["queryType"] = SSAP_QUERY_TYPE.toString(queryType)
        if (serialize):
            return _SSAPMessageFactory.__serializeMessage(jsonObj) 
        else:
            return jsonObj
    
    @staticmethod
    def buildSubscribeMessage(ontology, query, queryType, refreshTimeMillis, sessionKey):
        '''
        Builds a SUBSCRIBE SSAP message.
        
        Keyword arguments:
        ontology             -- the target ontology of the SUBSCRIBE operation.
        query                -- the query that filters the ontology instance that will trigger INDICATION messages.
        refreshTimeMillis    -- the period of time that separates two consecutive notification sequences (in milliseconds). 
        queryType            -- the type of the query (native or SQL-like).
        sessionKey           -- the identifier of the session.
        '''
        jsonObj = _SSAPMessageFactory.__buildMessageStructure(SSAP_MESSAGE_TYPE.SUBSCRIBE, sessionKey)  
        jsonObj["body"]["query"] = query
        jsonObj["body"]["queryType"] = SSAP_QUERY_TYPE.toString(queryType)
        jsonObj["body"]["msRefresh"] = refreshTimeMillis
        jsonObj["ontology"] = ontology
        return _SSAPMessageFactory.__serializeMessage(jsonObj) 
    
    @staticmethod
    def buildUnsubscribeMessage(subscriptionId, sessionKey):
        '''
        Builds an UNSUBSCRIBE SSAP message.
        
        Keyword arguments:
        subscriptionId       -- the identifier of the subscription that will be cancelled.
        sessionKey           -- the identifier of the session.
        '''
        jsonObj = _SSAPMessageFactory.__buildMessageStructure(SSAP_MESSAGE_TYPE.UNSUBSCRIBE, sessionKey)  
        jsonObj["body"]["idSuscripcion"] = subscriptionId
        return _SSAPMessageFactory.__serializeMessage(jsonObj) 
    
class _SSAPMessageParser(object):
    '''
    A class that parses the SSAP messages received from the SIB. 
    
    In some cases, the serialized SSAP messages that are sent by the SIB are quite peculiar 
    and make the JSON parser of the standard Python library crash. The methods of this class convert 
    all the serialized JSON strings to the standard JSON syntax BEFORE parsing them. 
    '''
    
    # The following dictionaries are not necessary, but they allow us to separate the enum constants
    # and the SSAP strings.
    
    __message_directions = {"RESPONSE" : SSAP_MESSAGE_DIRECTION.RESPONSE,
                            "ERROR" : SSAP_MESSAGE_DIRECTION.ERROR}
    
    __error_codes = {"AUTENTICATION" : SSAP_ERROR_CODE.AUTHENTICATION,
                     "AUTHORIZATION" : SSAP_ERROR_CODE.AUTHORIZATION,
                     "PROCESSOR" : SSAP_ERROR_CODE.PROCESSOR,
                     "PERSISTENCE" : SSAP_ERROR_CODE.PERSISTENCE,
                     "PARSE_SQL" : SSAP_ERROR_CODE.PARSE_SQL,
                     "ONTOLOGY_NOT_FOUND" : SSAP_ERROR_CODE.ONTOLOGY_NOT_FOUND,
                     "SIB_DEFINED_QUERY_NOT_FOUND":SSAP_ERROR_CODE.SIB_DEFINED_QUERY_NOT_FOUND,
                     "OTHER":SSAP_ERROR_CODE.OTHER}       
    
    __message_types = {"JOIN" : SSAP_MESSAGE_TYPE.JOIN, "LEAVE" : SSAP_MESSAGE_TYPE.LEAVE,
                       "INSERT" : SSAP_MESSAGE_TYPE.INSERT, "UPDATE" : SSAP_MESSAGE_TYPE.UPDATE,
                       "DELETE" : SSAP_MESSAGE_TYPE.DELETE, "QUERY" : SSAP_MESSAGE_TYPE.QUERY,
                       "SUBSCRIBE" : SSAP_MESSAGE_TYPE.SUBSCRIBE, "UNSUBSCRIBE" : SSAP_MESSAGE_TYPE.UNSUBSCRIBE,
                       "INDICATION" : SSAP_MESSAGE_TYPE.INDICATION, "CONFIG" : SSAP_MESSAGE_TYPE.CONFIG}
    
    @staticmethod
    def parse(serializedData):
        '''
        Parses a serialized JSON message received from the SIB.
        
        Keyword arguments:
        serializedData: a serialized JSON messages
        '''
        dataWithoutEscapedQuotes = serializedData.replace(b"\\\\", b"")
        jsonMessage = json.loads(bytes2String(dataWithoutEscapedQuotes))
        
        jsonMessage["messageType"] = _SSAPMessageParser.__message_types[jsonMessage["messageType"]]
        jsonMessage["direction"] = _SSAPMessageParser.__message_directions[jsonMessage["direction"]]
        
        if (isinstance(jsonMessage["body"], six.string_types)):
            # Some ssap messages have a string (and therefore non-json) body. In that case, we'll have to
            # parse it too.
            if(jsonMessage["messageType"] == SSAP_MESSAGE_TYPE.INDICATION) :
                jsonMessage["body"] = six.b(jsonMessage["body"]).replace(b"\"[", b"[").replace(b"]\"", b"]")
                
            jsonMessage["body"] = json.loads(bytes2String(jsonMessage["body"]))
        
        if (SSAPEndpoint.hasOkField(jsonMessage) and jsonMessage["body"]["ok"] and 
                jsonMessage["messageType"] in [SSAP_MESSAGE_TYPE.INSERT, SSAP_MESSAGE_TYPE.UPDATE]) :
            # The message body data is a string or an array of strings. We must convert it to a JSON object
            patched_data = six.b(jsonMessage["body"]["data"])             
            patched_data = patched_data.replace(b"ObjectId", b"\"ObjectId")
            patched_data = patched_data.replace(b"(\"", b"('")
            patched_data = patched_data.replace(b"\")", b"')\"")           
            jsonMessage["body"]["data"] = json.loads(bytes2String(patched_data))

        if ("errorCode" in jsonMessage["body"] and not (jsonMessage["body"]["errorCode"] is None)):
            jsonMessage["body"]["errorCode"] = _SSAPMessageParser.__error_codes[jsonMessage["body"]["errorCode"]]
        
        return jsonMessage
