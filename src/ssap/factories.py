# -*- coding: utf8 -*-
'''
 Python SSAP API
 Version 1.5
 
 © Indra Sistemas, S.A.
 2014  SPAIN
  
 All rights reserved
'''
from ssap.implementations.websockets import WebsocketBasedSSAPEndpoint, WebsocketConnectionData

class SSAPEndpointFactory(object):
    '''
    The SSAP endpoint factory. Currently, it can only instantiate websocket-based endpoints.
    '''
    
    @staticmethod
    def buildWebsocketBasedSSAPEndpoint(server_url, callback, debugMode=False):
        '''
        Instantiates a websocket-based SSAp endpoint.
        
        Keyword arguments:
        server_url     -- the URl of the websocket server.
        callback       -- the callback that will process the incoming SSAP messages.
        debugMode      -- enables debug log messages.
        '''
        connectionData = WebsocketConnectionData(server_url)
        endpoint = WebsocketBasedSSAPEndpoint(callback, connectionData, debugMode)
        return endpoint