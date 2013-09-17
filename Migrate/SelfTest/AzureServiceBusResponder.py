"""
AzureServiceBusResponder
~~~~~~~~~~~~~~~~~

This module provides AzureServiceBusResponder class
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import logging
import traceback
import ServerResponder
from azure.servicebus import *

class AzureServiceBusResponder(ServerResponder.ServerResponder):
    """This responder checks Azure message bus for info"""
    def __init__ (self , namespace, account , issuer , topic, timeout_seconds = 300):
        """
        Constructor
            
        Args:
            timeout_seconds - timeout in seconds for each attempt to wait for server response

        """
        super(AzureServiceBusResponder , self).__init__(timeout_seconds)
        self.__topic = topic
        self.__busService = ServiceBusService(service_namespace=namespace, account_key=account, issuer=issuer)

    def waitResponseByMachineName(self , machinename):
        """waits till response is done: by using machine name"""
        subscription = 'MachineName' + machinename
        # create subscription for the specific message
        self.__busService.create_subscription(self.__topic, subscription)

        rule = Rule()
        rule.filter_type = 'SqlFilter'
        rule.filter_expression = 'serverid LIKE \'' +machinename + '%\''

        self.__busService.create_rule(self.__topic, subscription, 'MachineNameMessageFilter', rule)
        self.__busService.delete_rule(self.__topic, subscription, DEFAULT_RULE_NAME)

        #wait till it arrives
        result = False
        try:
            msg = self.__busService.receive_subscription_message(self.__topic, subscription , self.getTimeout())
            if msg:
                logging.debug("Message arrived: " + msg.body)
                result = True
            else:
                result = False
        except Exception as e:
            logging.warning("! Cannot get message from subscription" + str(e));
            logging.warning(traceback.format_exc())

        #delete the subscription
        bus_service.delete_subscription(self.__topic, subscription)
        return True

    def waitResponseByIp(self , ip):
        """waits till response is done by ip"""
        raise NotImplementedError
        return 


