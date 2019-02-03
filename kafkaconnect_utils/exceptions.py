#!/usr/bin/env python
# -*- mode:python; coding:utf-8 -*-

from requests.exceptions import ConnectionError as ConnectionError
from requests.exceptions import HTTPError as HTTPError

class NoConnectServerAvailable (ConnectionError):
    """The Connect server is not available"""

class NoSchemaRegistryAvailable(ConnectionError):
    """The Schema Registry is not available"""
	
class NoConnectorAvailable (HTTPError):
    """The connector is not available in the Connect platform"""