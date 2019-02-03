#!/usr/bin/env python
# -*- mode:python; coding:utf-8 -*-

"""connect.manager wraps an Apache Kafka Connect REST interface.

The module provides a class, KafkaConnectManager, with a number of methods
to handle Kafka Connect connectors at a Confluent desployment.
"""

__author__     = "Miguel-Angel Monjas"
__copyright__  = "Copyright 2018, Miguel-Angel Monjas"
__license__    = "Apache License 2.0"
__version__    = "0.1.1"
__maintainer__ = "Miguel-Angel Monjas"
__email__      = "mmonjas@gmail.com"
__status__     = "Development"

from kafkaconnect_utils.config import Config
from kafkaconnect_utils.exceptions import NoConnectorAvailable, NoConnectServerAvailable
from requests.exceptions import HTTPError as HTTPError, ConnectionError as ConnectionError
import copy
from itertools import chain
import json
import requests
import string
from time import sleep

valid_connect_types = ["source", "sink", "all"]

class KafkaConnectManager (object):
    """
    Class for handling a Confluent Kafka Connect server
    """

    def __init__(self, manager_hostname=Config["connect_default_host"],
                       manager_port=Config["connect_default_port"]):
        """
        The constructor for the KafkaConnectManager class. It verifies whether connectivity
        and the Connect server are available.

        Parameters:
            manager_hostname (str): The Connect hostname.
            manager_port (str): The port where the Connect server runs.

        Raises:
            ConnectionError: An error ocurred if no connectivity is available, by
                attempting to access a Google machine.
            NoConnectServerAvailable: An error ocurred if the Connect server is
                not available (through the get_connector() method).
        """

        self._manager = manager_hostname
        self._manager_port = manager_port
        self._manager_ip = f"{manager_hostname}:{manager_port}"
        self._headers = {'accept': 'application/json'}

        try:
            r = requests.get(f"http://{self._manager_ip}", headers=self._headers)
            r.raise_for_status()
        except ConnectionError:
            raise NoConnectServerAvailable
        except HTTPError:
            # We're only testing whether the server is available
            pass
        except Exception as e:
            raise e

    def get_connectors(self, type_="source"):
        """
        The method to get the connectors registered at the Connect server.

        Parameters:
            type_ (str): type of connector. Default value is "source". The other
                allowed value is "sink"

        Returns:
            connectors (list): A list passing on the connectors available at the
                Connect server.

        Raises:
            ValueError: An error ocurred if the specified connector type is not
                supported (only "source" and "sink" are allowed)
            NoConnectServerAvailable: An error ocurred if the Connect server is
                not available.
            HTTPError: An error ocurred if the request didn't returned a 200
                status code (OK)
        """

        if type_ not in valid_connect_types:
            raise ValueError
        try :
            r = requests.get(f"http://{self._manager_ip}/connectors", headers=self._headers)
            r.raise_for_status()
        except ConnectionError:
            raise NoConnectServerAvailable
        except Exception as e:
            raise e
        response = []
        for connector in r.json() :
            info = self.get_connector_info(connector)
            if type_ == "all" or info["type"] == type_:
                response.append(info)
        return response

    def get_connector_info(self, id):
        """
        The method to get the information about a specific connector
        registered at the Connect server.

        Parameters:
            id (str): connector identifier. It is the field 'name' in
                the connector info

        Returns:
            info (dict): A dictionary including the connector information:
                name (str): connector name. It is the same value provided as
                    argument when calling the function
                type (str): type of connector (values can be 'source' or 'sink').
                class (str): connector technology (current values are those
                    supported by Concluent: JDBC, ActiveMQ, S3, Elasticsearch,
                    HDFS, JMS, and IBM MQ.
                config (dict): full connector configuration info. The content
                    depend on the type and class of connector.
                state (str): connector state (valid values are UNASSIGNED,
                    RUNNING, PAUSED, and FAILED).
                vendor (str): the connector vendor, if known.

                If the connector belongs to type JDBC and class source:
                tables (list): list of tables whose changes are being tracked.
                    It may be empty is the event listener depends on a query.
                topics (list): list of topics the connector is producing to.
                subjects (list): list of subjects the connector is managing.
                    There are two subjects per topic (one of the form
                    [topic]-key and other as [topic]-value)

        Raises:
            ValueError: An error ocurred when no connector identifier is
                provided
            NoConnectServerAvailable: An error ocurred if the Connect server is
                not available.
            NoConnectorAvailable: An error ocurred if the connector the request
                refers to is not available.
            HTTPError: An error ocurred if the request didn't returned a 200
                status code (OK)
        """

        if len(id) == 0:
            raise ValueError
        while True:
            try:
                r = requests.get(f"http://{self._manager_ip}/connectors/{id}", headers=self._headers)
                r.raise_for_status()
            except ConnectionError:
                raise NoConnectServerAvailable
            except HTTPError:
                if r.status_code == 404:
                    raise NoConnectorAvailable
                elif r.status_code == 409:
                    # {"error_code":409,
                    # "message":"Cannot complete request momentarily due to stale configuration (typically caused by a concurrent config change)"}
                    sleep(1)
                    continue
                else :
                    raise HTTPError
            except Exception as e:
                raise e
            break

        connector_info = copy.deepcopy(r.json())
        connector_info["vendor"] = "Unknown"
        connector_info["state"] = self.get_connector_status(id)
        if "Source" in connector_info["config"]["connector.class"]:
            connector_info["type"] = "source"
        elif "Sink" in connector_info["config"]["connector.class"]:
            connector_info["type"] = "sink"
        else :
            connector_info["type"] = "undetermined"
        if "confluent" in connector_info["config"]["connector.class"]:
            connector_info["vendor"] = "Confluent"
        if connector_info["config"]["connector.class"] == "io.confluent.connect.jdbc.JdbcSourceConnector":
            connector_info["class"] = "JDBC"
            if "mysql" in connector_info["config"]["connection.url"]:
                connector_info["jdbc_type"] = "mysql"
                connector_info["jdbc_location"] = connector_info["config"]["connection.url"].split("/")[2]
                connector_info["jdbc_database"] = connector_info["config"]["connection.url"].split("/")[3].split("?")[0]
            elif "oracle" in connector_info["config"]["connection.url"]:
                connector_info["jdbc_type"] = "oracle"
                connector_info["jdbc_location"] = connector_info["config"]["connection.url"].split("@")[1].split("/")[0]
                connector_info["jdbc_sid"] = connector_info["config"]["connection.url"].split("/")[-1]
            elif "postgresql" in connector_info["config"]["connection.url"]:
                connector_info["jdbc_type"] = "postgresql"
                connector_info["jdbc_location"] = connector_info["config"]["connection.url"].split("/")[2]
                connector_info["jdbc_database"] = connector_info["config"]["connection.url"].split("/")[-1]
            elif "sqlserver" in connector_info["config"]["connection.url"]:
                connector_info["jdbc_type"] = "sqlserver"
                connector_info["jdbc_location"] = connector_info["config"]["connection.url"].split(";")[0].split("/")[-1]
                connector_info["jdbc_database"] = connector_info["config"]["connection.url"].split(";")[-1].split("=")[1]
            elif "mariadb" in connector_info["config"]["connection.url"]:
                connector_info["jdbc_type"] = "mariadb"
                connector_info["jdbc_location"] = connector_info["config"]["connection.url"].split("/")[-2]
                connector_info["jdbc_database"] = connector_info["config"]["connection.url"].split("/")[-1]

            if "table.whitelist" in connector_info["config"]:
                connector_info["tables"] = [table for table in connector_info["config"]["table.whitelist"].split(',')]
                connector_info["topics"] = [f"{connector_info['config']['topic.prefix']}{table}" for table in connector_info["tables"]]
            else :
                # query
                connector_info["topics"] = connector_info["config"]["topic.prefix"]
                connector_info["tables"] = []
            f = lambda x: f"{x}-key"
            g = lambda x: f"{x}-value"
            connector_info["subjects"] = list(chain.from_iterable((f(x), g(x)) for x in connector_info["topics"]))
        elif connector_info["config"]["connector.class"] == "io.confluent.connect.activemq.ActiveMQSourceConnectorConfig":
            connector_info["class"] = "ActiveMQ"
        elif connector_info["config"]["connector.class"] == "io.confluent.connect.s3.S3SinkConnector":
            connector_info["class"] = "S3"
        elif connector_info["config"]["connector.class"] == "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector":
            connector_info["class"] = "Elasticsearch"
        elif connector_info["config"]["connector.class"] == "io.confluent.connect.hdfs.HdfsSinkConnector":
            connector_info["class"] = "HDFS"
        elif connector_info["config"]["connector.class"] == "io.confluent.connect.ibm.mq.IbmMQSourceConnectorConfig":
            connector_info["class"] = "IBM MQ"
        elif connector_info["config"]["connector.class"] == "io.confluent.connect.jdbc.JdbcSinkConnector":
            connector_info["class"] = "JDBC"
        elif connector_info["config"]["connector.class"] == "io.confluent.connect.jms.JmsSourceConnector":
            connector_info["class"] = "JMS"
        else :
            connector_info["topics"] = []
        return connector_info

    def get_connector_status(self, id):
        """
        The method to get the status of a specific connector

        Parameters:
            id (str): connector identifier.

        Returns:
            state (str): ond of the following values: RUNNING, PAUSED, UNASSIGNED or FAILED.

        Raises:
            ValueError: An error ocurred when no connector identifier is
                provided
            NoConnectServerAvailable: An error ocurred if the Connect server is
                not available.
            NoConnectorAvailable: An error ocurred if the connector the request
                refers to is not available.
            HTTPError: An error ocurred if the request didn't returned a 200
                status code (OK)
        """

        if len(id) == 0:
            raise ValueError
        try:
            r = requests.get(f"http://{self._manager_ip}/connectors/{id}/status", headers=self._headers)
            r.raise_for_status()
        except ConnectionError:
            raise NoConnectServerAvailable
        except HTTPError:
            if r.status_code == 404:
                raise NoConnectorAvailable
            else :
                raise HTTPError
        except Exception as e:
            raise e
        return r.json()["connector"]["state"]

    def load_connector(self, id, config):
        """
        The method to create and load a connector in the Connect platform

        Parameters:
            id (str): connector name.
            config (str or dict): connector configuration. If a string is used, it
                must be the result of the enconding of a dictionary

        Returns:
            result (bool): creation process result

        Raises:
            ValueError: An error ocurred when no connector identifier is
                provided
            NoConnectServerAvailable: An error ocurred if the Connect server is
                not available.
            HTTPError: An error ocurred if the request didn't returned a 200
                status code (OK)
        """

        if len(id) == 0 or len(config) == 0:
            raise ValueError
        if isinstance(config, str):
            try:
                config = json.loads(config)
            except :
                raise ValueError
        elif not isinstance(config, dict) :
            raise ValueError
        data = {"name": id,
                "config": config}
        try:
            r = requests.post(f"http://{self._manager_ip}/connectors", headers=self._headers, json=data)
            r.raise_for_status()
        except ConnectionError:
            raise NoConnectServerAvailable
        except Exception as e:
            raise e
        return True

    def pause_connector(self, id):
        """
        The method to pause a specific connector

        Parameters:
            id (str): connector identifier.

        Returns:
            result (bool): pause process result

        Raises:
            ValueError: An error ocurred when no connector identifier is
                provided
            NoConnectServerAvailable: An error ocurred if the Connect server is
                not available.
            NoConnectorAvailable: An error ocurred if the connector the request
                refers to is not available.
            HTTPError: An error ocurred if the request didn't returned a 200
                status code (OK)
        """

        if len(id) == 0:
            raise ValueError
        try :
            r = requests.put(f"http://{self._manager_ip}/connectors/{id}/pause", headers=self._headers)
            r.raise_for_status()
        except ConnectionError:
            raise NoConnectServerAvailable
        except Exception as e:
            raise e
        return True

    def resume_connector(self, id):
        """
        The method to resume a paused connector

        Parameters:
            id (str): connector identifier.

        Returns:
            result (bool): resume process result

        Raises:
            ValueError: An error ocurred when no connector identifier is
                provided
            NoConnectServerAvailable: An error ocurred if the Connect server is
                not available.
            NoConnectorAvailable: An error ocurred if the connector the request
                refers to is not available.
            HTTPError: An error ocurred if the request didn't returned a 200
                status code (OK)
        """

        if len(id) == 0:
            raise ValueError
        try:
            r = requests.put(f"http://{self._manager_ip}/connectors/{id}/resume", headers=self._headers)
            r.raise_for_status()
        except ConnectionError:
            raise NoConnectServerAvailable
        except HTTPError:
            if r.status_code == 404:
                raise NoConnectorAvailable
            else :
                raise HTTPError
        except Exception as e:
            raise e
        return True

    def restart_connector(self, id):
        """
        The method to restart a specific connector

        Parameters:
            id (str): connector identifier.

        Returns:
            result (bool): restart process result

        Raises:
            ValueError: An error ocurred when no connector identifier is
                provided
            NoConnectServerAvailable: An error ocurred if the Connect server is
                not available.
            NoConnectorAvailable: An error ocurred if the connector the request
                refers to is not available.
            HTTPError: An error ocurred if the request didn't returned a 200
                status code (OK)
                
        To-do: verify the HTTP method
        """

        if len(id) == 0:
            raise ValueError
        try :
            r = requests.post(f"http://{self._manager_ip}/connectors/{id}/restart", headers=self._headers)
            r.raise_for_status()
        except ConnectionError:
            raise NoConnectServerAvailable
        except HTTPError:
            if r.status_code == 404:
                raise NoConnectorAvailable
            else :
                raise HTTPError
        except Exception as e:
            raise e
        return True

    def delete_connector(self, id):
        """
        The method to delete a specific connector

        Parameters:
            id (str): connector identifier.

        Returns:
            result (bool): deletion process result

        Raises:
            ValueError: An error ocurred when no connector identifier is
                provided
            NoConnectServerAvailable: An error ocurred if the Connect server is
                not available.
            NoConnectorAvailable: An error ocurred if the connector the request
                refers to is not available.
            HTTPError: An error ocurred if the request didn't returned a 200
                status code (OK)
        """

        if len(id) == 0:
            raise ValueError
        try:
            r = requests.delete(f"http://{self._manager_ip}/connectors/{id}/", headers=self._headers)
            r.raise_for_status()
        except ConnectionError:
            raise NoConnectServerAvailable
        except HTTPError:
            if r.status_code == 404:
                raise NoConnectorAvailable
            else :
                raise HTTPError
        except Exception as e:
            raise e
        return True