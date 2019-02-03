#!/usr/bin/env python
# -*- mode:python; coding:utf-8 -*-

"""schema_registry.manager wraps a Confluent Schema Registry.

The module provides a class, SchemaRegistryManager, with a number of methods
to register, query and delete Avro schemas in Confluent Schema Registry.
"""

__author__     = "Miguel-Angel Monjas"
__copyright__  = "Copyright 2018, Miguel-Angel Monjas"
__license__    = "Apache License 2.0"
__version__    = "0.1.1"
__maintainer__ = "Miguel-Angel Monjas"
__email__      = "mmonjas@gmail.com"
__status__     = "Development"

import avro.schema
from avro.schema import SchemaParseException
from kafkaconnect_utils.config import Config
from kafkaconnect_utils.exceptions import NoSchemaRegistryAvailable
import json
import requests

class SchemaRegistryManager (object):
    """
    Class for handling a Confluent Schema Registry
    """

    def __init__(self, manager_hostname=Config["schema_registry_default_host"],
                       manager_port=Config["schema_registry_default_port"]):
        """
        The constructor for the SchemaRegistryManager class.

        Parameters:
            manager_hostname(str): The Schema Registry hostname.
            manager_port(str): The port where Schema Registry runs. Default value is '8081'.

        Raises:
            ConnectionError: An error ocurred if no connectivity is available, by 
                attempting to access a Google machine.
            NoSchemaRegistryAvailable: An error ocurred if the Schema Registry is not
                available (through the get_config() method).
        """

        self._manager = manager_hostname
        self._manager_port = manager_port
        self._manager_ip = f"{manager_hostname}:{manager_port}"

        _ = self.get_config()

    def get_config(self):
        """
        The method to access the Schema Registry configuration.

        Returns:
            config (dict): The Schema Registry configuration, as a dictionary.

        Raises:
            NoSchemaRegistryAvailable: An error ocurred if the Schema Registry is not 
                available.
            HTTPError: An error ocurred if the request didn't returned a 200 status 
                code (OK)
        """

        try:
            r = requests.get(f"http://{self._manager_ip}/config")
            r.raise_for_status()
        except ConnectionError:
            raise NoSchemaRegistryAvailable
        except Exception as e:
            raise e
        return r.json()

    def get_subjects(self):
        """
        The method to get the Subjects registered at the Schema Registry.

        Returns:
            subjects (list): A list passing on the subjects available at the Schema 
                Registry.

        Raises:
            NoSchemaRegistryAvailable: An error ocurred if the Schema Registry is not 
                available.
            HTTPError: An error ocurred if the request didn't returned a 200 status 
                code (OK)
        """
        
        try :
            r = requests.get(f"http://{self._manager_ip}/subjects")
            r.raise_for_status()
        except ConnectionError:
            raise NoSchemaRegistryAvailable
        except Exception as e:
            raise e
        return r.json()

    def get_subject_versions(self, subject):
        """
        The method to get a Subject versions.

        Parameters:
            subject (str): The Subject the request refers to.

        Returns:
            versions (list): A list of available versions.

        Raises:
            ValueError: An error ocurred if you did not provide a subjec.
            NoSchemaRegistryAvailable: An error ocurred if the Schema Registry is not 
                available.
            HTTPError: An error ocurred if the request didn't returned a 200 status 
                code (OK)
        """
        
        if len(subject) == 0:
            raise ValueError
        try:
            r = requests.get(f"http://{self._manager_ip}/subjects/{subject}/versions")
            r.raise_for_status()
        except ConnectionError:
            raise NoSchemaRegistryAvailable
        except Exception as e:
            raise e
        return r.json()

    def get_subject_schema(self, subject, version=None):
        """
        The method to get a schema of a Subject version.

        The method returns the schema associated to the most recent version unless a specific
        version identifier is provided.

        Parameters:
            subject (str): The Subject the request refers to.
            version (int): The Subject version the request refers to.

        Returns:
            schema (dict): A JSON-encoded Avro schema.

        Raises:
            ValueError: An error ocurred if you did not provide a subjec.
            NoSchemaRegistryAvailable: An error ocurred if the Schema Registry is not
                available.
            HTTPError: An error ocurred if the request didn't returned a 200 status 
                code (OK)
        """
        
        if len(subject) == 0:
            raise ValueError
        if version is None:
            version = max(self.get_subject_versions(subject))
        try:
            r = requests.get(f"http://{self._manager_ip}/subjects/{subject}/versions/{version}/schema")
            r.raise_for_status()
        except ConnectionError:
            raise NoSchemaRegistryAvailable
        except Exception as e:
            raise e
        return r.json()

    def get_subject_schema_id(self, subject, version=None):
        """
        The method to get a the schema identifier of a Subject version.

        The method returns the identifier of the schema associated to the most recent version unless
        a specific version identifier is provided.

        Parameters:
            subject (str): The Subject the request refers to.
            version (int): The Subject version the request refers to.

        Returns:
            id (int): The identifier of the Avro schema.

        Raises:
            ValueError: An error ocurred if you did not provide a subjec.
            NoSchemaRegistryAvailable: An error ocurred if the Schema Registry is not
                available.
            HTTPError: An error ocurred if the request didn't returned a 200 status 
                code (OK)
        """
        
        if len(subject) == 0:
            raise ValueError
        if version is None:
            version = max(self.get_subject_versions(subject))
        try:
            r = requests.get(f"http://{self._manager_ip}/subjects/{subject}/versions/{version}")
            r.raise_for_status()
        except ConnectionError:
            raise NoSchemaRegistryAvailable
        except Exception as e:
            raise e
        return r.json()["id"]

    def get_schema(self, schema_id):
        """
        The method to a schema from the Schema Registry.

        Parameters:
            schema_id (str): The identifier of the schema the request refers to.

        Returns:
            schema (dict): A JSON-encoded Avro schema.

        Raises:
            ValueError: An error ocurred if you did not provide an integer as schema identifier.
            NoSchemaRegistryAvailable: An error ocurred if the Schema Registry is not
                available.
            HTTPError: An error ocurred if the request didn't returned a 200 status 
                code (OK)
        """
        
        if not isinstance(schema_id, int):
            raise ValueError
        try:
            r = requests.get(f"http://{self._manager_ip}/schemas/ids/{schema_id}")
            r.raise_for_status()
        except ConnectionError:
            raise NoSchemaRegistryAvailable
        except Exception as e:
            raise e
        return json.loads(r.json()['schema'])

    def register_schema(self, subject, avro_schema):
        """
        The method register a schema at a Schema Registry.

        Parameters:
            subject (str): The subject the schema will be associated to.
            avro_schema (str or dict): A JSON-encoded Avro schema. It is possible to pass
                it to the method by means of a dictionary or as a string encoding a
                JSON dictionary.

        Returns:
            id (int): The schema identifier (different from the subject version).

        Raises:
            ValueError: An error ocurred if you did not provide a subject or schema.
            TypeError: An error ocurred if the provided schema is not a string or a
                dictionary.
            SchemaParseException: An error ocurred if the local validation of the Avro schema 
                is not successful.
            NoSchemaRegistryAvailable: An error ocurred if the Schema Registry is not 
                available.
            HTTPError: An error ocurred if the request to the Schema Regggistry didn't returned 
                a 200 status code (OK). Relevant codes are 409 (Incompatible Avro schema) and 
                422 (Invalid Avro schema).
        """
        
        if len(subject) == 0 or len(avro_schema) == 0:
            raise ValueError
        # Avro schema local validation
        try:
            if type(avro_schema) is str:
                avro.schema.Parse(avro_schema)
                avro_schema = json.loads(avro_schema)
            elif type(avro_schema) is dict:
                schema = json.dumps(avro_schema)
                avro.schema.Parse(schema)
            else :
                raise TypeError
        except (SchemaParseException, KeyError) as e:
            raise SchemaParseException

        headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
        data = json.dumps({'schema': json.dumps(avro_schema)})
        try :
            r = requests.post(f"http://{self._manager_ip}/subjects/{subject}/versions", json=data, headers=headers)
            r.raise_for_status()
        except ConnectionError:
            raise NoSchemaRegistryAvailable
        except Exception as e:
            raise e
        return r.json()["id"]

    def delete_subject(self, subject):
        """
        The method to delete a Subject.

        Parameters:
            subject (str): The Subject the request refers to.

        Returns:
            versions (list): A list passing on the identifier of the deleted version (int)

        Raises:
            ValueError: An error ocurred if you did not provide a subject.
            NoSchemaRegistryAvailable: An error ocurred if the Schema Registry is not
                available.
            HTTPError: An error ocurred if the request didn't returned a 200 status 
                code (OK). Relevant codes are 404 (subject not found)
        """
        
        if len(subject) == 0:
            raise ValueError
        try:
            r = requests.delete(f"http://{self._manager_ip}/subjects/{subject}")
            r.raise_for_status()
        except ConnectionError:
            raise NoSchemaRegistryAvailable
        except Exception as e:
            raise e
        return r.json()

    def delete_subject_version(self, subject, version=None):
        """
        The method to delete a version of a Subject.

        Parameters:
            subject (str): The Subject the request refers to.

        Returns:
            version (ini): Identifier of the deleted version (int)

        Raises:
            ValueError: An error ocurred if you did not provide a subject.
            NoSchemaRegistryAvailable: An error ocurred if the Schema Registry is not
                available.
            HTTPError: An error ocurred if the request didn't returned a 200 status 
                code (OK). Relevant codes are 404 (subject or version not found), and
                422 (invalid version)
        """
        
        if len(subject) == 0:
            raise ValueError
        if version is None:
            version = max(self.get_subject_versions(subject))
        try:
            r = requests.delete(f"http://{self._manager_ip}/subjects/{subject}/versions/{version}")
            r.raise_for_status()
        except ConnectionError:
            raise NoSchemaRegistryAvailable
        except Exception as e:
            raise e
        return r.json()