## Kafka Connect Utils

This package provide the means to interact with a **Confluent Schema Registry** (and thus register, query and delete Avro 
schemas and subjects) and with a **Confluent Kafka Connect server** (and thus manage connectors).

*(documentation created with `pydoc-markdownp`)*

---
## Package creation
Follow the [guidelines](https://packaging.python.org/tutorials/installing-packages/) so that `setuptools` and 
`wheel` are installed.

We have chosen to use Wheel files to distribute the package (see 
[here](https://packaging.python.org/tutorials/distributing-packages/)). To create the wheel file, go to the root folder 
and run:
```bash
python setup.py bdist_wheel
```

Do not activate the `universal` flag, as this package runs only in Python 3. Include in your `setup.cfg` file:

```bash
universal=0
```

Once your Wheel file is created, you can install it in any Python 3 environment by running:

```bash
pip install kafkaconnect_utils-[lastest version]-py3-none-any.whl
```

## Functionality
The `kafkaconnect_utils` package provides two classes, `SchemaRegistryManager` (with a number of methods for registering, 
querying and deleting subjects and Avro schemas in a Confluent Schema Registry) and `ConnectManager` (with functions
for querying connector information and managing connector in a Confluent Connect server).

## Specification: `kafkaconnect_utils.schema_registry_manager`

`kafkaconnect_utils.schema_registry_manager` wraps a Confluent Schema Registry so that Python classes can be used instead
of accessing the Schema Registry REST interface.

The module provides a class, `SchemaRegistryManager`, with a number of methods to register, query and delete 
Subjects and associated Avro schemas in a Confluent Schema Registry. In a Confluent Schema Registry, all Avro
schemas must be associated to a Subject, which contains a versioned list of Avro schemas. The last added schema
is the active Avro schema associated to the Subject.

You can instantiate a `SchemaRegistryManager` object accessing a Schema Registry available at `localhost`in the 
following way:

```python
from kafkaconnect_utils.schema_registry_manager import SchemaRegistryManager

manager = SchemaRegistryManager()
``` 
<h3 id="schema_registry.manager.SchemaRegistryManager">SchemaRegistryManager</h3>

```python
SchemaRegistryManager(self, manager_host='localhost', manager_port='8081')
```

Class for handling a Confluent Schema Registry. The constructor works in the following way:

*Parameters*:
- `manager_host` (`str`): The hostname where the Confluent Schema Manager is available. Default value is 'localhost'.
- `manager_port` (`str`): The port where the Confluent Schema Manager is available. Default value is '8081'.

*Returns*:
- `manager` (`SchemaRegistryManager`): An object belonging to the `SchemaRegistryManager` class.

*Raises*:
- `ConnectionError`: An error ocurred if no Internet connection is available.
 

<h4 id="kafkaconnect_utils.schema_registry_manager.SchemaRegistryManager.get_config">get_config</h4>

```python
SchemaRegistryManager.get_config(self)
```

The method to access the Schema Registry configuration.

*Returns*:
- `config` (`dict`): The Schema Registry configuration, as a dictionary.

*Raises*:
- `NoSchemaRegistryAvailable`: An error ocurred if the Schema Registry is not
        available.
- `HTTPError`: An error ocurred if the request did not returned a 200 status
        code (OK)

<h4 id="kafkaconnect_utils.schema_registry_manager.SchemaRegistryManager.get_subjects">get_subjects</h4>

```python
SchemaRegistryManager.get_subjects(self)
```

The method to get the Subjects registered at the Schema Registry.

*Returns*:
- `subjects` (`list`): A list passing on the subjects (`str`) available at the Schema Registry.

*Raises*:
- `NoSchemaRegistryAvailable`: An error ocurred if the Schema Registry is not available.
- `HTTPError`: An error ocurred if the request did not returned a 200 status code (OK)

<h4 id="kafkaconnect_utils.schema_registry_manager.SchemaRegistryManager.get_subject_versions">get_subject_versions</h4>

```python
SchemaRegistryManager.get_subject_versions(self, subject)
```

The method to get all the versions of a given Subject.

*Parameters*:
- `subject` (`str`): The Subject the request refers to.

*Returns*:
- `versions` (`list`): A list of available versions (`int`).

*Raises*:
- `ValueError`: An error ocurred if you did not provide a subject.
- `NoSchemaRegistryAvailable`: An error ocurred if the Schema Registry is not available.
- `HTTPError`: An error ocurred if the request did not returned a 200 status code (OK)

<h4 id="kafkaconnect_utils.schema_registry_manager.SchemaRegistryManager.get_subject_schema">get_subject_schema</h4>

```python
SchemaRegistryManager.get_subject_schema(self, subject, version=None)
```

The method to get a schema of a Subject version.

The method returns the schema associated to the most recent version unless a specific
version identifier is provided.

*Parameters*:
- `subject` (`str`): The Subject the request refers to.
- `version` (`int`): The Subject version the request refers to.

*Returns*:
- `schema` (`dict`): A JSON-encoded Avro schema.

*Raises*:
- `ValueError`: An error ocurred if you did not provide a subject.
- `NoSchemaRegistryAvailable`: An error ocurred if the Schema Registry is not available.
- `HTTPError`: An error ocurred if the request did not returned a 200 status code (OK).

<h4 id="kafkaconnect_utils.schema_registry_manager.SchemaRegistryManager.get_subject_schema_id">get_subject_schema_id</h4>

```python
SchemaRegistryManager.get_subject_schema_id(self, subject, version=None)
```

The method to get the Avro schema identifier of a Subject version.

The method returns the identifier of the schema associated to the most recent Subject version 
unless a specific version identifier is provided in the request.

*Parameters*:
- `subject` (`str`): The Subject the request refers to.
- `version` (`int`): The Subject version the request refers to.

*Returns*:
- `id` (`int`): The identifier of the Avro schema.

*Raises*:
- `ValueError`: An error ocurred if you did not provide a subjec.
- `NoSchemaRegistryAvailable`: An error ocurred if the Schema Registry is not available.
- `HTTPError`: An error ocurred if the request did not returned a 200 status code (OK)

<h4 id="kafkaconnect_utils.schema_registry_manager.SchemaRegistryManager.get_schema">get_schema</h4>

```python
SchemaRegistryManager.get_schema(self, schema_id)
```

The method to an Avro schema from the Schema Registry.

*Parameters*:
- `schema_id` (`int`): The identifier of the schema the request refers to.

*Returns*:
- `schema` (`dict`): A JSON-encoded Avro schema.

*Raises*:
- `ValueError`: An error ocurred if you did not provide an integer as schema identifier.
- `NoSchemaRegistryAvailable`: An error ocurred if the Schema Registry is not available.
- `HTTPError`: An error ocurred if the request did not returned a 200 status code (OK)

<h4 id="kafkaconnect_utils.schema_registry_manager.SchemaRegistryManager.register_schema">register_schema</h4>

```python
SchemaRegistryManager.register_schema(self, subject, avro_schema)
```

The method registers an Avro schema at a Schema Registry.

*Parameters*:
- `subject` (`str`): The subject the schema will be associated to.
- `avro_schema` (`str` or `dict`): A JSON-encoded Avro schema. It is possible to pass it to 
        the method by means of a dictionary or as a string encoding a JSON dictionary.

*Returns*:
- `id` (`int`): The schema identifier (different from the subject version).

*Raises*:
- `ValueError`: An error ocurred if you did not provide a subject or schema.
- `TypeError`: An error ocurred if the provided schema is not a string or a dictionary.
- `SchemaParseException`: An error ocurred if the local validation of the Avro schema
        is not successful.
- `NoSchemaRegistryAvailable`: An error ocurred if the Schema Registry is not available.
- `HTTPError`: An error ocurred if the request to the Schema Regggistry did not returned
        a 200 status code (OK). Relevant codes are 409 (Incompatible Avro schema) and
        422 (Invalid Avro schema).

<h4 id="kafkaconnect_utils.schema_registry_manager.SchemaRegistryManager.delete_subject">delete_subject</h4>

```python
SchemaRegistryManager.delete_subject(self, subject)
```

The method to delete a Subject.

*Parameters*:
- `subject` (`str`): The Subject the request refers to.

*Returns*:
- `versions` (`list`): A list passing on the identifier of the deleted version (`int`)

*Raises*:
- `ValueError`: An error ocurred if you did not provide a subject.
- `NoSchemaRegistryAvailable`: An error ocurred if the Schema Registry is not available.
- `HTTPError`: An error ocurred if the request did not returned a 200 status
        code (OK). Relevant codes are 404 (subject not found)

<h4 id="kafkaconnect_utils.schema_registry_manager.SchemaRegistryManager.delete_subject_version">delete_subject_version</h4>

```python
SchemaRegistryManager.delete_subject_version(self, subject, version=None)
```

The method to delete a version of a Subject.

*Parameters*:
- `subject` (`str`): The Subject the request refers to.

*Returns*:
- `version` (`int`): Identifier of the deleted version.

*Raises*:
- `ValueError`: An error ocurred if you did not provide a subject.
- `NoSchemaRegistryAvailable`: An error ocurred if the Schema Registry is not available.
- `HTTPError`: An error ocurred if the request did not returned a 200 status
        code (OK). Relevant codes are 404 (subject or version not found), and
        422 (invalid version)


## Specification: `kafkaconnect_utils.connect_manager`

`kafkaconnect_utils.schema_registry_manager` wraps a Confluent Kafka Connect server REST interface.

The module provides a class, `ConnectManager`, with a number of methods
to handle Kafka Connect connectors at a Confluent desployment.

You can instantiate an object in the following way:

```python
from kafkaconnect_utils.connect_manager import ConnectManager
manager = ConnectManager()
``` 
<h3 id="schema_registry.manager.SchemaRegistryManager">ConnectManager</h3>

```python
ConnectManager(self, manager_host='localhost', manager_port='8083')
```

Class for handling a Confluent Connect server. The constructor:

*Parameters*:
* `manager_host` (`str`): The hostname where the Confluent Connect server is available. Default value is 'localhost'.
* `manager_port` (`str`): The port where the Confluent Connect server is available. Default value is '8083'.

*Returns*:
* `manager` (`ConnectManager`): An object belonging to the `ConnectManager`.

*Raises*:
* `ConnectionError`: An error ocurred if no Internet connection is available.

<h4 id="kafkaconnect_utils.connect_manager.ConnectManager.get_connectors">get_connectors</h4>

```python
ConnectManager.get_connectors(self, type_='source')
```

The method to get the connectors registered at the Connect server.

*Parameters*:
- `type_` (`str`): type of connector. Default value is "source". The other
        allowed value is "sink"

*Returns*:
- `connectors` (`list`): A list passing on the connectors available at the
        Connect server.

*Raises*:
- `ValueError`: An error ocurred if the specified connector type is not
        supported (only "source" and "sink" are allowed)
- `NoConnectServerAvailable`: An error ocurred if the Connect server is
        not available.
- `HTTPError`: An error ocurred if the request did not returned a 200
        status code (OK)

<h4 id="kafkaconnect_utils.connect_manager.ConnectManager.get_connector_info">get_connector_info</h4>

```python
ConnectManager.get_connector_info(self, id)
```

The method to get the information about a specific connector
registered at the Connect server.

*Parameters*:
- `id` (`str`): connector identifier.

*Returns*:
- `info` (`dict`): A dictionary including the connector information:
    - `name` (`str`): connector name
    - `type` (`str`): type of connector (values can be `source` or `sink`).
    - `class` (`str`): connector technology (current values are those
            supported by Concluent: `JDBC`, `ActiveMQ`, `S3`, `Elasticsearch`,
            `HDFS`, `JMS`, and `IBM MQ`.
    - `config` (`dict`): full connector configuration info. The content
            depend on the type and class of connector.
    - `state` (`str`): connector status (valid values are `UNASSIGNED`,
            `RUNNING`, `PAUSED`, and `FAILED`).

    If the connector belongs to type JDBC and class source:
    - `tables` (`list`): list of tables whose changes are being tracked.
            It may be empty is the event listener depends on a query.
    - `topics` (`list`): list of topics the connector is producing to.
    - `subjects` (`list`): list of subjects the connector is managing.
            There are two subjects per topic (one of the form `[topic]-key` 
            and other as `[topic]-value`).

*Raises*:
- `ValueError`: An error ocurred when no connector identifier is
        provided
- `NoConnectServerAvailable`: An error ocurred if the Connect server is
        not available.
- `HTTPError`: An error ocurred if the request did not returned a 200
        status code (OK)

<h4 id="kafkaconnect_utils.connect_manager.ConnectManager.get_connector_status">get_connector_status</h4>

```python
ConnectManager.get_connector_status(self, id)
```

The method to get the status of a specific connector

*Parameters*:
- `id` (`str`): connector identifier.

*Returns*:
- `state` (`str`): one of the following values: `RUNNING`, `PAUSED`, `UNASSIGNED` or `FAILED`.

*Raises*:
- `ValueError`: An error ocurred when no connector identifier is
        provided
- `NoConnectServerAvailable`: An error ocurred if the Connect server is
        not available.
- `HTTPError`: An error ocurred if the request did not returned a 200
        status code (OK)

<h4 id="kafkaconnect_utils.connect_manager.ConnectManager.load_connector">load_connector</h4>

```python
ConnectManager.load_connector(self, id, config)
```

The method to create and load a connector in the Connect platform.

*Parameters*:
- `id` (`str`): connector name.
- `config` (`str`): connector configuration.

*Returns*:
- `result` (`bool`): creation process result

*Raises*:
- `ValueError`: An error ocurred when no connector identifier is
        provided
- `NoConnectServerAvailable`: An error ocurred if the Connect server is
        not available.
- `HTTPError`: An error ocurred if the request did not returned a 200
        status code (OK)

<h4 id="kafkaconnect_utils.connect_manager.ConnectManager.pause_connector">pause_connector</h4>

```python
ConnectManager.pause_connector(self, id)
```

The method to pause a specific connector

*Parameters*:
- `id` (`str`): connector identifier.

*Returns*:
- `result` (`bool`): pause process result

*Raises*:
- `ValueError`: An error ocurred when no connector identifier is
        provided
- `NoConnectServerAvailable`: An error ocurred if the Connect server is
        not available.
- `HTTPError`: An error ocurred if the request did not returned a 200
        status code (OK)

<h4 id="kafkaconnect_utils.connect_manager.ConnectManager.resume_connector">resume_connector</h4>

```python
ConnectManager.resume_connector(self, id)
```

The method to resume a paused connector

*Parameters*:
- `id` (`str`): connector identifier.

*Returns*:
- `result` (`bool`): resume process result

*Raises*:
- `ValueError`: An error ocurred when no connector identifier is
        provided
- `NoConnectServerAvailable`: An error ocurred if the Connect server is
        not available.
- `HTTPError`: An error ocurred if the request did not returned a 200
        status code (OK)

<h4 id="kafkaconnect_utils.connect_manager.ConnectManager.restart_connector">restart_connector</h4>

```python
ConnectManager.restart_connector(self, id)
```

The method to restart a specific connector

*Parameters*:
- `id` (`str`): connector identifier.

*Returns*:
- `result` (`bool`): restart process result

*Raises*:
- `ValueError`: An error ocurred when no connector identifier is
        provided
- `NoConnectServerAvailable`: An error ocurred if the Connect server is
        not available.
- `HTTPError`: An error ocurred if the request did not returned a 200
        status code (OK)

<h4 id="kafkaconnect_utils.connect_manager.ConnectManager.delete_connector">delete_connector</h4>

```python
ConnectManager.delete_connector(self, id)
```

The method to delete a specific connector

*Parameters*:
- `id` (`str`): connector identifier.

*Returns*:
- `result` (`bool`): deletion process result

*Raises*:
- `ValueError`: An error ocurred when no connector identifier is
        provided
- `NoConnectServerAvailable`: An error ocurred if the Connect server is
        not available.
- `HTTPError`: An error ocurred if the request did not returned a 200
        status code (OK)