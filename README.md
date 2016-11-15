# Schematizer


What is it?
-----------
The Schematizer is a schema store service that tracks and manages all the schemas
used in the Data Pipeline and provides features like automatic documentation support.
We use Apache Avro to represent our schemas.

[Read More](https://engineeringblog.yelp.com/2016/08/more-than-just-a-schema-store.html)


How to download
---------------
```
git clone git@github.com:Yelp/schematizer.git
```


Tests
-----
Running unit tests
```
make -f Makefile-opensource test
```

Running unit integration tests
```
make -f Makefile-opensource itest
```


Setup and Configuration
-----------------------
1. Create a mysql database for Schematizer Service::
```
CREATE DATABASE <db_name> DEFAULT CHARACTER SET utf8;
```

2. Create MySQL tables in `<db_name>` database for Schematizer Service::
```
cat schema/tables/*.sql | mysql <db_name>
```

3. Create a `topology.yaml` file
```
topology:
-   cluster: <schematizer_cluster_name>
    replica: master
    entries:
        - charset: utf8
          use_unicode: true
          host: <db_ip>
          db: <db_name>
          user: <db_user>
          passwd: <db_password>
          port: <db_port>
```

4. In `config.yaml` assign values to the following configs::
```
schematizer_cluster: <schematizer_cluster_name>

topology_path: /path/to/topology.yaml
```


Usage
-----
Use `serviceinitd/schematizer.py` to start the Schematizer service.

### Interactive directly with Schematizer Service.

Registering a schema::
```
curl -X POST --header 'Content-Type: application/json' --header 'Accept: text/plain' -d '{
  "namespace": "test_namespace",
  "source_owner_email": "test@test.com",
  "source": "test_source",
  "contains_pii": false,
  "schema": "{\"type\":\"record\",\"namespace\":\"test_namespace\",\"source\":\"test_source\",\"name\":\"test_name\",\"doc\":\"test_doc\",\"fields\":[{\"type\":\"string\",\"doc\":\"test_doc1\",\"name\":\"key1\"},{\"type\":\"string\",\"doc\":\"test_doc2\",\"name\":\"key2\"}]}"
}' 'http://127.0.0.1:8888/v1/schemas/avro'
```

Getting Schema By ID::
```
curl -X GET --header 'Accept: text/plain' 'http://127.0.0.1:8888/v1/schemas/<schema_id>'
```

### Interactive with Schematizer Service using Schematizer Client Lib.

Registering a schema::
```
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer
test_avro_schema_json = {
    "type": "record",
    "namespace": "test_namespace",
    "source": "test_source",
    "name": "test_name",
    "doc": "test_doc",
    "fields": [
        {"type": "string", "doc": "test_doc1", "name": "key1"},
        {"type": "string", "doc": "test_doc2", "name": "key2"}
    ]
}
schema_info = get_schematizer().register_schema_from_schema_json(
    namespace="test_namespace",
    source="test_source",
    schema_json=test_avro_schema_json,
    source_owner_email="test@test.com",
    contains_pii=False
)
```

Getting Schema By ID::
```
from data_pipeline.schematizer_clientlib.schematizer import get_schematizer

schema_info = get_schematizer().get_schema_by_id(
    schema_id=schema_info.schema_id
)
```


Disclaimer
-------
We're still in the process of setting up this service as a stand-alone. There may be additional work required to run a Schematizer instance and integrate with other applications.


License
-------
Schematizer is licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0


Contributing
------------
Everyone is encouraged to contribute to Schematizer by forking the Github repository and making a pull request or opening an issue.
