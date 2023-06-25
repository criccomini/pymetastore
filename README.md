# pymetastore 🐝 🐍

`pymetastore` is a Python client for Hive Metastore.

## Key Features
* Python-friendly interface for Hive Metastore
* Comprehensive support for metadata operations
* Fully compatible with the Hive metastore service over Thrift protocol

## Installation

Install pymetastore with pip:

```bash
pip install pymetastore
```

## Quick Start

Here's a taste of using `pymetastore` to connect to Hive Metastore and interact with metadata:

```python
from pymetastore.metastore import HMS

# create a connection to Hive Metastore
with HMS.create(host="localhost", port=9083) as hms:
    # list all databases
    databases = hms.list_databases()

    # get a database
    database = hms.get_database(name="test_db")

    # list all tables in a database
    tables = hms.list_tables(database_name=database.name)

    # get a table
    table = hms.get_table(
        database_name=database.name,
        table_name=tables[0],
    )

    # list all partitions of a table
    partitions = hms.list_partitions(
        database_name=database.name,
        table_name=table.name,
    )

    # get a partition
    partition = hms.get_partition(
        database_name=database.name,
        table_name=table.name,
        partition_name="partition=1",
    )
```
