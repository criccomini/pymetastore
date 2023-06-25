# pymetastore 🐝 🐍

`pymetastore` is a Python client for Hive Metastore.

## Features
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

with HMS.create(host="localhost", port=9083) as hms:
    databases = hms.list_databases()
    database = hms.get_database(name="test_db")
    tables = hms.list_tables(database_name=database.name)
    table = hms.get_table(
        database_name=database.name,
        table_name=tables[0],
    )
    partitions = hms.list_partitions(
        database_name=database.name,
        table_name=table.name,
    )
    partition = hms.get_partition(
        database_name=database.name,
        table_name=table.name,
        partition_name="partition=1",
    )
```
