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

Here's a taste of using `pymetastore` to connect to Hive Metastore and list all databases:

```python
from pymetastore.metastore import HMS

# create a connection to Hive Metastore
with HMS.create(host="localhost", port=9083) as hms:
    # list all databases
    databases = hms.client.get_all_databases()
    print(databases)
```
