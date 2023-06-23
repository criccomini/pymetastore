import os

import pytest
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport

from pymetastore.hive_metastore import ttypes
from pymetastore.hive_metastore.ThriftHiveMetastore import Client
from pymetastore.metastore import HMS, HColumn, HDatabase


@pytest.fixture(scope="module")
def hive_client():
    host = os.environ.get("HMS_HOST", "localhost")
    port = int(os.environ.get("HMS_PORT", 9083))
    transport = TSocket.TSocket(host, port)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = Client(protocol)
    transport.open()

    yield client

    transport.close()


@pytest.fixture(scope="module", autouse=True)
def setup_data(hive_client):
    db = ttypes.Database(
        name="test_db",
        description="This is a test database",
        locationUri="/tmp/test_db.db",
        parameters={},
    )

    if "test_db" in hive_client.get_all_databases():
        hive_client.drop_database("test_db", True, True)
    hive_client.create_database(db)

    cols = [
        ttypes.FieldSchema(name="col1", type="int", comment="c1"),
        ttypes.FieldSchema(name="col2", type="string", comment="c2"),
    ]

    partitionKeys = [ttypes.FieldSchema(name="partition", type="string", comment="")]

    serde_info = ttypes.SerDeInfo(
        name="test_serde",
        serializationLib="org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
        parameters={"field.delim": ","},
    )

    storageDesc = ttypes.StorageDescriptor(
        location="/tmp/test_db/test_table",
        cols=cols,
        inputFormat="org.apache.hadoop.mapred.TextInputFormat",
        outputFormat="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        compressed=False,
        numBuckets=-1,
        serdeInfo=serde_info,
        bucketCols=[],
    )

    table = ttypes.Table(
        tableName="test_table",
        dbName="test_db",
        owner="owner",
        createTime=0,
        lastAccessTime=0,
        retention=0,
        sd=storageDesc,
        partitionKeys=partitionKeys,
        parameters={},
        tableType="EXTERNAL_TABLE",
    )

    if "test_table" in hive_client.get_all_tables("test_db"):
        hive_client.drop_table("test_db", "test_table", True)
    hive_client.create_table(table)

    for i in range(1, 6):
        partition = ttypes.Partition(
            dbName="test_db",
            tableName="test_table",
            values=[str(i)],
            sd=storageDesc,
            parameters={},
        )
        hive_client.add_partition(partition)


def test_list_databases(hive_client):
    assert "test_db" in HMS(hive_client).list_databases()


@pytest.mark.xfail(reason="'owner_name' is None for some reason. Investigate.")
def test_get_database(hive_client):
    hms = HMS(hive_client)
    database = hms.get_database("test_db")
    assert isinstance(database, HDatabase)
    assert database.name == "test_db"
    assert database.location == "file:/tmp/test_db.db"
    # TODO Fix this bug. owner_name is None for some reason.
    assert database.owner_name == "owner"


def test_list_tables(hive_client):
    hms = HMS(hive_client)
    tables = hms.list_tables("test_db")
    assert isinstance(tables, list)
    assert "test_table" in tables
