import os

import pytest
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport

from pymetastore.hive_metastore import ttypes
from pymetastore.hive_metastore.ThriftHiveMetastore import Client
from pymetastore.htypes import HPrimitiveType, HType, HTypeCategory
from pymetastore.metastore import HMS, HColumn, HDatabase, HTable


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
        ownerName="owner",
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


def test_get_database(hive_client):
    hms = HMS(hive_client)
    database = hms.get_database("test_db")
    assert isinstance(database, HDatabase)
    assert database.name == "test_db"
    assert database.location == "file:/tmp/test_db.db"
    assert database.owner_name == "owner"


def test_list_tables(hive_client):
    hms = HMS(hive_client)
    tables = hms.list_tables("test_db")
    assert isinstance(tables, list)
    assert "test_table" in tables


@pytest.mark.xfail(reason="'table_type' is coming back MANAGED_TABLE.")
def test_get_table(hive_client):
    hms = HMS(hive_client)
    table = hms.get_table("test_db", "test_table")
    assert isinstance(table, HTable)
    assert table.database_name == "test_db"
    assert table.name == "test_table"
    assert table.owner == "owner"
    assert table.storage.location == "file:/tmp/test_db/test_table"
    assert len(table.columns) == 2
    assert isinstance(table.columns[0], HColumn)
    assert isinstance(table.columns[1], HColumn)
    assert len(table.partition_columns) == 1
    assert isinstance(table.partition_columns[0], HColumn)
    assert isinstance(table.parameters, dict)
    assert len(table.parameters) == 1
    assert (
        table.parameters.get("transient_lastDdlTime") is not None
    )  # this is not a parameter of the table we created, but the metastore adds it
    # This assertion fails, I leave it here on purpose. My current assumption is that
    # the metastore overrides some of the passed options with defaults. "MANAGEd_TABLE"
    # is the default value for tableType. See here for the defaults:
    # https://github.com/apache/hive/blob/14a1f70607db5ae6cf71b6d4343f308a5167581c/standalone-metastore/metastore-server/src/main/java/org/apache/hadoop/hive/metastore/client/builder/TableBuilder.java#L69C43-L69C43
    # Something similar happens also when you choose a "VIRTUAL_VIEW" table type, where
    # the metastore overrides the storage descriptor removing in the location the file::
    # prefix.
    assert table.table_type == "EXTERNAL_TABLE"


def test_get_table_columns(hive_client):
    hms = HMS(hive_client)
    table = hms.get_table("test_db", "test_table")
    columns = table.columns
    partition_columns = table.partition_columns

    assert columns[0].name == "col1"
    assert isinstance(columns[0].type, HType)
    assert isinstance(columns[0].type, HPrimitiveType)
    assert columns[0].type.name == "INT"
    assert columns[0].type.category == HTypeCategory.PRIMITIVE
    assert columns[0].comment == "c1"

    assert columns[1].name == "col2"
    assert isinstance(columns[1].type, HType)
    assert isinstance(columns[1].type, HPrimitiveType)
    assert columns[1].type.name == "STRING"
    assert columns[1].type.category == HTypeCategory.PRIMITIVE
    assert columns[1].comment == "c2"

    assert partition_columns[0].name == "partition"
    assert isinstance(partition_columns[0].type, HType)
    assert isinstance(partition_columns[0].type, HPrimitiveType)
    assert partition_columns[0].type.name == "STRING"
    assert partition_columns[0].type.category == HTypeCategory.PRIMITIVE
    assert partition_columns[0].comment == ""
