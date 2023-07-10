"""
Integration tests for the metastore module.
"""
import os

import pytest
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport

from pymetastore.hive_metastore import ttypes
from pymetastore.hive_metastore.ThriftHiveMetastore import Client
from pymetastore.htypes import (
    HCharType,
    HDecimalType,
    HListType,
    HMapType,
    HPrimitiveType,
    HStructType,
    HType,
    HTypeCategory,
    HUnionType,
    HVarcharType,
    PrimitiveCategory,
)
from pymetastore.metastore import (
    HMS,
    BucketingVersion,
    HColumn,
    HDatabase,
    HPartition,
    HStorage,
    HTable,
    HiveBucketProperty,
    StorageFormat,
)
from pymetastore.stats import *


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

    partition_keys = [ttypes.FieldSchema(name="partition", type="string", comment="")]

    serde_info = ttypes.SerDeInfo(
        name="test_serde",
        serializationLib="org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
        parameters={"field.delim": ","},
    )

    storage_desc = ttypes.StorageDescriptor(
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
        sd=storage_desc,
        partitionKeys=partition_keys,
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
            sd=storage_desc,
            parameters={},
        )
        hive_client.add_partition(partition)

    # testing primitive Types
    cols2 = [
        ttypes.FieldSchema(name="col3", type="void", comment="c3"),
        ttypes.FieldSchema(name="col4", type="boolean", comment="c4"),
        ttypes.FieldSchema(name="col5", type="tinyint", comment="c5"),
        ttypes.FieldSchema(name="col6", type="smallint", comment="c6"),
        ttypes.FieldSchema(name="col7", type="bigint", comment="c7"),
        ttypes.FieldSchema(name="col8", type="float", comment="c8"),
        ttypes.FieldSchema(name="col9", type="double", comment="c9"),
        ttypes.FieldSchema(name="col10", type="date", comment="c10"),
        ttypes.FieldSchema(name="col11", type="timestamp", comment="c11"),
        ttypes.FieldSchema(
            name="col12", type="timestamp with local time zone", comment="c12"
        ),
        ttypes.FieldSchema(name="col13", type="interval_year_month", comment="c13"),
        ttypes.FieldSchema(name="col14", type="interval_day_time", comment="c14"),
        ttypes.FieldSchema(name="col15", type="binary", comment="c15"),
    ]

    storage_desc = ttypes.StorageDescriptor(
        location="/tmp/test_db/test_table2",
        cols=cols2,
        inputFormat="org.apache.hadoop.mapred.TextInputFormat",
        outputFormat="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        compressed=False,
        numBuckets=-1,
        serdeInfo=serde_info,
        bucketCols=[],
    )

    table = ttypes.Table(
        tableName="test_table2",
        dbName="test_db",
        owner="owner",
        createTime=0,
        lastAccessTime=0,
        retention=0,
        sd=storage_desc,
        partitionKeys=partition_keys,
        parameters={},
        tableType="EXTERNAL_TABLE",
    )

    if "test_table2" in hive_client.get_all_tables("test_db"):
        hive_client.drop_table("test_db", "test_table2", True)
    hive_client.create_table(table)

    # testing Parameterized Types
    cols3 = [
        ttypes.FieldSchema(name="col16", type="decimal(10,2)", comment="c16"),
        ttypes.FieldSchema(name="col17", type="varchar(10)", comment="c17"),
        ttypes.FieldSchema(name="col18", type="char(10)", comment="c18"),
        ttypes.FieldSchema(name="col19", type="array<int>", comment="c19"),
        ttypes.FieldSchema(name="col20", type="map<int,string>", comment="c20"),
        ttypes.FieldSchema(name="col21", type="struct<a:int,b:string>", comment="c21"),
        ttypes.FieldSchema(name="col22", type="uniontype<int,string>", comment="c22"),
    ]

    storage_desc = ttypes.StorageDescriptor(
        location="/tmp/test_db/test_table3",
        cols=cols3,
        inputFormat="org.apache.hadoop.mapred.TextInputFormat",
        outputFormat="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        compressed=False,
        numBuckets=-1,
        serdeInfo=serde_info,
        bucketCols=[],
    )

    table = ttypes.Table(
        tableName="test_table3",
        dbName="test_db",
        owner="owner",
        createTime=0,
        lastAccessTime=0,
        retention=0,
        sd=storage_desc,
        partitionKeys=partition_keys,
        parameters={},
        tableType="EXTERNAL_TABLE",
    )

    if "test_table3" in hive_client.get_all_tables("test_db"):
        hive_client.drop_table("test_db", "test_table3", True)
    hive_client.create_table(table)

    cols4 = [
        ttypes.FieldSchema(name="col4", type="boolean", comment="c4"),
        ttypes.FieldSchema(name="col5", type="double", comment="c5"),
        ttypes.FieldSchema(name="col6", type="bigint", comment="c6"),
        ttypes.FieldSchema(name="col7", type="string", comment="c7"),
        ttypes.FieldSchema(name="col8", type="binary", comment="c8"),
        ttypes.FieldSchema(name="col9", type="decimal(10,2)", comment="c9"),
        ttypes.FieldSchema(name="col10", type="date", comment="c10"),
    ]

    storage_desc = ttypes.StorageDescriptor(
        location="/tmp/test_db/test_table4",
        cols=cols4,
        inputFormat="org.apache.hadoop.mapred.TextInputFormat",
        outputFormat="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        compressed=False,
        numBuckets=-1,
        serdeInfo=serde_info,
        bucketCols=[],
    )

    table = ttypes.Table(
        tableName="test_table4",
        dbName="test_db",
        owner="owner",
        createTime=0,
        lastAccessTime=0,
        retention=0,
        sd=storage_desc,
        partitionKeys=partition_keys,
        parameters={},
        tableType="EXTERNAL_TABLE",
    )

    if "test_table4" in hive_client.get_all_tables("test_db"):
        hive_client.drop_table("test_db", "test_table4", True)
    hive_client.create_table(table)

    stats_desc = ttypes.ColumnStatisticsDesc(True, "test_db", "test_table4")

    stats_bool = ttypes.BooleanColumnStatsData(numTrues=10, numFalses=10, numNulls=0)
    stats_obj_bool = ttypes.ColumnStatisticsObj(
        "col4", "boolean", ttypes.ColumnStatisticsData(booleanStats=stats_bool)
    )

    stats_double = ttypes.DoubleColumnStatsData(
        lowValue=0, highValue=1, numNulls=0, numDVs=100
    )
    stats_obj_double = ttypes.ColumnStatisticsObj(
        "col5", "double", ttypes.ColumnStatisticsData(doubleStats=stats_double)
    )
    stats_long = ttypes.LongColumnStatsData(
        lowValue=0, highValue=100, numNulls=0, numDVs=100
    )
    stats_obj_long = ttypes.ColumnStatisticsObj(
        "col6", "bigint", ttypes.ColumnStatisticsData(longStats=stats_long)
    )
    stats_string = ttypes.StringColumnStatsData(
        avgColLen=10, maxColLen=10, numNulls=5, numDVs=10
    )
    stats_obj_string = ttypes.ColumnStatisticsObj(
        "col7", "string", ttypes.ColumnStatisticsData(stringStats=stats_string)
    )
    stats_binary = ttypes.BinaryColumnStatsData(avgColLen=10, maxColLen=10, numNulls=5)
    stats_obj_binary = ttypes.ColumnStatisticsObj(
        "col8", "binary", ttypes.ColumnStatisticsData(binaryStats=stats_binary)
    )
    stats_decimal = ttypes.DecimalColumnStatsData(
        lowValue=ttypes.Decimal(1, b"123445.3"),
        highValue=ttypes.Decimal(1, b"1232324124"),
        numNulls=0,
        numDVs=100,
    )
    stats_obj_decimal = ttypes.ColumnStatisticsObj(
        "col9", "decimal(10,2)", ttypes.ColumnStatisticsData(decimalStats=stats_decimal)
    )
    stats_date = ttypes.DateColumnStatsData(
        lowValue=ttypes.Date(0), highValue=ttypes.Date(1), numNulls=0, numDVs=100
    )
    stats_obj_date = ttypes.ColumnStatisticsObj(
        "col10", "date", ttypes.ColumnStatisticsData(dateStats=stats_date)
    )
    col_stats = ttypes.ColumnStatistics(
        stats_desc,
        [
            stats_obj_bool,
            stats_obj_double,
            stats_obj_long,
            stats_obj_string,
            stats_obj_binary,
            stats_obj_decimal,
            stats_obj_date,
        ],
    )
    hive_client.update_table_column_statistics(col_stats)


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
    # this is not a parameter of the table we created, but the metastore adds it
    assert table.parameters.get("transient_lastDdlTime") is not None
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


def test_list_columns(hive_client):
    hms = HMS(hive_client)
    columns = hms.list_columns("test_db", "test_table")

    assert isinstance(columns, list)
    assert len(columns) == 2
    assert columns[0] == "col1"
    assert columns[1] == "col2"


def test_list_partitions(hive_client):
    hms = HMS(hive_client)
    partitions = hms.list_partitions("test_db", "test_table")

    assert isinstance(partitions, list)
    assert len(partitions) == 5
    for i in range(1, 6):
        assert f"partition={i}" in partitions


def test_get_partitions(hive_client):
    hms = HMS(hive_client)
    partitions = hms.get_partitions("test_db", "test_table")
    expected_storage = HStorage(
        storage_format=StorageFormat(
            serde="org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
            input_format="org.apache.hadoop.mapred.TextInputFormat",
            output_format="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        ),
        skewed=True,
        location="file:/tmp/test_db/test_table",
        bucket_property=HiveBucketProperty(
            bucketed_by=[],
            bucket_count=-1,
            version=BucketingVersion.V1,
            sorting_columns=[],
        ),
        serde_parameters={"field.delim": ","},
    )

    assert isinstance(partitions, list)
    assert len(partitions) == 5  # we created 5 partitions in the setup_data fixture

    missing_partitions = {1, 2, 3, 4, 5}

    for partition in partitions:
        assert isinstance(partition, HPartition)
        missing_partitions.discard(int(partition.values[0]))
        assert partition.database_name == "test_db"
        assert partition.table_name == "test_table"
        assert partition.sd == expected_storage
        assert partition.create_time != 0
        assert partition.last_access_time == 0
        assert partition.cat_name == "hive"
        assert isinstance(partition.parameters, dict)
        assert partition.write_id == -1

    assert missing_partitions == set()


def test_get_partition(hive_client):
    hms = HMS(hive_client)
    partition = hms.get_partition("test_db", "test_table", "partition=1")
    expected_storage = HStorage(
        storage_format=StorageFormat(
            serde="org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
            input_format="org.apache.hadoop.mapred.TextInputFormat",
            output_format="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        ),
        skewed=True,
        location="file:/tmp/test_db/test_table",
        bucket_property=HiveBucketProperty(
            bucketed_by=[],
            bucket_count=-1,
            version=BucketingVersion.V1,
            sorting_columns=[],
        ),
        serde_parameters={"field.delim": ","},
    )

    assert isinstance(partition, HPartition)
    assert partition.database_name == "test_db"
    assert partition.table_name == "test_table"
    assert partition.values == ["1"]
    assert partition.sd == expected_storage
    assert partition.create_time != 0
    assert partition.last_access_time == 0
    assert partition.cat_name == "hive"
    assert isinstance(partition.parameters, dict)
    assert partition.write_id == -1


def test_primitive_types(hive_client):
    hms = HMS(hive_client)

    table = hms.get_table("test_db", "test_table2")
    columns = table.columns

    assert len(columns) == 13

    assert columns[0].name == "col3"
    assert isinstance(columns[0].type, HType)
    assert isinstance(columns[0].type, HPrimitiveType)
    assert columns[0].type.name == "VOID"
    assert columns[0].type.category == HTypeCategory.PRIMITIVE
    assert columns[0].comment == "c3"

    assert columns[1].name == "col4"
    assert isinstance(columns[1].type, HType)
    assert isinstance(columns[1].type, HPrimitiveType)
    assert columns[1].type.name == "BOOLEAN"
    assert columns[1].type.category == HTypeCategory.PRIMITIVE
    assert columns[1].comment == "c4"

    assert columns[2].name == "col5"
    assert isinstance(columns[2].type, HType)
    assert isinstance(columns[2].type, HPrimitiveType)
    assert columns[2].type.name == "BYTE"
    assert columns[2].type.category == HTypeCategory.PRIMITIVE
    assert columns[2].comment == "c5"

    assert columns[3].name == "col6"
    assert isinstance(columns[3].type, HType)
    assert isinstance(columns[3].type, HPrimitiveType)
    assert columns[3].type.name == "SHORT"
    assert columns[3].type.category == HTypeCategory.PRIMITIVE
    assert columns[3].comment == "c6"

    assert columns[4].name == "col7"
    assert isinstance(columns[4].type, HType)
    assert isinstance(columns[4].type, HPrimitiveType)
    assert columns[4].type.name == "LONG"
    assert columns[4].type.category == HTypeCategory.PRIMITIVE
    assert columns[4].comment == "c7"

    assert columns[5].name == "col8"
    assert isinstance(columns[5].type, HType)
    assert isinstance(columns[5].type, HPrimitiveType)
    assert columns[5].type.name == "FLOAT"
    assert columns[5].type.category == HTypeCategory.PRIMITIVE
    assert columns[5].comment == "c8"

    assert columns[6].name == "col9"
    assert isinstance(columns[6].type, HType)
    assert isinstance(columns[6].type, HPrimitiveType)
    assert columns[6].type.name == "DOUBLE"
    assert columns[6].type.category == HTypeCategory.PRIMITIVE
    assert columns[6].comment == "c9"

    assert columns[7].name == "col10"
    assert isinstance(columns[7].type, HType)
    assert isinstance(columns[7].type, HPrimitiveType)
    assert columns[7].type.name == "DATE"
    assert columns[7].type.category == HTypeCategory.PRIMITIVE
    assert columns[7].comment == "c10"

    assert columns[8].name == "col11"
    assert isinstance(columns[8].type, HType)
    assert isinstance(columns[8].type, HPrimitiveType)
    assert columns[8].type.name == "TIMESTAMP"
    assert columns[8].type.category == HTypeCategory.PRIMITIVE
    assert columns[8].comment == "c11"

    assert columns[9].name == "col12"
    assert isinstance(columns[9].type, HType)
    assert isinstance(columns[9].type, HPrimitiveType)
    assert columns[9].type.name == "TIMESTAMPLOCALTZ"
    assert columns[9].type.category == HTypeCategory.PRIMITIVE
    assert columns[9].comment == "c12"

    assert columns[10].name == "col13"
    assert isinstance(columns[10].type, HType)
    assert isinstance(columns[10].type, HPrimitiveType)
    assert columns[10].type.name == "INTERVAL_YEAR_MONTH"
    assert columns[10].type.category == HTypeCategory.PRIMITIVE
    assert columns[10].comment == "c13"

    assert columns[11].name == "col14"
    assert isinstance(columns[11].type, HType)
    assert isinstance(columns[11].type, HPrimitiveType)
    assert columns[11].type.name == "INTERVAL_DAY_TIME"
    assert columns[11].type.category == HTypeCategory.PRIMITIVE
    assert columns[11].comment == "c14"

    assert columns[12].name == "col15"
    assert isinstance(columns[12].type, HType)
    assert isinstance(columns[12].type, HPrimitiveType)
    assert columns[12].type.name == "BINARY"
    assert columns[12].type.category == HTypeCategory.PRIMITIVE
    assert columns[12].comment == "c15"


def test_parameterized_types(hive_client):
    hms = HMS(hive_client)
    table = hms.get_table("test_db", "test_table3")
    columns = table.columns

    assert len(columns) == 7

    assert columns[0].name == "col16"
    assert isinstance(columns[0].type, HType)
    assert isinstance(columns[0].type, HDecimalType)
    assert columns[0].type.name == "DECIMAL"
    assert columns[0].type.category == HTypeCategory.PRIMITIVE
    assert columns[0].comment == "c16"
    assert columns[0].type.precision == 10
    assert columns[0].type.scale == 2

    assert columns[1].name == "col17"
    assert isinstance(columns[1].type, HType)
    assert isinstance(columns[1].type, HVarcharType)
    assert columns[1].type.name == "VARCHAR"
    assert columns[1].type.category == HTypeCategory.PRIMITIVE
    assert columns[1].comment == "c17"
    assert columns[1].type.length == 10

    assert columns[2].name == "col18"
    assert isinstance(columns[2].type, HType)
    assert isinstance(columns[2].type, HCharType)
    assert columns[2].type.name == "CHAR"
    assert columns[2].type.category == HTypeCategory.PRIMITIVE
    assert columns[2].comment == "c18"
    assert columns[2].type.length == 10

    assert columns[3].name == "col19"
    assert isinstance(columns[3].type, HType)
    assert isinstance(columns[3].type, HListType)
    assert columns[3].type.name == "LIST"
    assert columns[3].type.category == HTypeCategory.LIST
    assert columns[3].comment == "c19"
    assert isinstance(columns[3].type.element_type, HType)
    assert isinstance(columns[3].type.element_type, HPrimitiveType)
    assert columns[3].type.element_type.name == "INT"

    assert columns[4].name == "col20"
    assert isinstance(columns[4].type, HType)
    assert isinstance(columns[4].type, HMapType)
    assert columns[4].type.name == "MAP"
    assert columns[4].type.category == HTypeCategory.MAP
    assert columns[4].comment == "c20"
    assert isinstance(columns[4].type.key_type, HType)
    assert isinstance(columns[4].type.key_type, HPrimitiveType)
    assert columns[4].type.key_type.name == "INT"
    assert isinstance(columns[4].type.value_type, HType)
    assert isinstance(columns[4].type.value_type, HPrimitiveType)
    assert columns[4].type.value_type.name == "STRING"

    assert columns[5].name == "col21"
    assert isinstance(columns[5].type, HType)
    assert isinstance(columns[5].type, HStructType)
    assert columns[5].type.name == "STRUCT"
    assert columns[5].type.category == HTypeCategory.STRUCT
    assert columns[5].comment == "c21"
    assert len(columns[5].type.names) == 2
    assert len(columns[5].type.types) == 2
    assert columns[5].type.names[0] == "a"
    assert columns[5].type.names[1] == "b"
    assert isinstance(columns[5].type.types[0], HType)
    assert isinstance(columns[5].type.types[0], HPrimitiveType)
    assert columns[5].type.types[0].name == "INT"
    assert isinstance(columns[5].type.types[1], HType)
    assert isinstance(columns[5].type.types[1], HPrimitiveType)
    assert columns[5].type.types[1].name == "STRING"

    assert columns[6].name == "col22"
    assert isinstance(columns[6].type, HType)
    assert isinstance(columns[6].type, HUnionType)
    assert columns[6].type.name == "UNION"
    assert columns[6].type.category == HTypeCategory.UNION
    assert columns[6].comment == "c22"
    assert len(columns[6].type.types) == 2
    assert isinstance(columns[6].type.types[0], HType)
    assert isinstance(columns[6].type.types[0], HPrimitiveType)
    assert columns[6].type.types[0].name == "INT"
    assert isinstance(columns[6].type.types[1], HType)
    assert isinstance(columns[6].type.types[1], HPrimitiveType)
    assert columns[6].type.types[1].name == "STRING"


def test_table_stats(hive_client):
    hms = HMS(hive_client)
    table = hms.get_table("test_db", "test_table4")

    statistics = hms.get_table_stats(
        table,
        [
            HColumn("col4", HPrimitiveType(PrimitiveCategory.BOOLEAN)),
            HColumn("col5", HPrimitiveType(PrimitiveCategory.DOUBLE)),
            HColumn("col7", HPrimitiveType(PrimitiveCategory.STRING)),
            HColumn("col8", HPrimitiveType(PrimitiveCategory.BINARY)),
            HColumn("col9", HDecimalType(1, 1)),
            HColumn("col10", HPrimitiveType(PrimitiveCategory.DATE)),
            HColumn("col6", HPrimitiveType(PrimitiveCategory.LONG)),
        ],
    )

    assert len(statistics) == 7
    assert statistics[0].tableName == "test_table4"
    assert statistics[0].dbName == "test_db"
    assert statistics[0].stats is not None

    assert isinstance(statistics[0].stats, BooleanTypeStats)
    assert statistics[0].stats.numTrues == 10
    assert statistics[0].stats.numFalses == 10
    assert statistics[0].stats.numNulls == 0

    assert isinstance(statistics[1].stats, DoubleTypeStats)
    assert statistics[1].stats.lowValue == 0
    assert statistics[1].stats.highValue == 1
    assert statistics[1].stats.numNulls == 0
    assert statistics[1].stats.cardinality == 100

    assert isinstance(statistics[2].stats, StringTypeStats)
    assert statistics[2].stats.avgColLen == 10
    assert statistics[2].stats.maxColLen == 10
    assert statistics[2].stats.numNulls == 5
    assert statistics[2].stats.cardinality == 10

    assert isinstance(statistics[3].stats, BinaryTypeStats)
    assert statistics[3].stats.avgColLen == 10
    assert statistics[3].stats.maxColLen == 10
    assert statistics[3].stats.numNulls == 5

    assert isinstance(statistics[4].stats, DecimalTypeStats)
    assert statistics[4].stats.lowValue == ttypes.Decimal(scale=1, unscaled=b"123445.3")
    assert statistics[4].stats.highValue == ttypes.Decimal(1, b"1232324124")
    assert statistics[4].stats.numNulls == 0
    assert statistics[4].stats.cardinality == 100

    assert isinstance(statistics[5].stats, DateTypeStats)
    assert statistics[5].stats.lowValue == ttypes.Date(0)
    assert statistics[5].stats.highValue == ttypes.Date(1)
    assert statistics[5].stats.numNulls == 0
    assert statistics[5].stats.cardinality == 100

    assert isinstance(statistics[6].stats, LongTypeStats)
    assert statistics[6].stats.lowValue == 0
    assert statistics[6].stats.highValue == 100
    assert statistics[6].stats.numNulls == 0
    assert statistics[6].stats.cardinality == 100
