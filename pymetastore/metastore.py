from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional

from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport

from .hive_metastore import ThriftHiveMetastore as hms
from .hive_metastore.ttypes import (
    Database,
    FieldSchema,
    Partition,
    PrincipalType,
    SerDeInfo,
    StorageDescriptor,
    Table,
)
from .htypes import HType, TypeParser
from .stats import (
    BinaryTypeStats,
    BooleanTypeStats,
    ColumnStats,
    DateTypeStats,
    DecimalTypeStats,
    DoubleTypeStats,
    LongTypeStats,
    StringTypeStats,
)


class HPrincipalType(Enum):
    ROLE = "ROLE"
    USER = "USER"
    GROUP = "GROUP"


@dataclass
class HPrivilegeGrantInfo:
    privilege: str
    grantor: str
    grantor_type: HPrincipalType
    create_time: int
    grant_option: bool


@dataclass
class HPrincipalPrivilegeSet:
    user_privileges: List[HPrivilegeGrantInfo]
    group_privileges: List[HPrivilegeGrantInfo]
    role_privileges: List[HPrivilegeGrantInfo]


@dataclass
class HDatabase:
    name: str
    location: Optional[str] = None
    owner_name: Optional[str] = None
    owner_type: Optional[HPrincipalType] = None
    comment: Optional[str] = None
    parameters: Optional[Dict[str, str]] = None


@dataclass
class HColumn:
    name: str
    type: HType
    comment: Optional[str] = None


class HSortingOrder(Enum):
    ASC = 1
    DESC = 0


@dataclass
class HSortingColumn:
    column: str
    order: HSortingOrder


class BucketingVersion(Enum):
    V1 = 1
    V2 = 2


@dataclass
class HiveBucketProperty:
    bucketed_by: List[str]
    bucket_count: int
    version: BucketingVersion = BucketingVersion.V1
    sorting_columns: List[HSortingColumn] = field(default_factory=list)


@dataclass
class StorageFormat:
    serde: str
    input_format: str
    output_format: str


@dataclass
class HStorage:
    storage_format: StorageFormat
    skewed: bool = False
    location: Optional[str] = None
    bucket_property: Optional[HiveBucketProperty] = None
    serde_parameters: Optional[Dict[str, str]] = None


@dataclass
class HTable:
    database_name: str
    name: str
    table_type: str
    columns: List[HColumn]
    partition_columns: List[HColumn]
    storage: HStorage
    parameters: Dict[str, str]
    view_original_text: Optional[str] = None
    view_expanded_text: Optional[str] = None
    write_id: Optional[int] = None
    owner: Optional[str] = None


@dataclass
class HSkewedInfo:
    skewed_col_names: List[str]
    skewed_col_values: List[List[str]]
    skewed_col_value_location_maps: Dict[List[str], str]


@dataclass
class HPartition:
    database_name: str
    table_name: str
    values: List[str]
    parameters: Dict[str, str]
    create_time: int
    last_access_time: int
    sd: HStorage
    cat_name: str
    write_id: int


class HMS:
    def __init__(self, client: hms.Client):
        self.client = client

    @staticmethod
    def create(host: str = "localhost", port: int = 9083) -> "_HMSConnection":
        return _HMSConnection(host, port)

    def list_databases(self) -> List[str]:
        databases = self.client.get_all_databases()
        db_names = []
        for database in databases:
            db_names.append(database)
        return db_names

    def get_database(self, name: str) -> HDatabase:
        db: Database = self.client.get_database(name)

        if db.ownerType is PrincipalType.USER:
            owner_type = HPrincipalType.USER
        elif db.ownerType is PrincipalType.ROLE:
            owner_type = HPrincipalType.ROLE
        else:
            owner_type = None

        return HDatabase(
            db.name,  # pyright: ignore[reportGeneralTypeIssues]
            db.locationUri,
            db.ownerName,
            owner_type,
            db.description,
            db.parameters,
        )

    def list_tables(self, database_name: str) -> List[str]:
        return self.client.get_all_tables(database_name)

    def list_columns(self, database_name: str, table_name: str) -> List[str]:
        # TODO: Rather than ignore these pyright errors, do appropriate None handling
        columns = self.client.get_table(
            database_name,
            table_name,
        ).sd.cols  # pyright: ignore[reportOptionalMemberAccess]
        self.client.get_schema(database_name, table_name)
        column_names = []
        for column in columns:  # pyright: ignore[reportOptionalIterable]
            column_names.append(column.name)
        return column_names

    def list_partitions(
        self,
        database_name: str,
        table_name: str,
        max_parts: int = -1,
    ) -> List[str]:
        partitions = self.client.get_partition_names(
            database_name,
            table_name,
            max_parts,
        )
        return partitions

    def get_partitions(
        self,
        database_name: str,
        table_name: str,
        max_parts: int = -1,
    ) -> List[HPartition]:
        result_partitions = []
        partitions: List[Partition] = self.client.get_partitions(
            database_name,
            table_name,
            max_parts,
        )

        assert isinstance(partitions, list)

        for partition in partitions:
            assert isinstance(partition, Partition)
            assert isinstance(partition.sd, StorageDescriptor)
            assert isinstance(partition.sd.serdeInfo, SerDeInfo)
            assert isinstance(partition.dbName, str)
            assert isinstance(partition.tableName, str)
            assert isinstance(partition.values, list)
            assert isinstance(partition.parameters, dict)
            assert isinstance(partition.createTime, int)
            assert isinstance(partition.lastAccessTime, int)
            assert isinstance(partition.catName, str)

            serialization_lib = (
                ""
                if partition.sd.serdeInfo.serializationLib is None
                else partition.sd.serdeInfo.serializationLib
            )
            input_format = (
                "" if partition.sd.inputFormat is None else partition.sd.inputFormat
            )
            output_format = (
                "" if partition.sd.outputFormat is None else partition.sd.outputFormat
            )
            bucket_cols = (
                [] if partition.sd.bucketCols is None else partition.sd.bucketCols
            )
            sort_cols = [] if partition.sd.sortCols is None else partition.sd.sortCols
            num_buckets = (
                0 if partition.sd.numBuckets is None else partition.sd.numBuckets
            )
            is_skewed = partition.sd.skewedInfo is not None

            assert isinstance(serialization_lib, str)
            assert isinstance(input_format, str)
            assert isinstance(output_format, str)
            assert isinstance(bucket_cols, list)
            assert isinstance(sort_cols, list)
            assert isinstance(num_buckets, int)

            storage_format = StorageFormat(
                serialization_lib,
                input_format,
                output_format,
            )

            bucket_property = HiveBucketProperty(
                bucket_cols,
                num_buckets,
                BucketingVersion.V1,
                sort_cols,
            )

            sd = HStorage(
                storage_format,
                is_skewed,
                partition.sd.location,
                bucket_property,
                partition.sd.serdeInfo.parameters,
            )

            result_partition = HPartition(
                partition.dbName,
                partition.tableName,
                partition.values,
                partition.parameters,
                partition.createTime,
                partition.lastAccessTime,
                sd,
                partition.catName,
                partition.writeId,
            )

            result_partitions.append(result_partition)

        return result_partitions

    def get_partition(
        self,
        database_name: str,
        table_name: str,
        partition_name: str,
    ) -> HPartition:
        partition: Partition = self.client.get_partition_by_name(
            database_name,
            table_name,
            partition_name,
        )
        assert partition is not None
        assert isinstance(partition, Partition)
        assert isinstance(partition.sd, StorageDescriptor)
        assert isinstance(partition.sd.serdeInfo, SerDeInfo)
        assert isinstance(partition.sd.serdeInfo.serializationLib, str)
        assert partition.sd.inputFormat is not None
        assert partition.sd.outputFormat is not None

        serialization_lib = partition.sd.serdeInfo.serializationLib
        input_format = partition.sd.inputFormat
        output_format = partition.sd.outputFormat
        storage_format = StorageFormat(
            serialization_lib,
            input_format,
            output_format,
        )
        sort_cols = [] if partition.sd.sortCols is None else partition.sd.sortCols
        bucket_cols = [] if partition.sd.bucketCols is None else partition.sd.bucketCols
        num_buckets = partition.sd.numBuckets or 0
        is_skewed = partition.sd.skewedInfo is not None
        location = "" if partition.sd.location is None else partition.sd.location
        serde_parameters = (
            {}
            if partition.sd.serdeInfo.parameters is None
            else partition.sd.serdeInfo.parameters
        )
        catName = "" if partition.catName is None else partition.catName
        writeId = -1 if partition.writeId is None else partition.writeId
        last_access_time = (
            -1 if partition.lastAccessTime is None else partition.lastAccessTime
        )
        partition_parameters = (
            {} if partition.parameters is None else partition.parameters
        )
        create_time = -1 if partition.createTime is None else partition.createTime
        values = [] if partition.values is None else partition.values
        db_name = partition.dbName
        partition_table_name = partition.tableName

        assert isinstance(sort_cols, list)
        assert isinstance(bucket_cols, list)
        assert isinstance(num_buckets, int)
        assert isinstance(location, str)
        assert isinstance(serde_parameters, dict)
        assert isinstance(catName, str)
        assert isinstance(writeId, int)
        assert isinstance(last_access_time, int)
        assert isinstance(partition_parameters, dict)
        assert isinstance(create_time, int)
        assert isinstance(values, list)
        assert isinstance(db_name, str)
        assert isinstance(partition_table_name, str)

        bucket_property = HiveBucketProperty(
            bucket_cols,
            num_buckets,
            BucketingVersion.V1,
            sort_cols,
        )
        sd = HStorage(
            storage_format,
            is_skewed,
            location,
            bucket_property,
            serde_parameters,
        )
        result_partition = HPartition(
            db_name,
            partition_table_name,
            values,
            partition_parameters,
            create_time,
            last_access_time,
            sd,
            catName,
            writeId,
        )

        return result_partition

    def get_table(self, database_name: str, table_name: str) -> HTable:
        table: Table = self.client.get_table(database_name, table_name)

        columns = []

        partition_columns = []
        if table.partitionKeys is not None:
            if isinstance(table.partitionKeys, list):
                t_part_columns: List[FieldSchema] = table.partitionKeys
                for column in t_part_columns:
                    if column is not None:
                        if isinstance(column, FieldSchema):
                            if column.type is not None:
                                type_parser = TypeParser(column.type)
                            else:
                                raise TypeError(f"Expected type to be str, got None")
                            if column.comment is not None:
                                comment = column.comment
                            else:
                                comment = ""
                            if column.name is not None:
                                name = column.name
                            else:
                                raise TypeError(f"Expected name to be str, got None")
                            partition_columns.append(
                                HColumn(name, type_parser.parse_type(), comment)
                            )

        if table.sd is not None:
            if table.sd.cols is not None:
                if isinstance(table.sd.cols, list):
                    t_columns: List[FieldSchema] = table.sd.cols
                    for column in t_columns:
                        if column is not None:
                            if isinstance(column, FieldSchema):
                                if column.type is not None:
                                    type_parser = TypeParser(column.type)
                                else:
                                    raise TypeError(
                                        f"Expected type to be str, got None"
                                    )
                                if column.comment is not None:
                                    comment = column.comment
                                else:
                                    comment = ""
                                if column.name is not None:
                                    name = column.name
                                else:
                                    raise TypeError(
                                        f"Expected name to be str, got None"
                                    )
                                columns.append(
                                    HColumn(name, type_parser.parse_type(), comment)
                                )

            if table.sd.serdeInfo is not None:
                if isinstance(table.sd.serdeInfo, SerDeInfo):
                    if table.sd.serdeInfo.serializationLib is not None:
                        if isinstance(table.sd.serdeInfo.serializationLib, str):
                            serde = table.sd.serdeInfo.serializationLib
                        else:
                            raise TypeError(
                                f"Expected serializationLib to be str, got {type(table.sd.serdeInfo.serializationLib)}"
                            )
                    else:
                        raise TypeError(
                            f"Expected serdeInfo to be str, got {type(table.sd.serdeInfo)}"
                        )
                else:
                    raise TypeError(
                        f"Expected serdeInfo to be SerDeInfo, got {type(table.sd.serdeInfo)}"
                    )
            else:
                raise TypeError(f"Expected serdeInfo to be SerDeInfo, got None")

            if table.sd.inputFormat is not None:
                if isinstance(table.sd.inputFormat, str):
                    input_format = table.sd.inputFormat
                else:
                    raise TypeError(
                        f"Expected inputFormat to be str, got {type(table.sd.inputFormat)}"
                    )
            else:
                raise TypeError(f"Expected inputFormat to be str, got None")

            if table.sd.outputFormat is not None:
                if isinstance(table.sd.outputFormat, str):
                    output_format = table.sd.outputFormat
                else:
                    raise TypeError(
                        f"Expected outputFormat to be str, got {type(table.sd.outputFormat)}"
                    )
            else:
                raise TypeError(f"Expected outputFormat to be str, got None")

            storage_format = StorageFormat(serde, input_format, output_format)

            bucket_property = None
            if table.sd.bucketCols is not None:
                sort_cols = []
                if table.sd.sortCols is not None:
                    if isinstance(table.sd.sortCols, list):
                        for order in table.sd.sortCols:
                            sort_cols.append(HSortingColumn(order.col, order.order))
                    else:
                        raise TypeError(
                            f"Expected bucketCols to be list, got {type(table.sd.sortCols)}"
                        )

                version = BucketingVersion.V1
                if table.parameters is not None:
                    if isinstance(table.parameters, dict):
                        if (
                            table.parameters.get(
                                "TABLE_BUCKETING_VERSION", BucketingVersion.V1
                            )
                            == BucketingVersion.V2
                        ):
                            version = BucketingVersion.V2
                    else:
                        raise TypeError(
                            f"Expected parameters to be dict, got {type(table.parameters)}"
                        )
                else:
                    raise TypeError(
                        f"Expected parameters to be dict, got {type(table.parameters)}"
                    )

                if table.sd.numBuckets is not None:
                    if isinstance(table.sd.numBuckets, int):
                        num_buckets = table.sd.numBuckets
                    else:
                        raise TypeError(
                            f"Expected numBuckets to be int, got {type(table.sd.numBuckets)}"
                        )
                else:
                    raise TypeError(
                        f"Expected numBuckets to be int, got {type(table.sd.numBuckets)}"
                    )

                bucket_property = HiveBucketProperty(
                    table.sd.bucketCols, num_buckets, version, sort_cols
                )

            if table.sd.skewedInfo is None:
                is_skewed = False
            else:
                is_skewed = True

            if table.sd.location is not None:
                if isinstance(table.sd.location, str):
                    location = table.sd.location
                else:
                    raise TypeError(
                        f"Expected location to be str, got {type(table.sd.location)}"
                    )
            else:
                location = None

            if table.sd.serdeInfo is not None:
                if isinstance(table.sd.serdeInfo, SerDeInfo):
                    serde_info = table.sd.serdeInfo
                else:
                    raise TypeError(
                        f"Expected serdeInfo to be SerDeInfo, got {type(table.sd.serdeInfo)}"
                    )
            else:
                raise TypeError(
                    f"Expected serdeInfo to be SerDeInfo, got {type(table.sd.serdeInfo)}"
                )
            if serde_info.parameters is not None:
                if isinstance(serde_info.parameters, dict):
                    serde_parameters = serde_info.parameters
                else:
                    raise TypeError(
                        f"Expected serdeInfo.parameters to be dict, got {type(serde_info.parameters)}"
                    )
            else:
                raise TypeError(
                    f"Expected serdeInfo.parameters to be dict, got {type(serde_info.parameters)}"
                )
        else:
            raise TypeError(
                f"Expected sd to be StorageDescriptor, got {type(table.sd)}"
            )

        storage = HStorage(
            storage_format,
            is_skewed,
            location,
            bucket_property,
            serde_parameters,
        )

        if table.parameters is not None:
            if isinstance(table.parameters, dict):
                params = table.parameters
            else:
                raise TypeError(
                    f"Expected parameters to be dict, got {type(table.parameters)}"
                )
        else:
            raise TypeError(
                f"Expected parameters to be dict, got {type(table.parameters)}"
            )

        if table.tableType is not None:
            if isinstance(table.tableType, str):
                table_type = table.tableType
            else:
                raise TypeError(
                    f"Expected tableType to be str, got {type(table.tableType)}"
                )
        else:
            raise TypeError(
                f"Expected tableType to be str, got {type(table.tableType)}"
            )

        if table.tableName is not None:
            table_name = table.tableName
        else:
            raise TypeError(
                f"Expected tableName to be str, got {type(table.tableName)}"
            )

        if table.dbName is not None:
            db_name = table.dbName
        else:
            raise TypeError(f"Expected dbName to be str, got {type(table.dbName)}")

        return HTable(
            db_name,
            table_name,
            table_type,
            columns,
            partition_columns,
            storage,
            params,
            table.viewOriginalText,
            table.viewExpandedText,
            table.writeId,
            table.owner,
        )

    def get_table_stats(
        self,
        table: HTable,
        columns: List[HColumn],
    ) -> List[ColumnStats]:
        assert isinstance(table, HTable)
        assert isinstance(columns, list)

        if len(columns) > 0:
            assert all(isinstance(column, HColumn) for column in columns)

        assert table.name is not None
        assert table.database_name is not None
        assert table.columns is not None
        assert len(table.columns) > 0

        if len(columns) == 0:
            columns = table.columns

        column_names = [column.name for column in columns]
        results = []

        for column in column_names:
            res = self.client.get_table_column_statistics(
                table.database_name,
                table.name,
                column,
            )
            results.append(res)

        result_columns = []
        for col in results:
            for obj in col.statsObj:
                name = obj.colName
                col_type = obj.colType
                stats = obj.statsData

                if stats.booleanStats is not None:
                    c_stats = BooleanTypeStats(
                        stats.booleanStats.numTrues,
                        stats.booleanStats.numFalses,
                        stats.booleanStats.numNulls,
                    )
                elif stats.longStats is not None:
                    c_stats = LongTypeStats(
                        stats.longStats.lowValue,
                        stats.longStats.highValue,
                        stats.longStats.numNulls,
                        stats.longStats.numDVs,
                    )
                elif stats.doubleStats is not None:
                    c_stats = DoubleTypeStats(
                        stats.doubleStats.lowValue,
                        stats.doubleStats.highValue,
                        stats.doubleStats.numNulls,
                    )
                elif stats.stringStats is not None:
                    c_stats = StringTypeStats(
                        stats.stringStats.avgColLen,
                        stats.stringStats.maxColLen,
                        stats.stringStats.numDVs,
                        stats.stringStats.numNulls,
                    )
                elif stats.binaryStats is not None:
                    c_stats = BinaryTypeStats(
                        stats.binaryStats.avgColLen,
                        stats.binaryStats.maxColLen,
                        stats.binaryStats.numNulls,
                    )
                elif stats.decimalStats is not None:
                    c_stats = DecimalTypeStats(
                        stats.decimalStats.lowValue,
                        stats.decimalStats.highValue,
                        stats.decimalStats.numNulls,
                        stats.decimalStats.numDVs,
                    )
                elif stats.dateStats is not None:
                    c_stats = DateTypeStats(
                        stats.dateStats.numDVs,
                        stats.dateStats.lowValue,
                        stats.dateStats.highValue,
                        stats.dateStats.numNulls,
                    )
                else:
                    c_stats = None

                result = ColumnStats(
                    catName=col.statsDesc.catName,
                    dbName=col.statsDesc.dbName,
                    tableName=col.statsDesc.tableName,
                    partName=col.statsDesc.partName,
                    isTblLevel=col.statsDesc.isTblLevel,
                    lastAnalyzed=col.statsDesc.lastAnalyzed,
                    columnName=name,
                    columnType=col_type,
                    stats=c_stats,
                )

                result_columns.append(result)

        return result_columns


class _HMSConnection:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.transport = None

    def __enter__(self):
        socket = TSocket.TSocket(self.host, self.port)
        self.transport = TTransport.TBufferedTransport(socket)
        protocol = TBinaryProtocol(self.transport)
        self.transport.open()
        return HMS(hms.Client(protocol))

    def __exit__(self, type, value, traceback):
        if self.transport is not None:
            self.transport.close()
