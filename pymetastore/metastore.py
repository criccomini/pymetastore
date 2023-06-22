from enum import Enum
from typing import Dict, List, Optional

from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport

from .hive_metastore import ThriftHiveMetastore as hms
from .hive_metastore.ttypes import *
from .htypes import HType


class HPrincipalType(Enum):
    ROLE = "ROLE"
    USER = "USER"
    GROUP = "GROUP"

class HPrivilegeGrantInfo:
    def __init__(self, privilege: str, grantor: str, grantor_type: HPrincipalType, create_time: int, grant_option: bool):
        self.privilege = privilege
        self.grantor = grantor
        self.grantor_type = grantor_type
        self.create_time = create_time
        self.grant_option = grant_option

class HPrincipalPrivilegeSet:
    def __init__(self, user_privileges: List[HPrivilegeGrantInfo], group_privileges: List[HPrivilegeGrantInfo], role_privileges: List[HPrivilegeGrantInfo]):
        self.user_privileges = user_privileges
        self.group_privileges = group_privileges
        self.role_privileges = role_privileges

class HDatabase:
    def __init__(
        self,
        name: str,
        location: Optional[str] = None,
        owner_name: Optional[str] = None,
        owner_type: Optional[HPrincipalType] = None,
        comment: Optional[str] = None,
        parameters: Optional[Dict[str, str]] = None,
    ):
        self.name = name
        self.location = location
        self.owner_name = owner_name
        self.owner_type = owner_type
        self.comment = comment
        self.parameters = parameters


class HColumn:
    def __init__(self, name: str, type: HType, comment: Optional[str] = None):
        self.name = name
        self.type = type
        self.comment = comment


class HSortingOrder(Enum):
    ASC = 1
    DESC = 0

class HSortingColumn:
    def __init__(self, order: HSortingOrder):
        self.column_name = order.col
        
        if order.order == HSortingOrder.ASC:
            self.order = HSortingOrder.ASC
        else:
            self.order = HSortingOrder.DESC


class BucketingVersion(Enum):
    V1 = 1
    V2 = 2

class HiveBucketProperty:
    def __init__(self, bucketed_by: List[str], bucket_count: int, version: BucketingVersion = None, sorting_columns: List[HSortingColumn] = None):
        self.bucketed_by = bucketed_by
        self.bucket_count = bucket_count
        self.version = version
        self.sorting_columns = sorting_columns


class StorageFormat:
    def __init__(self, serde: str, input_format: str, output_format: str) -> None:
        self.serde = serde
        self.input_format = input_format
        self.output_format = output_format


class HStorage:
    def __init__(
        self,
        storage_format: StorageFormat,
        skewed: bool = False,
        location: Optional[str] = None,
        bucket_property: Optional[HiveBucketProperty] = None,
        serde_parameters: Optional[Dict[str, str]] = None,
    ):
        if storage_format is None:
            raise ValueError("storageFormat cannot be None")
        self.storage_format = storage_format
        self.skewed = skewed
        self.location = location
        self.bucket_property = bucket_property
        self.serde_parameters = serde_parameters


class HTable:
    def __init__(
        self,
        database_name: str,
        name: str,
        table_type: str,
        columns: List[HColumn],
        partition_columns: List[HColumn],
        storage: HStorage,
        parameters: Dict[str, str],
        view_original_text: Optional[str] = None,
        view_expanded_text: Optional[str] = None,
        write_id: Optional[int] = None,
        owner: Optional[str] = None,
    ):
        self.database_name = database_name
        self.name = name
        self.storage = storage
        self.table_type = table_type
        self.columns = columns
        self.partition_columns = partition_columns
        self.parameters = parameters
        self.view_original_text = view_original_text
        self.view_expanded_text = view_expanded_text
        self.write_id = write_id
        self.owner = owner

class HPartition:
    def __init__(self, 
                 database_name: str, 
                 table_name: str, 
                 values: List[str],
                 parameters: Dict[str, str], 
                 create_time: int, 
                 last_access_time: int, 
                 sd: HStorage,
                 privilege_set: HPrincipalPrivilegeSet = None,
                 cat_name: str = None,
                 write_id: int = None
                 ):
        self.database_name = database_name
        self.table_name = table_name
        self.values = values
        self.parameters = parameters
        self.create_time = create_time
        self.last_access_time = last_access_time
        self.sd = sd
        self.privilege_set = privilege_set
        self.cat_name = cat_name
        self.write_id = write_id

class HMS:
    def __init__(self, client: hms.Client):
        self.client = client

    @staticmethod
    def create(host="localhost", port=9083):
        host = host
        port = port
        socket = TSocket.TSocket(host, port)
        transport = TTransport.TBufferedTransport(socket)
        protocol = TBinaryProtocol(transport)
        transport.open()
        yield hms.Client(protocol)
        transport.close()

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

    def list_tables(self, databaseName: str) -> List[str]:
        tables = self.client.get_all_tables(databaseName)
        table_names = []
        for table in tables:
            table_names.append(table.tableName)
        return table_names

    def list_columns(self, databaseName: str, tableName: str) -> List[str]:
        # TODO: Rather than ignore these pyright errors, do appropriate None handling
        columns = self.client.get_table(
            databaseName,
            tableName,
        ).sd.cols  # pyright: ignore[reportOptionalMemberAccess]
        self.client.get_schema(databaseName, tableName)
        column_names = []
        for column in columns:  # pyright: ignore[reportOptionalIterable]
            column_names.append(column.name)
        return column_names
    
    def list_partitions(self, databaseName: str, tableName: str, max_parts: int = -1) -> List[str]:
        partitions = self.client.get_partition_names(databaseName, tableName, max_parts)
        return partitions
    
    def get_partition(self, databaseName: str, tableName: str, partition_name: str) -> HPartition:
        
        partition: Partition = self.client.get_partition_by_name(databaseName, tableName, partition_name)
        
        storage_format = StorageFormat(partition.sd.serdeInfo.serializationLib, partition.sd.inputFormat, partition.sd.outputFormat)
        bucket_property = HiveBucketProperty(partition.sd.bucketCols, partition.sd.numBuckets, partition.sd.sortCols)
        sd = HStorage(storage_format,partition.sd.skewedInfo, partition.sd.location, bucket_property, partition.sd.serdeInfo.parameters) 
        
        result_partition = HPartition(partition.dbName, 
                                      partition.tableName, 
                                      partition.values, 
                                      partition.parameters, 
                                      partition.createTime, 
                                      partition.lastAccessTime, 
                                      sd, 
                                      partition.privileges, 
                                      partition.catName, 
                                      partition.writeId)
        return result_partition

    def get_table(self, databaseName: str, tableName: str) -> HTable:
        table: Table = self.client.get_table(databaseName, tableName)
        columns = []

        t_columns: List[FieldSchema] = table.sd.cols
        
        for column in t_columns:
            type_parser = TypeParser(column.type)
    
            columns.append(HColumn(column.name, type_parser.parse_type(), column.comment))
        
        t_part_columns: List[FieldSchema] = table.partitionKeys

        partition_columns = []
        for column in t_part_columns:
            type_parser = TypeParser(column.type)
            partition_columns.append(HColumn(column.name, type_parser.parse_type(), column.comment))
        
        storage_format = StorageFormat(table.sd.serdeInfo.serializationLib, table.sd.inputFormat, table.sd.outputFormat)
        

        bucket_property = None
        if table.sd.bucketCols is not None:
            sort_cols = []
            for col in table.sd.sortCols:
                sort_cols.append(HSortingColumn(col))

            version = BucketingVersion.V1
            if table.parameters.get("TABLE_BUCKETING_VERSION", BucketingVersion.V1) == BucketingVersion.V2:
               version = BucketingVersion.V2

            bucket_property = HiveBucketProperty(table.sd.bucketCols, table.sd.numBuckets, version, sort_cols)

        storage = HStorage(storage_format, table.sd.skewedInfo is not None, table.sd.location, bucket_property, table.sd.serdeInfo.parameters)
        
        return HTable(table.dbName, 
                      table.tableName, 
                      table.tableType, 
                      columns, 
                      partition_columns, 
                      storage, 
                      table.parameters, 
                      table.viewOriginalText, 
                      table.viewExpandedText, 
                      table.writeId, table.owner)

            

            
            
        
