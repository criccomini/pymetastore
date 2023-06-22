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


# TODO: Implement
class HiveBucketProperty:
    def __init__(self):
        # TODO!!
        pass


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
