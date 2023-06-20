from enum import Enum
from typing import Dict, List
from htypes import HType
from thrift.transport import TTransport, TSocket
from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from hms.ttypes import *
from hms import ThriftHiveMetastore as hms

class HPrincipalType(Enum):
    ROLE = "ROLE"
    USER = "USER"

class HDatabase:
    def __init__(self, name: str, 
                 location: str =None, 
                 ownerName:str =None, 
                 ownerType: HPrincipalType =None, 
                 comment=None, 
                 parameters: Dict[str, str]=None):
        
        self.name = name
        self.location = location
        self.ownerName = ownerName
        self.ownerType = ownerType
        self.comment = comment
        self.parameters = parameters

class HColumn:
    def __init__(self, name: str, type: HType, comment: str =None):
        self.name = name
        self.type = type
        self.comment = comment

#TODO: Implement
class HiveBucketProperty:
    def __init__(self):
        # TODO!!
        pass

class StorageFormat:
    def __init__(self, serde: str, inputFormat: str, outputFormat: str) -> None:
        self.serde = serde
        self.inputFormat = inputFormat
        self.outputFormat = outputFormat

class HStorage:
    def __init__(self, storageFormat: StorageFormat,
                 skewed: bool = False, 
                 location: str =None, 
                 bucketProperty: HiveBucketProperty =None, 
                 serdeParameters: Dict[str, str] =None):
        
        if storageFormat is None:
            raise ValueError("storageFormat cannot be None")
        self.storageFormat = storageFormat
        self.skewed = skewed
        self.location = location
        self.bucketProperty = bucketProperty
        self.serdeParameters = serdeParameters

class HTable:
    def __init__(self, databaseName: str, name: str, 
                 tableType: str, columns: List[HColumn], 
                 partitionColumns: List[HColumn],
                 storage: HStorage, 
                 parameters: Dict[str, str], 
                 viewOriginalText: str = None, 
                 viewExpandedText: str = None, 
                 writeId: int = None, 
                 owner: str = None):
        
        self.databaseName = databaseName
        self.name = name
        self.storage = storage
        self.tableType = tableType
        self.columns = columns
        self.partitionColumns = partitionColumns
        self.parameters = parameters
        self.viewOriginalText = viewOriginalText
        self.viewExpandedText = viewExpandedText
        self.writeId = writeId
        self.owner = owner

class HMS:
    def __init__(self, host='localhost', port=9090):
        self.host = host
        self.port = port
        self.socket = TSocket.TSocket(self.host, self.port)
        self.transport = TTransport.TBufferedTransport(self.socket)
        self.protocol = TBinaryProtocol(self.transport)
        self.client = hms.Client(self.protocol)

    def connect(self):
        self.transport.open()

    def disconnect(self):
        self.transport.close()

    def list_databases(self) -> List[str]:
        databases = self.client.get_all_databases()
        db_names = []
        for database in databases:
            db_names.append(database.name)
        return db_names
            
    def get_database(self, name: str) -> HDatabase:
        db: Database = self.client.get_database(name)
        
        if db.ownerType is PrincipalType.USER:
            ownerType = HPrincipalType.USER
        elif db.ownerType is PrincipalType.ROLE:
            ownerType = HPrincipalType.ROLE
        else:
            ownerType = None

        return HDatabase(db.name, db.locationUri, db.ownerName, ownerType, db.description, db.parameters)
    
    def list_tables(self, databaseName: str) -> List[str]:
        tables = self.client.get_all_tables(databaseName)
        table_names = []
        for table in tables:
            table_names.append(table.tableName)
        return table_names
    
    def list_columns(self, databaseName: str, tableName: str) -> List[str]:
        columns = self.client.get_table(databaseName, tableName).sd.cols
        self.client.get_schema(databaseName, tableName)
        column_names = []
        for column in columns:
            column_names.append(column.name)
        return column_names