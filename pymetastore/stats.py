from dataclasses import dataclass
from htypes import HType
from typing import Optional

@dataclass
class TypeStats:
    typeName: str
    numNulls: int

@dataclass
class BooleanTypeStats(TypeStats):
    typeName: str = "boolean"
    numTrues: int
    numFalses: int

@dataclass
class DoubleTypeStats(TypeStats):
    typeName: str = "double"
    lowValue: Optional(float) = None
    highValue: Optional(float) = None
    cardinality: int

@dataclass
class LongTypeStats(TypeStats):
    typeName: str = "long"
    lowValue: Optional(int) = None
    highValue: Optional(int) = None
    cardinality: int

@dataclass
class StringTypeStats(TypeStats):
    typeName: str = "string"
    maxColLen: int
    avgColLen: float
    cardinality: int

@dataclass
class BinaryTypeStats(TypeStats):
    typeName: str = "binary"
    maxColLen: int
    avgColLen: float

@dataclass
class DecimalTypeStats(TypeStats):
    typeName: str = "decimal"
    lowValue: Optional(float) = None
    highValue: Optional(float) = None
    cardinality: int

@dataclass
class DateTypeStats(TypeStats):
    typeName: str = "date"
    lowValue: Optional(int) = None
    highValue: Optional(int) = None
    cardinality: int

@dataclass
class ColumnStats:
    isTblLevel: bool
    dbNmae: str
    tableName: str
    partName: Optional(str) = None
    lastAnalyzed: Optional(int) = None
    catName: Optional(str) = None
    columnName: str
    columnType: HType
    stats: TypeStats




