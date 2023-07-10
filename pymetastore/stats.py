from dataclasses import dataclass
from typing import Optional

from pymetastore.hive_metastore import ttypes


@dataclass
class TypeStats:
    numNulls: int
    typeName: str


@dataclass
class BooleanTypeStats(TypeStats):
    numTrues: int
    numFalses: int

    def __init__(self, numTrues: int, numFalses: int, numNulls: int):
        super().__init__(numNulls, typeName="boolean")
        self.numTrues = numTrues
        self.numFalses = numFalses


@dataclass
class DoubleTypeStats(TypeStats):
    cardinality: int
    lowValue: Optional[float]
    highValue: Optional[float]

    def __init__(
        self,
        cardinality: int,
        numNulls: int,
        lowValue: Optional[float] = None,
        highValue: Optional[float] = None,
    ):
        super().__init__(typeName="double", numNulls=numNulls)
        self.cardinality = cardinality
        self.lowValue = lowValue
        self.highValue = highValue


@dataclass
class LongTypeStats(TypeStats):
    cardinality: int
    lowValue: Optional[int]
    highValue: Optional[int]

    def __init__(
        self,
        cardinality: int,
        numNulls: int,
        lowValue: Optional[int] = None,
        highValue: Optional[int] = None,
    ):
        super().__init__(typeName="long", numNulls=numNulls)
        self.cardinality = cardinality
        self.lowValue = lowValue
        self.highValue = highValue


@dataclass
class StringTypeStats(TypeStats):
    maxColLen: int
    avgColLen: float
    cardinality: int

    def __init__(
        self, maxColLen: int, avgColLen: float, cardinality: int, numNulls: int
    ):
        super().__init__(typeName="string", numNulls=numNulls)
        self.maxColLen = maxColLen
        self.avgColLen = avgColLen
        self.cardinality = cardinality


@dataclass
class BinaryTypeStats(TypeStats):
    maxColLen: int
    avgColLen: float

    def __init__(self, maxColLen: int, avgColLen: float, numNulls: int):
        super().__init__(typeName="binary", numNulls=numNulls)
        self.maxColLen = maxColLen
        self.avgColLen = avgColLen


@dataclass
class DecimalTypeStats(TypeStats):
    cardinality: int
    lowValue: Optional[ttypes.Decimal]
    highValue: Optional[ttypes.Decimal]

    def __init__(
        self,
        cardinality: int,
        numNulls: int,
        lowValue: Optional[ttypes.Decimal] = None,
        highValue: Optional[ttypes.Decimal] = None,
    ):
        super().__init__(typeName="decimal", numNulls=numNulls)
        self.cardinality = cardinality
        self.lowValue = lowValue
        self.highValue = highValue


@dataclass
class DateTypeStats(TypeStats):
    cardinality: int
    lowValue: Optional[ttypes.Date]
    highValue: Optional[ttypes.Date]

    def __init__(
        self,
        cardinality: int,
        numNulls: int,
        lowValue: Optional[ttypes.Date] = None,
        highValue: Optional[ttypes.Date] = None,
    ):
        super().__init__(typeName="date", numNulls=numNulls)
        self.cardinality = cardinality
        self.lowValue = lowValue
        self.highValue = highValue


@dataclass
class ColumnStats:
    isTblLevel: bool
    dbName: str
    tableName: str
    columnName: str
    columnType: str
    stats: Optional[TypeStats] = None  # not all column types have stats
    partName: Optional[str] = None
    lastAnalyzed: Optional[int] = None
    catName: Optional[str] = None
