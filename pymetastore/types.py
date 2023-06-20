from typing import List, Any
from collections import namedtuple
from enum import Enum

class PrimitiveCategory(Enum):
    VOID = "VOID"
    BOOLEAN = "BOOLEAN"
    BYTE = "BYTE"
    SHORT = "SHORT"
    INT = "INT"
    LONG = "LONG"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    STRING = "STRING"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMPLOCALTZ = "TIMESTAMPLOCALTZ"
    BINARY = "BINARY"
    DECIMAL = "DECIMAL"
    VARCHAR = "VARCHAR"
    CHAR = "CHAR"
    INTERVAL_YEAR_MONTH = "INTERVAL_YEAR_MONTH"
    INTERVAL_DAY_TIME = "INTERVAL_DAY_TIME"
    UNKNOWN = "UNKNOWN"

class HTypeCategory(Enum):
    PRIMITIVE = PrimitiveCategory
    STRUCT = "STRUCT"
    MAP = "MAP"
    LIST = "LIST"
    UNION = "UNION"

class HType:
    def __init__(self, name: str, category: HTypeCategory):
        self.name = name
        self.category = category

class HPrimitiveType(HType):
    def __init__(self, primitiveType: PrimitiveCategory):
        super().__init__(primitiveType.value, HTypeCategory.PRIMITIVE)
        self.primitiveType = primitiveType

class HMapType(HType):
    def __init__(self, keyType: HType, valueType: HType):
        super().__init__("MAP", HTypeCategory.MAP)
        self.keyType = keyType
        self.valueType = valueType

class HListType(HType):
    def __init__(self, elementType: HType):
        super().__init__("LIST", HTypeCategory.LIST)
        self.elementType = elementType

class HUnionType(HType):
    def __init__(self, types: List[HType]):
        super().__init__("UNION", HTypeCategory.UNION)
        self.types = types

class HVarcharType(HType):
    def __init__(self, length: int):
        MAX_VARCHAR_LENGTH = 65535
        if length > MAX_VARCHAR_LENGTH:
            raise ValueError("Varchar length cannot exceed 65535")
        super().__init__("VARCHAR", HTypeCategory.PRIMITIVE)
        self.length = length

class HCharType(HType):
    def __init__(self, length: int):
        MAX_CHAR_LENGTH = 255
        if length > MAX_CHAR_LENGTH:
            raise ValueError("Char length cannot exceed 255")
        super().__init__("CHAR", HTypeCategory.PRIMITIVE)
        self.length = length

class HDecimalType(HType):
    def __init__(self, precision: int, scale: int):
        MAX_PRECISION = 38
        MAX_SCALE = 38
        if precision > MAX_PRECISION:
            raise ValueError("Decimal precision cannot exceed 38")
        if scale > MAX_SCALE:
            raise ValueError("Decimal scale cannot exceed 38")
        super().__init__("DECIMAL", HTypeCategory.PRIMITIVE)
        self.precision = precision
        self.scale = scale

class HStructType(HType):
    def __init__(self, names: List[str], types:List[HType]):
        if len(names) != len(types):
            raise ValueError("mismatched size of names and types.")
        if None in names:
            raise ValueError("names cannot contain None")
        if None in types:
            raise ValueError("types cannot contain None")
        
        super().__init__("STRUCT", HTypeCategory.STRUCT)
        self.names = names
        self.types = types

# We need to maintain a list of the expected serialized type names. Thrift will return the type in string format and we will have to parse it to get the type.
class SerdeTypeNameConstants(Enum):
    # Primitive types
    VOID = "void"
    BOOLEAN = "boolean"
    TINYINT = "tinyint"
    SMALLINT = "smallint"
    INT = "int"
    BIGINT = "bigint"
    FLOAT = "float"
    DOUBLE = "double"
    STRING = "string"
    DATE = "date"
    CHAR = "char"
    VARCHAR = "varchar"
    TIMESTAMP = "timestamp"
    TIMESTAMPLOCALTZ = "timestamp with local time zone"
    DECIMAL = "decimal"
    BINARY = "binary"
    INTERVAL_YEAR_MONTH = "interval_year_month"
    INTERVAL_DAY_TIME = "interval_day_time"
    # Complex types
    LIST = "array"
    MAP = "map"
    STRUCT = "struct"
    UNION = "uniontype"
    UNKNOWN = "unknown"

# We need a mapping between the two definitions of primitive types. The first is the one used by Hive and the second is the one used by Thrift returned values.
primitive_to_serde_mapping = {
    PrimitiveCategory.VOID: SerdeTypeNameConstants.VOID,
    PrimitiveCategory.BOOLEAN: SerdeTypeNameConstants.BOOLEAN,
    PrimitiveCategory.BYTE: SerdeTypeNameConstants.TINYINT,
    PrimitiveCategory.SHORT: SerdeTypeNameConstants.SMALLINT,
    PrimitiveCategory.INT: SerdeTypeNameConstants.INT,
    PrimitiveCategory.LONG: SerdeTypeNameConstants.BIGINT,
    PrimitiveCategory.FLOAT: SerdeTypeNameConstants.FLOAT,
    PrimitiveCategory.DOUBLE: SerdeTypeNameConstants.DOUBLE,
    PrimitiveCategory.STRING: SerdeTypeNameConstants.STRING,
    PrimitiveCategory.DATE: SerdeTypeNameConstants.DATE,
    PrimitiveCategory.TIMESTAMP: SerdeTypeNameConstants.TIMESTAMP,
    PrimitiveCategory.TIMESTAMPLOCALTZ: SerdeTypeNameConstants.TIMESTAMPLOCALTZ,
    PrimitiveCategory.BINARY: SerdeTypeNameConstants.BINARY,
    PrimitiveCategory.DECIMAL: SerdeTypeNameConstants.DECIMAL,
    PrimitiveCategory.VARCHAR: SerdeTypeNameConstants.VARCHAR,
    PrimitiveCategory.CHAR: SerdeTypeNameConstants.CHAR,
    PrimitiveCategory.INTERVAL_YEAR_MONTH: SerdeTypeNameConstants.INTERVAL_YEAR_MONTH,
    PrimitiveCategory.INTERVAL_DAY_TIME: SerdeTypeNameConstants.INTERVAL_DAY_TIME,
    PrimitiveCategory.UNKNOWN: SerdeTypeNameConstants.UNKNOWN
}

# We also need the inverse though. 
serde_to_primitive_mapping = {v: k for k, v in primitive_to_serde_mapping.items()}

# This function gets us the serde type name from the primitive one.
def primitive_to_serde(primitive_category: PrimitiveCategory) -> SerdeTypeNameConstants:
    return primitive_to_serde_mapping[primitive_category]

# This function gets us the primitive type name from the serde one.
def serde_to_primitive(serde_type_name: SerdeTypeNameConstants) -> PrimitiveCategory:
    return serde_to_primitive_mapping[serde_type_name]

# We also need a way to get the serde enum key from the string we get from Thrift.
def get_serde_type_by_value(value):
    for member in SerdeTypeNameConstants:
        if member.value == value:
            return member.name
    raise ValueError(f"{value} not found in {SerdeTypeNameConstants.__name__}")

# To be used for tokening the Thrift type string. We need to tokenize the string to get the type name and the type parameters.
class Token(namedtuple('Token', ['position', 'text', 'type'])):
    def __new__(cls, position: int, text: str, type_: bool) -> Any:
        if text is None:
            raise ValueError("text is null")
        return super().__new__(cls, position, text, type_)

    def __str__(self):
        return f"{self.position}:{self.text}"

class PrimitiveParts(namedtuple('PrimitiveParts', ['type_name', 'type_params'])):
    def __new__(cls, type_name: str, type_params: List[str]) -> Any:
        if type_name is None:
            raise ValueError("type_name is null")
        if type_params is None:
            raise ValueError("type_params is null")
        return super().__new__(cls, type_name, type_params)

    def __str__(self):
        return f"{self.type_name}:{self.type_params}"

# Util functions to parse the Thrift column types (as strings) and return the expected hive type
class TypeParser:

    def __init__(self, type_info_string: str):
        self.type_string = type_info_string
        self.type_tokens = self.tokenize(type_info_string)
        self.index = 0
        
    # we want the base name of the type, in general the format of a parameterized type is <base_name>(<type1>, <type2>, ...), so the base name is the string before the first '('
    @staticmethod
    def get_base_name(type_name: str) -> str:
        index = type_name.find('(')
        if index == -1:
            return type_name
        return type_name[:index]
    
    # Is the type named using the allowed characters?
    @staticmethod
    def is_valid_type_char(c: str) -> bool:
        return c.isalnum() or c in ('_', '.', ' ', '$')
    
    # The concept of the tokenizer is to split the type string into tokens. The tokens are either type names or type parameters. The type parameters are enclosed in parentheses.
    def tokenize(self, type_info_string: str) -> List[Token]:
        tokens = []
        begin = 0
        end = 1
        while end <= len(type_info_string):
            if begin > 0 and type_info_string[begin - 1] == '(' and type_info_string[begin] == "'":
                begin += 1
                end += 1
                while type_info_string[end] != "'":
                    end += 1

            elif type_info_string[begin] == "'" and type_info_string[begin + 1] == ')':
                begin += 1
                end += 1

            if (
                end == len(type_info_string)
                or not self.is_valid_type_char(type_info_string[end - 1])
                or not self.is_valid_type_char(type_info_string[end])
            ):
                token = Token(begin, type_info_string[begin:end], self.is_valid_type_char(type_info_string[begin]))
                tokens.append(token)
                begin = end
            end += 1
        
        return tokens
    
    def peek(self) -> Token:
        if self.index >= len(self.type_tokens):
            raise ValueError("Error: Unexpected end of 'typeInfoString'")
        return self.type_tokens[self.index]

    def expect(self, item: str, alternative: str = None) -> Token:
        if self.index >= len(self.type_tokens):
            raise ValueError(f"Error: {item} expected at the end of 'typeInfoString'")

        token = self.type_tokens[self.index]

        if item == "type":
            if token.text() not in [SerdeTypeNameConstants.LIST.value,
                                SerdeTypeNameConstants.MAP.value,
                                SerdeTypeNameConstants.STRUCT.value,
                                SerdeTypeNameConstants.UNION.value] and (self.get_base_name(token.text()) is None) and (token.text() != alternative):
                raise ValueError(f"Error: {item} expected at the position {token.position()} of 'typeInfoString' but '{token.text()}' is found.")
        elif item == "name":
            if not token.type() and token.text() != alternative:
                raise ValueError(f"Error: {item} expected at the position {token.position()} of 'typeInfoString' but '{token.text()}' is found.")
        elif item != token.text() and token.text() != alternative:
            raise ValueError(f"Error: {item} expected at the position {token.position()} of 'typeInfoString' but '{token.text()}' is found.")

        self.index += 1 # increment the index
        return token
    
    def parse_params(self) -> List[str]:
        params = []

        token = self.peek(self.type_tokens, index)
        if token is not None and token.text() == "(":
            token, index = self.expect("(", None, self.type_tokens, index)
            token = self.peek(self.type_tokens, index)
            while token is None or token.text() != ")":
                token, index = self.expect("name", None, self.type_tokens, index)
                params.append(token.text())
                token, index = self.expect(",", ")", self.type_tokens, index)

            if not params:
                raise ValueError("type parameters expected for type string 'typeInfoString'")

        return params
    
    def parse_type(self) -> HType:
        token: Token = self.expect("type")

        # first we take care of primitive types.
        serde_val = get_serde_type_by_value(token.text())
        if serde_val is not None:
            primitive_type = serde_to_primitive(serde_val)
            if primitive_type is not PrimitiveCategory.UNKNOWN:
                params = self.parse_params()
                if primitive_type is PrimitiveCategory.CHAR or PrimitiveCategory.VARCHAR:
                    if len(params) == 0:
                        raise ValueError("char/varchar type must have a length specified")
                    if len(params) == 1:
                        length = int(params[0])
                        if primitive_type is PrimitiveCategory.CHAR:
                            return HCharType(length)
                        elif primitive_type is PrimitiveCategory.VARCHAR:
                            return HVarcharType(length)
                    else: 
                        raise ValueError(f"Error: {token.text()} type takes only one parameter, but instead {len(params)} parameters are found.")
                elif primitive_type is PrimitiveCategory.DECIMAL:
                    if len(params) == 0:
                        return HDecimalType(10, 0)
                    elif len(params) == 1:
                        precision = int(params[0])
                        return HDecimalType(precision, 0)
                    elif len(params) == 2:
                        precision = int(params[0])
                        scale = int(params[1])
                        return HDecimalType(precision, scale)
                    else:
                        raise ValueError(f"Error: {token.text()} type takes only two parameters, but instead {len(params)} parameters are found.")
                else:
                    return HPrimitiveType(primitive_type)
        
        # next we take care of complex types.
        if SerdeTypeNameConstants.LIST.value == token.text():
            self.expect("<")
            element_type = self.parse_type()
            self.expect(">")
            return HListType(element_type)
        
        if SerdeTypeNameConstants.MAP.value == token.text():
            self.expect("<")
            key_type = self.parse_type()
            self.expect(",")
            value_type = self.parse_type()
            self.expect(">")
            return HMapType(key_type, value_type)
        
        if SerdeTypeNameConstants.STRUCT.value == token.text():
            self.expect("<")
            names = []
            types = []
            token = self.peek()
            while token is not None and token.text() != ">":
                field_name = self.expect("name")
                self.expect(":")
                field_type = self.parse_type()
                names.append(field_name)
                types.append(field_type)
                token = self.peek()
                if token.text() == ",":
                    self.expect(",")
                    token = self.peek()
            self.expect(">")
            return HStructType(names, types)
        
        if SerdeTypeNameConstants.UNION.value == token.text():
            self.expect("<")
            types = []
            token = self.peek()
            while token is not None and token.text() != ">":
                field_type = self.parse_type()
                types.append(field_type)
                token = self.peek()
                if token.text() == ",":
                    self.expect(",")
                    token = self.peek()
            self.expect(">")
            return HUnionType(types)
        
        # if we reach this point and we haven't figure out what to do, then we better raise an error.
        raise ValueError(f"Error: {token.text()} is not a valid type.")