import pytest

from pymetastore.htypes import (
    HCharType,
    HDecimalType,
    HListType,
    HMapType,
    HPrimitiveType,
    HStructType,
    HTypeCategory,
    HUnionType,
    HVarcharType,
    PrimitiveCategory,
    SerdeTypeNameConstants,
    Token,
    TypeParser,
)

# Although type systems change, it is a rare event and even if it happens, we
# should be very careful as way too many things can break. For this reason
# we'll be testing values and members for the types we define to catch any
# changes. For this reason we'll be primarily be testing the values and not the
# utility functions for the conversion between types. These functions are
# trivial and they primarily expose a thin API over the mappings we define.


# tests for primitive types.
def test_primitivecategory_members():
    expected_members = {
        "VOID",
        "BOOLEAN",
        "BYTE",
        "SHORT",
        "INT",
        "LONG",
        "FLOAT",
        "DOUBLE",
        "STRING",
        "DATE",
        "TIMESTAMP",
        "TIMESTAMPLOCALTZ",
        "BINARY",
        "DECIMAL",
        "VARCHAR",
        "CHAR",
        "INTERVAL_YEAR_MONTH",
        "INTERVAL_DAY_TIME",
        "UNKNOWN",
    }

    actual_members = {member.name for member in PrimitiveCategory}
    assert actual_members == expected_members


def test_primitivecategory_values():
    expected_values = {
        "VOID",
        "BOOLEAN",
        "BYTE",
        "SHORT",
        "INT",
        "LONG",
        "FLOAT",
        "DOUBLE",
        "STRING",
        "DATE",
        "TIMESTAMP",
        "TIMESTAMPLOCALTZ",
        "BINARY",
        "DECIMAL",
        "VARCHAR",
        "CHAR",
        "INTERVAL_YEAR_MONTH",
        "INTERVAL_DAY_TIME",
        "UNKNOWN",
    }

    actual_values = {member.value for member in PrimitiveCategory}
    assert actual_values == expected_values


# tests for HType definitions.
def test_htype_members():
    expected_members = {"PRIMITIVE", "STRUCT", "MAP", "LIST", "UNION"}

    actual_members = {member.name for member in HTypeCategory}
    assert actual_members == expected_members


def test_htype_values():
    expected_values = {PrimitiveCategory, "STRUCT", "MAP", "LIST", "UNION"}

    actual_values = {member.value for member in HTypeCategory}
    assert actual_values == expected_values


# Tests for the Serde definitions. Remember that we are converting between
# Thrift and Hive types so we need to account for that. It's important to test
# that stuff here because the thrift types are auto generated and we should
# catch any changes that might happen in the .thrift files as soon as possible.
def test_serdetypes_members():
    expected_members = {
        "VOID",
        "BOOLEAN",
        "TINYINT",
        "SMALLINT",
        "INT",
        "BIGINT",
        "FLOAT",
        "DOUBLE",
        "STRING",
        "DATE",
        "CHAR",
        "VARCHAR",
        "TIMESTAMP",
        "TIMESTAMPLOCALTZ",
        "DECIMAL",
        "BINARY",
        "INTERVAL_YEAR_MONTH",
        "INTERVAL_DAY_TIME",
        "LIST",
        "MAP",
        "STRUCT",
        "UNION",
        "UNKNOWN",
    }

    actual_members = {member.name for member in SerdeTypeNameConstants}
    assert actual_members == expected_members


def test_serdetypes_values():
    expected_values = {
        "void",
        "boolean",
        "tinyint",
        "smallint",
        "int",
        "bigint",
        "float",
        "double",
        "string",
        "date",
        "char",
        "varchar",
        "timestamp",
        "timestamp with local time zone",
        "decimal",
        "binary",
        "interval_year_month",
        "interval_day_time",
        "array",
        "map",
        "struct",
        "uniontype",
        "unknown",
    }

    actual_values = {member.value for member in SerdeTypeNameConstants}
    assert actual_values == expected_values


def test_token_creation():
    # Test successful creation of Token instance
    token = Token(position=0, text="double", type_=True)
    assert token.position == 0
    assert token.text == "double"
    assert token.type is True

    # Test the string representation of the Token instance
    assert str(token) == "0:double"

    # Test failure when providing None as text
    with pytest.raises(ValueError):
        Token(position=0, text=None, type_=True)


def test_token_properties():
    token = Token(position=1, text="<", type_=False)

    # Test the properties (position, text, type_) of the Token instance
    assert token.position == 1
    assert token.text == "<"
    assert token.type is False

    # Test the string representation of the Token instance
    assert str(token) == "1:<"


# Tests for the TypeParser class
def test_typeparser_tokenize():
    parser = TypeParser("STRING")
    assert parser.index == 0
    assert parser.type_tokens == [Token(position=0, text="STRING", type_=True)]
    parser = TypeParser("STRUCT<a:INT,b:STRING>")
    assert parser.index == 0

    assert parser.type_tokens == [
        Token(position=0, text="STRUCT", type_=True),
        Token(position=6, text="<", type_=False),
        Token(position=7, text="a", type_=True),
        Token(position=8, text=":", type_=False),
        Token(position=9, text="INT", type_=True),
        Token(position=12, text=",", type_=False),
        Token(position=13, text="b", type_=True),
        Token(position=14, text=":", type_=False),
        Token(position=15, text="STRING", type_=True),
        Token(position=21, text=">", type_=False),
    ]

    parser = TypeParser("MAP<STRING,STRUCT<a:INT,b:STRING>>")
    assert parser.index == 0
    assert parser.type_tokens == [
        Token(position=0, text="MAP", type_=True),
        Token(position=3, text="<", type_=False),
        Token(position=4, text="STRING", type_=True),
        Token(position=10, text=",", type_=False),
        Token(position=11, text="STRUCT", type_=True),
        Token(position=17, text="<", type_=False),
        Token(position=18, text="a", type_=True),
        Token(position=19, text=":", type_=False),
        Token(position=20, text="INT", type_=True),
        Token(position=23, text=",", type_=False),
        Token(position=24, text="b", type_=True),
        Token(position=25, text=":", type_=False),
        Token(position=26, text="STRING", type_=True),
        Token(position=32, text=">", type_=False),
        Token(position=33, text=">", type_=False),
    ]

    parser = TypeParser("UNIONTYPE<int,double,array<string>,struct<a:int,b:string>>")
    assert parser.index == 0

    assert parser.type_tokens == [
        Token(position=0, text="UNIONTYPE", type_=True),
        Token(position=9, text="<", type_=False),
        Token(position=10, text="int", type_=True),
        Token(position=13, text=",", type_=False),
        Token(position=14, text="double", type_=True),
        Token(position=20, text=",", type_=False),
        Token(position=21, text="array", type_=True),
        Token(position=26, text="<", type_=False),
        Token(position=27, text="string", type_=True),
        Token(position=33, text=">", type_=False),
        Token(position=34, text=",", type_=False),
        Token(position=35, text="struct", type_=True),
        Token(position=41, text="<", type_=False),
        Token(position=42, text="a", type_=True),
        Token(position=43, text=":", type_=False),
        Token(position=44, text="int", type_=True),
        Token(position=47, text=",", type_=False),
        Token(position=48, text="b", type_=True),
        Token(position=49, text=":", type_=False),
        Token(position=50, text="string", type_=True),
        Token(position=56, text=">", type_=False),
        Token(position=57, text=">", type_=False),
    ]
    parser = TypeParser("uniontype<integer,string>")
    assert parser.index == 0
    assert parser.type_tokens == [
        Token(position=0, text="uniontype", type_=True),
        Token(position=9, text="<", type_=False),
        Token(position=10, text="integer", type_=True),
        Token(position=17, text=",", type_=False),
        Token(position=18, text="string", type_=True),
        Token(position=24, text=">", type_=False),
    ]

    parser = TypeParser(
        "STRUCT<houseno:STRING,streetname:STRING,town:STRING,postcode:STRING>"
    )
    assert parser.index == 0
    assert parser.type_tokens == [
        Token(position=0, text="STRUCT", type_=True),
        Token(position=6, text="<", type_=False),
        Token(position=7, text="houseno", type_=True),
        Token(position=14, text=":", type_=False),
        Token(position=15, text="STRING", type_=True),
        Token(position=21, text=",", type_=False),
        Token(position=22, text="streetname", type_=True),
        Token(position=32, text=":", type_=False),
        Token(position=33, text="STRING", type_=True),
        Token(position=39, text=",", type_=False),
        Token(position=40, text="town", type_=True),
        Token(position=44, text=":", type_=False),
        Token(position=45, text="STRING", type_=True),
        Token(position=51, text=",", type_=False),
        Token(position=52, text="postcode", type_=True),
        Token(position=60, text=":", type_=False),
        Token(position=61, text="STRING", type_=True),
        Token(position=67, text=">", type_=False),
    ]


# Basic Types
def test_basic_type_parser():
    # String Type
    parser = TypeParser("string")
    ptype = parser.parse_type()
    assert ptype.name == HPrimitiveType(PrimitiveCategory.STRING).name
    assert ptype.category == HPrimitiveType(PrimitiveCategory.STRING).category

    # Int Type
    parser = TypeParser("int")
    ptype = parser.parse_type()
    assert ptype.name == HPrimitiveType(PrimitiveCategory.INT).name
    assert ptype.category == HPrimitiveType(PrimitiveCategory.INT).category

    # Long Type
    parser = TypeParser("double")
    ptype = parser.parse_type()
    assert ptype.name == HPrimitiveType(PrimitiveCategory.DOUBLE).name
    assert ptype.category == HPrimitiveType(PrimitiveCategory.DOUBLE).category

    # Float Type
    parser = TypeParser("float")
    ptype = parser.parse_type()
    assert ptype.name == HPrimitiveType(PrimitiveCategory.FLOAT).name
    assert ptype.category == HPrimitiveType(PrimitiveCategory.FLOAT).category

    # Boolean Type
    parser = TypeParser("boolean")
    ptype = parser.parse_type()
    assert ptype.name == HPrimitiveType(PrimitiveCategory.BOOLEAN).name
    assert ptype.category == HPrimitiveType(PrimitiveCategory.BOOLEAN).category

    # Binary Type
    parser = TypeParser("binary")
    ptype = parser.parse_type()
    assert ptype.name == HPrimitiveType(PrimitiveCategory.BINARY).name
    assert ptype.category == HPrimitiveType(PrimitiveCategory.BINARY).category

    # Timestamp Type
    parser = TypeParser("timestamp")
    ptype = parser.parse_type()
    assert ptype.name == HPrimitiveType(PrimitiveCategory.TIMESTAMP).name
    assert ptype.category == HPrimitiveType(PrimitiveCategory.TIMESTAMP).category

    # Date Type
    parser = TypeParser("date")
    ptype = parser.parse_type()
    assert ptype.name == HPrimitiveType(PrimitiveCategory.DATE).name
    assert ptype.category == HPrimitiveType(PrimitiveCategory.DATE).category


# Decimal Type
def test_decimal_type_parser():
    parser = TypeParser("decimal")
    ptype = parser.parse_type()

    # default precision and scale decimal
    assertd = HDecimalType(10, 0)
    assert isinstance(ptype, HDecimalType)
    assert ptype.precision == assertd.precision
    assert ptype.scale == assertd.scale

    # with precision defined only
    parser = TypeParser("decimal(20)")
    ptype = parser.parse_type()
    assertd = HDecimalType(20, 0)
    assert isinstance(ptype, HDecimalType)
    assert ptype.precision == assertd.precision
    assert ptype.scale == assertd.scale

    # with precision and scale defined
    parser = TypeParser("decimal(20,10)")
    ptype = parser.parse_type()
    assertd = HDecimalType(20, 10)
    assert isinstance(ptype, HDecimalType)
    assert ptype.precision == assertd.precision
    assert ptype.scale == assertd.scale

    # Test accepted boundaries for parameters
    parser = TypeParser("decimal(39,0)")
    with pytest.raises(ValueError, match="Decimal precision cannot exceed 38"):
        parser.parse_type()

    # Test accepted boundaries for parameters
    parser = TypeParser("decimal(37,40)")
    with pytest.raises(ValueError, match="Decimal scale cannot exceed 38"):
        parser.parse_type()

    # Test accepted boundaries for parameters
    parser = TypeParser("decimal(39,40)")
    with pytest.raises(ValueError, match="Decimal precision cannot exceed 38"):
        parser.parse_type()


# CHAR Type
def test_char_type_parser():
    parser = TypeParser("char(10)")
    ptype = parser.parse_type()
    assert isinstance(ptype, HCharType)
    assert ptype.length == 10

    # Testing char without length
    parser = TypeParser("char")
    with pytest.raises(
        ValueError, match="char/varchar type must have a length specified"
    ):
        parser.parse_type()

    # Testing char with more than 255 length
    parser = TypeParser("char(65536)")
    with pytest.raises(ValueError, match="Char length cannot exceed 255"):
        parser.parse_type()

    # Testing char with more than one parameter
    parser = TypeParser("char(10,20)")
    with pytest.raises(
        ValueError,
        match="Error: char type takes only one parameter, but instead 2 parameters are found.",
    ):
        parser.parse_type()

    # VARCHAR Type


def test_var_char_type_parser():
    parser = TypeParser("varchar(10)")
    ptype = parser.parse_type()
    assert isinstance(ptype, HVarcharType)
    assert ptype.length == 10

    # Testing varchar without length
    parser = TypeParser("varchar")
    with pytest.raises(
        ValueError, match="char/varchar type must have a length specified"
    ):
        parser.parse_type()

    # Testing varchar with more than 65535 length
    parser = TypeParser("varchar(65536)")
    with pytest.raises(ValueError, match="Varchar length cannot exceed 65535"):
        parser.parse_type()

    # Testing varchar with more than one parameter
    parser = TypeParser("varchar(10,20)")
    with pytest.raises(
        ValueError,
        match="Error: varchar type takes only one parameter, but instead 2 parameters are found.",
    ):
        parser.parse_type()


# ARRAY Type
def test_array_type_parser():
    parser = TypeParser("array<int>")
    ptype = parser.parse_type()
    assert isinstance(ptype, HListType)
    assert ptype.element_type.name == "INT"
    assert (
        ptype.element_type.category.PRIMITIVE
        == HPrimitiveType(PrimitiveCategory.INT).category
    )


def test_struct_type_parser():
    parser = TypeParser("struct<name:string,age:int>")
    ptype: HStructType = parser.parse_type()
    assert isinstance(ptype, HStructType)
    assert ptype.names == ["name", "age"]
    assert ptype.types[0].category == HPrimitiveType(PrimitiveCategory.STRING).category
    assert ptype.types[0].name == HPrimitiveType(PrimitiveCategory.STRING).name
    assert ptype.types[1].category == HPrimitiveType(PrimitiveCategory.INT).category
    assert ptype.types[1].name == HPrimitiveType(PrimitiveCategory.INT).name


def test_map_type_parser():
    parser = TypeParser("map<string,int>")
    ptype = parser.parse_type()
    assert isinstance(ptype, HMapType)
    assert ptype.key_type.name == "STRING"
    assert ptype.key_type.category == HPrimitiveType(PrimitiveCategory.STRING).category
    assert ptype.value_type.name == "INT"
    assert ptype.value_type.category == HPrimitiveType(PrimitiveCategory.INT).category


def test_union_type_parser():
    parser = TypeParser("uniontype<int,string>")
    ptype = parser.parse_type()
    assert isinstance(ptype, HUnionType)
    assert ptype.types[0].name == "INT"
    assert ptype.types[0].category == HPrimitiveType(PrimitiveCategory.INT).category
    assert ptype.types[1].name == "STRING"
    assert ptype.types[1].category == HPrimitiveType(PrimitiveCategory.STRING).category
