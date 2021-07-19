This package provides necessary infrastructure to create, read, convert to and from POJO classes
schema-defined rows.

### Schema definition

Schema is defined as a set of columns which are split into key columns chunk and value columns chunk.
Each column defined by a name, nullability flag, and a `org.apache.ignite.internal.schema.NativeType`.
Type is a thin wrapper over the `org.apache.ignite.internal.schema.NativeTypeSpec` to provide differentiation
between types of one kind with different size (an example of such differentiation is bitmask(n) or number(n)).
`org.apache.ignite.internal.schema.NativeTypeSpec` provides necessary indirection to read a column as a
`java.lang.Object` without needing to switch over the column type.

`NativeType` defines one of the following types: 

Type | Size | Description
---- | ---- | -----------
Bitmask(n)|⌈n/8⌉ bytes|A fixed-length bitmask of n bits
Int8|1 byte|1-byte signed integer
Uint8|1 byte|1-byte unsigned integer
Int16|2 bytes|2-byte signed integer
Uint16|2 bytes|2-byte unsigned integer
Int32|4 bytes|4-byte signed integer
Uint32|4 bytes|4-byte unsigned integer
Int64|8 bytes|8-byte signed integer
Uint64|8 bytes|8-byte unsigned integer
Float|4 bytes|4-byte floating-point number
Double|8 bytes|8-byte floating-point number
Number([n])|Variable|Variable-length number (optionally bound by n bytes in size)
Decimal|Variable|Variable-length floating-point number
UUID|16 bytes|UUID
String|Variable|A string encoded with a given Charset
Date|3 bytes|A timezone-free date encoded as a year (15 bits), month (4 bits), day (5 bits)
Time|4 bytes|A timezone-free time encoded as padding (5 bits), hour (5 bits), minute (6 bits), second (6 bits), millisecond (10 bits)
Datetime|7 bytes|A timezone-free datetime encoded as (date, time)
Timestamp|8 bytes|Number of milliseconds since Jan 1, 1970 00:00:00.000 (with no timezone)
Binary|Variable|Variable-size byte array

Arbitrary nested object serialization at this point is not supported, but can be provided in the future by either 
explicit inlining, or by providing an upper-level serialization primitive that will be mapped to a `Binary` column.

### Row layout
A row itself does not contain any type metadata and only contains necessary information required for fast column 
lookup. In a row, key columns and value columns are separated and written to chunks with identical structure 
(so that chunk is self-sufficient, and, provided with the column types can be read independently).

Row structure has the following format:

    ┌─────────────────────────────┬─────────────────────┐
    │           Header            │        Data         │
    ├─────────┬─────────┬─────────┼──────────┬──────────┤
    │ Schema  │ Flags   │ Key     │ Key      │ Value    │
    │ Version │         │ Hash    │ Chunk    │ Chunk    │
    ├─────────┼─────────┼─────────┼──────────┼──────────┤
    │ 2 Bytes │ 2 Bytes │ 4 Bytes │ Variable │ Variable │
    └─────────┴─────────┴─────────┴──────────┴──────────┘


Each chunk section has the following structure:

                                                                           ┌────────────────────────┐
                                                                           │                        │
    ┌─────────┬─────────────────────────┬─────────────────────────┬────────┴────────┬──────────┬────⌄─────┐
    │ Full    │ Null-Defaults           │ Varsize Columns Offsets │ Varsize Columns │ Fixsize  │ Varsize  │
    │ Size    │ Map                     │ Table Size              │ Offsets Table   │ Columns  │ Columns  │
    ├─────────┼─────────────────────────┼─────────────────────────┼─────────────────┼──────────┼──────────┤
    │ 4 Bytes │ ⌈Number of columns / 8⌉ │ 2 Bytes                 │ Variable        │ Variable │ Variable │
    └─────────┴─────────────────────────┴─────────────────────────┴─────────────────┴──────────┴──────────┘
All columns within a group are split into groups of fixed-size columns and variable-size columns. Withing the group of 
fixsize columns, the columns are sorted by size, then by column name. Within the group of varsize columns, the columns 
are sorted by column name. Inside a row default values and nulls are omitted and encoded in the null-defaults map 
(essentially, a bitset). The size of the varsize columns offsets table is equal to the number of non-null 
varsize columns multiplied by entry size. A single entry size in the offsets table may be 1-2-4 bytes depending on 
choosed format. The very first varlen offset is omited because it offset can be easily calculated using a null-map and 
fixed-size columns lengths. The offset stored in the offsets table is calculated from (the beginning of data section) 
the very first column value offset. **A row is always written in the most compact way.**

### Row construction and access
To assemble a row with some schema, an instance of `org.apache.ignite.internal.schema.row.RowAssembler`
must be used which provides the low-level API for building rows. When using the row assembler, the
columns must be passed to the assembler in the internal schema sort order. Additionally, when constructing
the instance of the assembler, the user should pre-calculate the size of the row to avoid extra array copies,
and the number of non-null varlen columns for key and value chunks. Less restrictive building techniques
are provided by class (de)serializers and tuple builder, which take care of sizing and column order.

To read column values of a row, one needs to construct a subclass of
`org.apache.ignite.internal.schema.row.Row` which provides necessary logic to read arbitrary columns with
type checking. For primitive types, `org.apache.ignite.internal.schema.row.Row` provides boxed and non-boxed
value methods to avoid boxing in scenarios where boxing can be avoided (deserialization of non-null columns to
POJO primitives, for example).
