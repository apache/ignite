# Mappers

In addition to binary views, which allows using a Tuple as key-value/record object, Ignite offers generic views, which allow using arbitrary
user classes as key-value/record types. These views require a custom mapping for binding objects or object`s fields with the table columns 
in one-to-one manner.

[Mapper API](Mapper.java) provides a number of shortcut methods for most simple cases, and a [MapperBuilder](MapperBuilder.java) 
for more complex ones. In normal use, there is no need to implement a custom Mapper.

Mapper API allows to map either a whole object to a one column, or an object fields to the columns.
In both cases, it is possible to specify [TypeConverter](TypeConverter.java) for converting data to/from a column compatible type.
If the object type doesn't match the column type, then converter for the column is mandatory, and supposed it is defined in the mapper 
by a user.

User objects are converted into binary form using mapper and converters on the client-side, and neither na user object, nor the mapper 
object itself, nor a converter object, nor their code are transferred among the nodes. This allows to use different classes/mappers and/or 
their versions on different nodes and provide better flexibility for maintaining user application on the top of Ignite.

In case of one column mapping, when key or value contains exactly one column, the column can be detected at runtime,
thus `Mapper.of(Class)` method without specifying exact column can be used for nativly supported types (e.g. Long, LocalDate, UUID and etc).
If a user provide POJO class, then Ignite will try to map object fields to columns with the same name. 
 
##Single column mapping.
* `Mapper.of(Class)` - maps natively supported type to a single available column. It is safe to use for keys. 
However, be carefull using it for values as if may become invalid once a new column will be added.
* `Mapper.of(Class, String)` - safer than above because column name is specified explicitly.
* `Mapper.of(Class, String , TypeConverter)` - same with type converter.

##Multi-column mapping.
* `Mapper.of(Class)` - when passed a type, which has no native support, tries to map object fields to the columns with same names. 
* `Mapper.of(Class, String, String,  String...)` - maps class fields to the columns regarding (field_name, column_name) pairs in vararg.  

##MapperBuilder approach.

* `Mapper.builder(Class)` - creates a mapper builder for the class.

[Mapper builder](MapperBuilder.java) class provides method for more complex cases, and allows to set converters for individual columns.
Builder object is not reusable.

* `MapperBuilder.map(String, String,  String...)` - maps class fields to the columns regarding (field_name, column_name) pairs in vararg.
* `MapperBuilder.map(String, TypeConverter)` - defines a converter for given column.
* `MapperBuilder.map(String, String, TypeConverter)` - maps the field to the column with type converter, is a shortcut for 
`MapperBuilder.map(field, column).converter(Converter.class, column)`
* `MapperBuilder.automap()` - maps object fields to the columns with the same names. This does not affect fields that were already mapped 
manually via one of the method above. Any field that doesn't match a column by name is ignored.
