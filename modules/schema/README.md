# Schema module

This module provides implementation for schema management components:

* Public API for schema definition and evolution
* Schema manager component that implements necessary machinery to translate schema management commands to corresponding
  metastorage modifications, as well as schema modification event processing logic 
* Necessary logic to build and upgrade rows of specific schema that encode user data in schema-defined format.

## Schema-aware tables
We require that at any moment in time an Ignite table has only one most recent relevant schema. Upon schema 
modification, we assign a monotonically growing identifier to each version of the cache schema. The ordering guarantees 
are provided by the underlying distributed metastorage. The history of schema versions must be kept in the metastorage 
for a long enough period of time to allow upgrade of all existing data stored in a given table.
              
Given a schema evolution history, a row migration from version `N-k` to version `N` is a straightforward operation. 
We identify fields that were dropped during the last k schema operations and fields that were added (taking into account
default field values) and update the row based on the field modifications. Afterward, the updated row is written in
the schema version `N` layout format. The row upgrade may happen on read with an optional writeback or on next update. 
Additionally, row upgrade in background is possible.
              
Since the row key hashcode is inlined to the row data for quick key lookups, we require that the set of key columns 
do not change during the schema evolution. In the future, we may remove this restriction, but this will require careful 
hashcode calculation adjustments. Removing a column from the key columns does not seem to be possible since it may 
produce duplicates, and we assume PK has no duplicates.
              
Additionally to adding and removing columns, it may be possible to allow for column type migrations when the type change 
is non-ambiguous (a type upcast, e.g. Int8 → Int16, or by means of a certain expression, e,g, Int8 → String using 
the `CAST` expression).
 
### Data Layout
We assume that there is exactly one valid binary representation for each key, thus binary keys representations can be 
safely compared instead of keys themselves avoiding unnecessary deserialization. To achieve that, key columns are fixed 
at a time of table created, and key columns can't be added or removed. All the key columns values must be provided 
for a table operation, to resolve unambiguity of 'null or absent' column.     

Data layout is documentation can be found [here](src/main/java/org/apache/ignite/internal/schema/README.md)

## Object-to-schema mapping

Mappers API provides two interfaces for two different cases: 
* [OneColumnMapper](../api/src/main/java/org/apache/ignite/table/mapper/OneColumnMapper.java) for the case, 
when a whole object is mapped to a one column. 
* [PojoMapper](../api/src/main/java/org/apache/ignite/table/mapper/OneColumnMapper.java) for the case, then object fields are
mapped to columns.

Mappers and Type converters designed as node specific stuff and never transferred among nodes.
All the machinery translating user object to an intermediate (binary) representation is applied on the client side. 
 
Mapper is used only for creating marshaller for user objects every time a schema has changed or Record/KeyValue view instance is created.
**For better performance, a marshaller code can be generated and/or marshaller instance can be cached and reused.
 
See [Mapper API](../api/src/main/java/org/apache/ignite/table/mapper/README.md) for details.