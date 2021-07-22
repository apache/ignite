# Schema module

This module provides implementation for schema management components:

* Public API for schema definition and evolution
* Schema manager component that implements necessary machinary to translate schema management commands to corresponding
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

### Dynamic schema expansion (live schema)
Ignite can operate in two modes that provide different flexibility level and restrictions wrt object-to-schema mapping:
 * Strict mode. When a user attempts to insert/update an object to a table, Ignite checks that the object does not 
 contain any extra columns that are not present in the current table schema. If such columns are detected, Ignite will
 fail the operation requiring the user to manually update the schema before working with added columns.     
 * Live mode. When an object is inserted into a table, we attempt to 'fit' object fields to the schema columns. If the 
 object has some extra fields which are not present in the current schema, the schema is automatically updated to store 
 additional extra fields that are present in the object. If there are two concurrent live schema modifications, they can 
 either merge together if modifications are non-conflicting (e.g. adding disjoint sets of columns or adding columns with
 the same definition), or one of the modifications will fail (e.g. two columns with the same name, but conflicting type
 are being inserted). Live schema will try to automatically expand the schema even if there was an explicit drop column
 command executed right before the live schema expansion. **Live schema never drops columns during automatic schema 
 evolution.** If a schema has columns that were not fulfilled by object fields, they will be either kept `null` or 
 populated with defaults when provided, or the update will fail with an exception.
 
### Data Layout
Data layout is documentation can be found [here](src/main/java/org/apache/ignite/internal/schema/README.md)

## Object-to-schema mapping
