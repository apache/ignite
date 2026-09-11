# Message table

Exports registered message IDs, classes and fields to `table.xml`.
Fields, including inherited fields and CLASS-retained annotations, are read from
compiled classes through the JDK compiler API. No node or intermediate manifest is needed.
XML is written using the JDK DOM and Transformer APIs without an external XML library.
Each `field` contains separate `type` and `name` elements, plus an optional
`serialization` element for serialization annotations. The message-level
`jdkMarshalled` marker is a separate element.
Element position defines field order; field indexes and declaring classes are omitted.

Coverage: core, indexing, Calcite and ZooKeeper providers. Unregistered classes and
third-party providers are out of scope. For `CompressedMessage`, only registration
is included with an empty schema; its hand-written wire format is not checked.

This module only generates the table. It does not compare revisions, assess compatibility,
generate patches or publish CI warnings.

## Generate

Use JDK 17. Maven generates `target/table.xml` during `process-classes`, including
when running `test`, `package` or `install`. From the repository root, build the
required modules first if their current artifacts are not installed:

```sh
./mvnw -pl modules/compatibility-check -am clean install -DskipTests -Dmaven.javadoc.skip=true
```

With dependencies already installed, regenerate the table with:

```sh
./mvnw -pl modules/compatibility-check process-classes
```

The build only writes `modules/compatibility-check/target/table.xml`. To update the
checked-in table explicitly:

```sh
cp modules/compatibility-check/target/table.xml \
    modules/compatibility-check/src/main/resources/messages/table.xml
```
