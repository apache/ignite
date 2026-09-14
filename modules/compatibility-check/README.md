# Message table

Exports registered message IDs, classes and fields to `table.xml`.
Fields, including inherited fields and CLASS-retained annotations, are read from
compiled classes through the JDK compiler API. No node or intermediate manifest is needed.
XML is written using the JDK StAX API without an external XML library.
Each `field` contains separate `type` and `name` elements, plus an optional
`annotations` element containing serialization annotations. Ordered wire fields
also have an `order` attribute with their generated serializer state. Marker
annotations use empty elements; annotations with values contain text. Annotation names are
sorted for stable output. Annotation element names are fully qualified class names. Message-level annotations use the same `annotations` structure directly inside
`message`.
Element position follows field order; declaring classes are omitted.

Coverage: core, indexing, Calcite and ZooKeeper providers. Unregistered classes and
third-party providers are out of scope. For `CompressedMessage`, only registration
is included with an empty schema; its hand-written wire format is not checked.

This module only generates the table. It does not compare revisions, assess compatibility
or generate patches.

## Generate

Use JDK 17. Enable the `message-table` profile to update
`modules/compatibility-check/src/main/resources/messages/table.xml` directly.
Generation runs during `process-classes`, so the profile can be added to the
regular build command:

```sh
./mvnw test-compile -Pall-java,licenses,lgpl,checkstyle,examples,check-licenses,message-table -B -V -T 1C
```

Without the profile, the generation step does not run. Review the XML diff and
include it in the commit when message definitions change.

After generation, CI can check that the committed table is up to date with:

```sh
git diff --exit-code -- modules/compatibility-check/src/main/resources/messages/table.xml
```
