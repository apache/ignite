# Message table

Exports registered Ignite messages to
[`src/main/resources/messages/table.xml`](src/main/resources/messages/table.xml) for rolling upgrade review.
The table contains message IDs, classes, ordered wire fields and serialization annotations.
Logical `@Marshalled` fields are written separately from ordered wire fields.

The table covers core, indexing, Calcite and ZooKeeper providers. Unregistered messages
and third-party providers are out of scope. `CompressedMessage` is included with an empty
schema because it has a hand-written serializer.

This module only generates the table. It does not compare revisions or decide whether a
change is compatible.

Example:

```xml
<message id="300" class="org.apache.ignite.internal.processors.query.calcite.message.QueryStartRequest">
  <orderedFields>
    <field order="0">
      <type>java.lang.String</type>
      <name>schema</name>
    </field>
    <field order="7">
      <type>byte[]</type>
      <name>paramsBytes</name>
    </field>
  </orderedFields>
  <marshalledFields>
    <field>
      <annotations>
        <org.apache.ignite.internal.Marshalled>value=paramsBytes</org.apache.ignite.internal.Marshalled>
      </annotations>
      <type>java.lang.Object[]</type>
      <name>params</name>
    </field>
  </marshalledFields>
</message>
```

## Generate

Use JDK 17. Enable the `message-table` profile to update
`modules/compatibility-check/src/main/resources/messages/table.xml` directly.
To generate only the message table, run:

```sh
./mvnw -pl modules/compatibility-check -Pmessage-table process-classes
```

Without the profile, the generation step does not run. Review the XML diff and
include it in the commit when message definitions change.

After generation, CI can check that the committed table is up to date with:

```sh
git diff --exit-code -- modules/compatibility-check/src/main/resources/messages/table.xml
```
