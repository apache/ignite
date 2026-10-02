# Message table

Exports registered Ignite messages to
[`src/main/resources/messages/table.xml`](src/main/resources/messages/table.xml) to control compatibility changes.
The table contains message IDs, classes, ordered wire fields and serialization annotations.
Logical `@Marshalled` fields are written separately from ordered wire fields.

The table covers messages registered by
[`CoreMessagesProvider`](../core/src/main/java/org/apache/ignite/internal/CoreMessagesProvider.java),
[`GridH2ValueMessageFactory`](../indexing/src/main/java/org/apache/ignite/internal/processors/query/h2/twostep/msg/GridH2ValueMessageFactory.java),
[`CalciteMessageFactory`](../calcite/src/main/java/org/apache/ignite/internal/processors/query/calcite/message/CalciteMessageFactory.java) and
[`ZkMessageFactory`](../zookeeper/src/main/java/org/apache/ignite/spi/discovery/zk/internal/ZkMessageFactory.java).
Unregistered messages and third-party providers are out of scope. `CompressedMessage` is included
with an empty schema because it has a hand-written serializer.

This module only generates the table. It does not compare revisions or decide whether a
change is compatible.

Example:

```xml
<message id="13013" class="org.apache.ignite.internal.processors.plugin.PluginsDataBagItem">
  <orderedFields>
    <field order="0">
      <type>byte[]</type>
      <name>dataBytes</name>
    </field>
  </orderedFields>
  <marshalledFields>
    <field>
      <annotations>
        <org.apache.ignite.internal.Marshalled>value=dataBytes</org.apache.ignite.internal.Marshalled>
      </annotations>
      <type>java.util.Map&lt;java.lang.String,java.io.Serializable&gt;</type>
      <name>data</name>
    </field>
  </marshalledFields>
</message>
```

## Generate

Use JDK 17. Enable the `message-table` profile to update the table directly:

```sh
./mvnw -pl modules/compatibility-check -Pmessage-table process-classes
```

Without the profile, the generation step does not run. Review the XML diff and
include it in the commit when message definitions change.

After generation, CI can check that the committed table is up to date with:

```sh
git diff --exit-code -- modules/compatibility-check/src/main/resources/messages/table.xml
```
