# Ignite Network Annotation Processor module

This module provides a series of annotation processors for code generation related to the Network module.

This module provides the following annotations:

1. `@AutoSerializable` — annotation for marking `NetworkMessage` implementations. Such implementations will be taken
   into account by the annotation processor when generating corresponding `MessageSerializer`, `MessageDeserializer`
   and `MessageSerializationFactory` instances.
