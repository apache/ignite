# Message Processor Module

This Maven module provides an annotation processor that automatically generates efficient serializer classes for all types implementing the `Message` interface. It is designed to run during the Java compilation phase and integrates seamlessly into build process.

## Purpose

The `MessageProcessor` performs the following tasks:

- **Code Generation**: For each class implementing the `Message` interface, a corresponding `*Serializer` class is generated. This serializer includes `writeTo` and `readFrom` methods that support stateful message encoding and decoding.
- **Annotation Validation**: The processor checks for the presence and correctness of the `@Order` annotation on fields, ensuring fields are serialized in a defined and sequential order.
- **Naming Convention Enforcement**: It verifies that all fields annotated with `@Order` have a corresponding pair of accessor methods:
    - A getter named exactly after the field (e.g., `fieldName()`)
    - A setter named after the field, accepting a single argument (e.g., `fieldName(Type val)`)

## Usage

1. **Prepare your Message class**

Content of the methods `writeTo` and `readFrom` can be generated with `MessageCodeGenerator` in `ignite-codegen` module.

```java
public class MyMessage implements Message {
    @Order(0)
    private int id;

    public int id() { return id; }

    public void id(int id) { this.id = id; }
  
    public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        return MyMessageSerializer.writeTo(this, buf, writer);
    }

    public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        return MyMessageSerializer.readFrom(this, buf, reader);
    }
}
```

2. **Generated Output**

At compile time, a class `MyMessageSerializer` within same package will be generated with methods:

- boolean writeTo(MyMessage msg, ByteBuffer buf, MessageWriter writer)
- boolean readFrom(MyMessage msg, ByteBuffer buf, MessageReader reader)

3. **Validation**

If the `@Order` values are not sequential starting from 0, or if getter/setter method names do not match the field name, compilation will fail with a meaningful error message pointing to the problematic element.

## **Maven codegen modules**

To avoid a circular dependency between `ignite-core` (which contains message classes) and `ignite-codegen` (which needs to know about them), the annotation processor is **temporarily placed in a separate module**. This separation allows the processor to remain independent of core classes during compilation.

In the future, this structure may be consolidated when build system constraints are resolved.

