# Code generation module

This Maven module provides annotation processors that automatically generate code, such as serializers, walkers, etc. It is designed to run during the Java compilation phase and integrates seamlessly into the build process.

## Message Processor

This annotation processor automatically generates efficient serializer classes for all types implementing the `Message` interface. 

### Purpose

The `MessageProcessor` performs the following tasks:

- **Code Generation**: For each class implementing the `Message` interface, a corresponding `*Serializer` class is generated. This serializer includes `writeTo` and `readFrom` methods that support stateful message encoding and decoding.
- **Annotation Validation**: The processor checks for the presence and correctness of the `@Order` annotation on fields, ensuring fields are serialized in a defined and sequential order.
- **Naming Convention Enforcement**: It verifies that all fields annotated with `@Order` have a corresponding pair of accessor methods:
    - A getter named exactly after the field (e.g., `fieldName()`)
    - A setter named after the field, accepting a single argument (e.g., `fieldName(Type val)`)
-  **Access Modifiers**: Getters and setters for all fields annotated with `@Order` must be declared as `public`

### Usage

1. **Prepare your Message class**

```java
public class MyMessage implements Message {
    @Order(0)
    private int id;

    public int id() { return id; }

    public void id(int id) { this.id = id; }
}
```

2. **Generated Output**

At compile time, a class `MyMessageSerializer` implemented `MessageSerializer` will be generated with methods:

- boolean writeTo(Message msg, ByteBuffer buf, MessageWriter writer)
- boolean readFrom(Message msg, ByteBuffer buf, MessageReader reader)

3. **Validation**

If the `@Order` values are not sequential starting from 0, or if getter/setter method names do not match the field name, compilation will fail with a meaningful error message pointing to the problematic element.
