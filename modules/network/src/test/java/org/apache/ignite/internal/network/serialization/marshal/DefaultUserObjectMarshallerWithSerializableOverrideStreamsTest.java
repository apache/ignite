/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.network.serialization.marshal;

import static org.apache.ignite.internal.network.serialization.marshal.Throwables.causalChain;
import static org.apache.ignite.internal.network.serialization.marshal.Throwables.rootCauseOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamField;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for how {@link DefaultUserObjectMarshaller} handles {@link ObjectOutputStream}/{@link ObjectInputStream} specifics
 * when working with writeObject()/readObject() of {@link Serializable}s.
 */
class DefaultUserObjectMarshallerWithSerializableOverrideStreamsTest {
    private final ClassDescriptorRegistry descriptorRegistry = new ClassDescriptorRegistry();
    private final ClassDescriptorFactory descriptorFactory = new ClassDescriptorFactory(descriptorRegistry);

    private final DefaultUserObjectMarshaller marshaller = new DefaultUserObjectMarshaller(descriptorRegistry, descriptorFactory);

    /** Reader+writer is static so that writeObject()/readObject() can easily find it. */
    private static ReaderAndWriter<?> readerAndWriter;

    /** Static access to the marshaller (for using in parameterized tests). */
    private static UserObjectMarshaller staticMarshaller;
    /** Static access to the registry (for using in parameterized tests). */
    private static ClassDescriptorRegistry staticDescriptorRegistry;

    /** Putter is static so that writeObject() can easily find it. */
    private static FieldPutter fieldPutter;
    /** Filler is static so that readObject() can easily find it. */
    private static FieldFiller fieldFiller;

    @BeforeEach
    void initStatics() {
        staticMarshaller = marshaller;
        staticDescriptorRegistry = descriptorRegistry;
    }

    private <T> T marshalAndUnmarshalNonNull(Object object) throws MarshalException, UnmarshalException {
        MarshalledObject marshalled = marshaller.marshal(object);
        return unmarshalNonNull(marshalled);
    }

    private <T> T unmarshalNonNull(MarshalledObject marshalled) throws UnmarshalException {
        T unmarshalled = marshaller.unmarshal(marshalled.bytes(), descriptorRegistry);

        assertThat(unmarshalled, is(notNullValue()));

        return unmarshalled;
    }

    @ParameterizedTest
    @MethodSource("readWriteSpecs")
    <T> void supportsReadsAndWritesInWriteObjectAndReadObject(ReadWriteSpec<T> spec) throws Exception {
        readerAndWriter = new ReaderAndWriter<>(spec.writer, spec.reader);

        WithCustomizableOverride<T> original = new WithCustomizableOverride<>();
        WithCustomizableOverride<T> unmarshalled = marshalAndUnmarshalNonNull(original);

        spec.assertUnmarshalledValue(unmarshalled.value);
    }

    @ParameterizedTest
    @MethodSource("readWriteSpecs")
    <T> void objectStreamsFromReadWriteObjectReadsWritesUsingOurFormat(ReadWriteSpec<T> spec) throws Exception {
        readerAndWriter = new ReaderAndWriter<>(spec.writer, spec.reader);

        WithCustomizableOverride<T> original = new WithCustomizableOverride<>();
        MarshalledObject marshalled = marshaller.marshal(original);

        byte[] overrideBytes = readOverrideBytes(marshalled);
        T overrideValue = spec.parseOverrideValue(overrideBytes);

        spec.assertUnmarshalledValue(overrideValue);
    }

    private byte[] readOverrideBytes(MarshalledObject marshalled) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(marshalled.bytes()));

        ProtocolMarshalling.readDescriptorOrCommandId(dis);
        ProtocolMarshalling.readObjectId(dis);

        return dis.readAllBytes();
    }

    private static Stream<Arguments> readWriteSpecs() {
        return Stream.of(
                // the following test perfectly-matching pairs like writeByte()/readByte()
                new ReadWriteSpec<>("data byte", oos -> oos.writeByte(42), DataInput::readByte, (byte) 42),
                new ReadWriteSpec<>("short", oos -> oos.writeShort(42), DataInput::readShort, (short) 42),
                new ReadWriteSpec<>("int", oos -> oos.writeInt(42), DataInput::readInt, 42),
                new ReadWriteSpec<>("long", oos -> oos.writeLong(42), DataInput::readLong, 42L),
                new ReadWriteSpec<>("float", oos -> oos.writeFloat(42.0f), DataInput::readFloat, 42.0f),
                new ReadWriteSpec<>("double", oos -> oos.writeDouble(42.0), DataInput::readDouble, 42.0),
                new ReadWriteSpec<>("char", oos -> oos.writeChar('a'), DataInput::readChar, 'a'),
                new ReadWriteSpec<>("boolean", oos -> oos.writeBoolean(true), DataInput::readBoolean, true),
                new ReadWriteSpec<>("stream byte", oos -> oos.write(42), ObjectInputStream::read, DataInputStream::read, 42),
                new ReadWriteSpec<>(
                        "byte array",
                        oos -> oos.write(new byte[]{42, 43}),
                        is -> readBytes(is, 2),
                        is -> readBytes(is, 2),
                        new byte[]{42, 43}
                ),
                new ReadWriteSpec<>(
                        "byte array range",
                        oos -> oos.write(new byte[]{42, 43}, 0, 2),
                        is -> readRange(is, 2),
                        is -> readRange(is, 2),
                        new byte[]{42, 43}
                ),
                new ReadWriteSpec<>("UTF", oos -> oos.writeUTF("Привет"), DataInput::readUTF, "Привет"),
                new ReadWriteSpec<>(
                        "object",
                        oos -> oos.writeObject(new SimpleNonSerializable(42)),
                        ObjectInputStream::readObject,
                        DefaultUserObjectMarshallerWithSerializableOverrideStreamsTest::consumeAndUnmarshal,
                        new SimpleNonSerializable(42)
                ),
                new ReadWriteSpec<>(
                        "unshared",
                        oos -> oos.writeUnshared(new SimpleNonSerializable(42)),
                        ObjectInputStream::readUnshared,
                        DefaultUserObjectMarshallerWithSerializableOverrideStreamsTest::consumeAndUnmarshal,
                        new SimpleNonSerializable(42)
                ),

                // the following test writing methods only (readers are just to help testing them)
                new ReadWriteSpec<>("writeBytes", oos -> oos.writeBytes("abc"), input -> readBytesFully(input, 3), "abc".getBytes()),
                new ReadWriteSpec<>("writeChars", oos -> oos.writeChars("a"), DataInput::readChar, 'a'),

                // the following test reading methods only (writers are just to help testing them)
                new ReadWriteSpec<>(
                        "readFully",
                        oos -> oos.write(new byte[]{42, 43}),
                        input -> readBytesFully(input, 2),
                        new byte[]{42, 43}
                ),
                new ReadWriteSpec<>(
                        "readFully range",
                        oos -> oos.write(new byte[]{42, 43}),
                        input -> readBytesRangeFully(input, 2),
                        new byte[]{42, 43}
                ),
                new ReadWriteSpec<>("readUnsignedByte", oos -> oos.writeByte(42), DataInput::readUnsignedByte, 42),
                new ReadWriteSpec<>("readUnsignedShort", oos -> oos.writeShort(42), DataInput::readUnsignedShort, 42),
                new ReadWriteSpec<>(
                        "readAllBytes",
                        oos -> oos.write(new byte[]{42, 43}),
                        InputStream::readAllBytes,
                        InputStream::readAllBytes,
                        new byte[]{42, 43}
                ),
                new ReadWriteSpec<>(
                        "readNBytes",
                        oos -> oos.write(new byte[]{42, 43}),
                        ois -> ois.readNBytes(2),
                        dis -> readBytesFully(dis, 2),
                        new byte[]{42, 43}
                ),
                new ReadWriteSpec<>(
                        "readNBytes range",
                        oos -> oos.write(new byte[]{42, 43}),
                        ois -> readNumBytesRange(ois, 2),
                        dis -> readNumBytesRange(dis, 2),
                        new byte[]{42, 43}
                )
        ).map(Arguments::of);
    }

    @SuppressWarnings("SameParameterValue")
    private static byte[] readBytes(InputStream is, int count) throws IOException {
        byte[] bytes = new byte[count];
        int read = is.read(bytes);
        assertThat(read, is(count));
        return bytes;
    }

    @SuppressWarnings("SameParameterValue")
    private static byte[] readRange(InputStream is, int count) throws IOException {
        byte[] bytes = new byte[count];
        int read = is.read(bytes, 0, count);
        assertThat(read, is(count));
        return bytes;
    }

    private static byte[] readBytesFully(DataInput is, int count) throws IOException {
        byte[] bytes = new byte[count];
        is.readFully(bytes);
        return bytes;
    }

    @SuppressWarnings("SameParameterValue")
    private static byte[] readBytesRangeFully(DataInput is, int count) throws IOException {
        byte[] bytes = new byte[count];
        is.readFully(bytes, 0, count);
        return bytes;
    }

    @SuppressWarnings("SameParameterValue")
    private static byte[] readNumBytesRange(InputStream is, int count) throws IOException {
        byte[] bytes = new byte[count];
        is.readNBytes(bytes, 0, count);
        return bytes;
    }

    private static <T> T consumeAndUnmarshal(DataInputStream stream) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        stream.transferTo(baos);

        try {
            return staticMarshaller.unmarshal(baos.toByteArray(), staticDescriptorRegistry);
        } catch (UnmarshalException e) {
            throw new RuntimeException("Unmarshalling failed", e);
        }
    }

    @Test
    void putFieldsWritesAllPrimitiveTypesAndObjectsSoThatDefaultUnmarshallingReadsThem() throws Exception {
        fieldPutter = putField -> {
            putField.put("byteVal", (byte) 101);
            putField.put("shortVal", (short) 102);
            putField.put("intVal", 103);
            putField.put("longVal", (long) 104);
            putField.put("floatVal", (float) 105);
            putField.put("doubleVal", (double) 106);
            putField.put("charVal", 'z');
            putField.put("booleanVal", true);
            putField.put("objectVal", new IntHolder(142));
        };

        WithPutFields unmarshalled = marshalAndUnmarshalNonNull(new WithPutFields());

        assertThat(unmarshalled.byteVal, is((byte) 101));
        assertThat(unmarshalled.shortVal, is((short) 102));
        assertThat(unmarshalled.intVal, is(103));
        assertThat(unmarshalled.longVal, is((long) 104));
        assertThat(unmarshalled.floatVal, is((float) 105));
        assertThat(unmarshalled.doubleVal, is((double) 106));
        assertThat(unmarshalled.charVal, is('z'));
        assertThat(unmarshalled.booleanVal, is(true));
        assertThat(unmarshalled.objectVal, is(new IntHolder(142)));
    }

    @Test
    void putFieldsWritesDefaultValuesForFieldsNotSpecifiedExplicitly() throws Exception {
        fieldPutter = putField -> {
            // do not put anything -> defaults should be written
        };

        WithPutFields unmarshalled = marshalAndUnmarshalNonNull(new WithPutFields());

        assertThat(unmarshalled.byteVal, is((byte) 0));
        assertThat(unmarshalled.shortVal, is((short) 0));
        assertThat(unmarshalled.intVal, is(0));
        assertThat(unmarshalled.longVal, is((long) 0));
        assertThat(unmarshalled.floatVal, is((float) 0));
        assertThat(unmarshalled.doubleVal, is((double) 0));
        assertThat(unmarshalled.charVal, is('\0'));
        assertThat(unmarshalled.booleanVal, is(false));
        assertThat(unmarshalled.objectVal, is(nullValue()));
    }

    @Test
    void putFieldsThrowsForAnUnknownFieldAccess() {
        fieldPutter = putField -> putField.put("no-such-field", 1);

        Exception exception = assertThrows(Exception.class, () -> marshalAndUnmarshalNonNull(new WithPutFields()));
        assertThat(causalChain(exception), hasItem(hasProperty("message", equalTo("Did not find a field with name no-such-field"))));
    }

    @Test
    void readFieldsReadsAllPrimitiveTypesAndObjectsWrittenWithDefaultMarshalling() throws Exception {
        WithReadFields original = new WithReadFields();
        original.byteVal = (byte) 101;
        original.shortVal = (short) 102;
        original.intVal = 103;
        original.longVal = 104;
        original.floatVal = (float) 105;
        original.doubleVal = 106;
        original.charVal = 'z';
        original.booleanVal = true;
        original.objectVal = new IntHolder(142);

        fieldFiller = (getField, target) -> {
            target.byteVal = getField.get("byteVal", (byte) 201);
            target.shortVal = getField.get("shortVal", (short) 202);
            target.intVal = getField.get("intVal", 203);
            target.longVal = getField.get("longVal", (long) 204);
            target.floatVal = getField.get("floatVal", (float) 205);
            target.doubleVal = getField.get("doubleVal", (double) 206);
            target.charVal = getField.get("charVal", '!');
            target.booleanVal = getField.get("booleanVal", false);
            target.objectVal = getField.get("objectVal", new IntHolder(242));
        };

        WithReadFields unmarshalled = marshalAndUnmarshalNonNull(original);

        assertThat(unmarshalled.byteVal, is((byte) 101));
        assertThat(unmarshalled.shortVal, is((short) 102));
        assertThat(unmarshalled.intVal, is(103));
        assertThat(unmarshalled.longVal, is((long) 104));
        assertThat(unmarshalled.floatVal, is((float) 105));
        assertThat(unmarshalled.doubleVal, is((double) 106));
        assertThat(unmarshalled.charVal, is('z'));
        assertThat(unmarshalled.booleanVal, is(true));
        assertThat(unmarshalled.objectVal, is(new IntHolder(142)));
    }

    @Test
    void whenReadFieldsIsUsedTheDefaultMechanismFillsNoFieldsItself() throws Exception {
        fieldFiller = (getField, target) -> {
            // do not fill anything, so default values must remain
        };

        WithReadFields unmarshalled = marshalAndUnmarshalNonNull(new WithReadFields());

        assertThat(unmarshalled.byteVal, is((byte) 0));
        assertThat(unmarshalled.shortVal, is((short) 0));
        assertThat(unmarshalled.intVal, is(0));
        assertThat(unmarshalled.longVal, is((long) 0));
        assertThat(unmarshalled.floatVal, is((float) 0));
        assertThat(unmarshalled.doubleVal, is((double) 0));
        assertThat(unmarshalled.charVal, is('\0'));
        assertThat(unmarshalled.booleanVal, is(false));
        assertThat(unmarshalled.objectVal, is(nullValue()));
    }

    @Test
    void getFieldsThrowsForAnUnknownFieldAccess() {
        fieldFiller = (getField, target) -> getField.get("no-such-field", 1);

        Exception exception = assertThrows(Exception.class, () -> marshalAndUnmarshalNonNull(new WithReadFields()));
        assertThat(causalChain(exception), hasItem(hasProperty("message", equalTo("Did not find a field with name no-such-field"))));
    }

    @Test
    void getFieldAlwaysReturnsFalseForDefaulted() {
        // TODO: IGNITE-15948 - test that defaulted() works as intended when it's ready

        fieldFiller = (getField, target) -> {
            assertFalse(getField.defaulted("byteVal"));
        };

        assertDoesNotThrow(() -> marshalAndUnmarshalNonNull(new WithReadFields()));
    }

    @Test
    void nestedPutReadFieldsAreSupported() throws Exception {
        NestHostWithPutGetFields unmarshalled = marshalAndUnmarshalNonNull(new NestHostWithPutGetFields());

        assertThat(unmarshalled.intValue, is(2));
        assertThat(unmarshalled.objectValue, is(12));
        assertThat(unmarshalled.nested.intValue, is(1));
        assertThat(unmarshalled.nested.objectValue, is(11));
    }

    @Test
    void readFieldsObjectStreamClassIsAccessible() {
        fieldFiller = (getField, target) -> {
            assertThat(getField.getObjectStreamClass().getName(), is(equalTo(WithReadFields.class.getName())));
        };

        assertDoesNotThrow(() -> marshalAndUnmarshalNonNull(new WithReadFields()));
    }

    @SuppressWarnings("unchecked")
    @Test
    void writeUnsharedWritesNewCopyOfObjectEachTime() throws Exception {
        readerAndWriter = new ReaderAndWriter<>(
                oos -> {
                    var object = new IntHolder(42);
                    oos.writeUnshared(object);
                    oos.writeUnshared(object);
                },
                ois -> {
                    Object o1 = ois.readObject();
                    Object o2 = ois.readObject();
                    return List.of(o1, o2);
                }
        );

        var result = (WithCustomizableOverride<List<IntHolder>>) marshalAndUnmarshalWithCustomizableOverride();

        assertThat(result.value.get(0), is(not(sameInstance(result.value.get(1)))));
    }

    @SuppressWarnings("unchecked")
    @Test
    void noBackReferenceIsGeneratedToObjectWrittenWithWriteUnshared() throws Exception {
        readerAndWriter = new ReaderAndWriter<>(
                oos -> {
                    var object = new IntHolder(42);
                    oos.writeUnshared(object);
                    oos.writeObject(object);
                },
                ois -> {
                    Object o1 = ois.readObject();
                    Object o2 = ois.readObject();
                    return List.of(o1, o2);
                }
        );

        var result = (WithCustomizableOverride<List<IntHolder>>) marshalAndUnmarshalWithCustomizableOverride();

        assertThat(result.value.get(0), is(not(sameInstance(result.value.get(1)))));
    }

    @Test
    void readUnsharedThrowsAnExceptionIfTheObjectWasAlreadyReadEarlier() {
        readerAndWriter = new ReaderAndWriter<>(
                oos -> {
                    var object = new IntHolder(42);
                    oos.writeObject(object);
                    oos.writeObject(object);
                },
                ois -> {
                    Object o1 = ois.readObject();
                    Object o2 = ois.readUnshared();
                    return List.of(o1, o2);
                }
        );

        Throwable ex = assertThrows(UnmarshalException.class, this::marshalAndUnmarshalWithCustomizableOverride);
        assertThat(rootCauseOf(ex), is(instanceOf(InvalidObjectException.class)));
        assertThat(rootCauseOf(ex).getMessage(), is("cannot read back reference as unshared"));
    }

    @Test
    void readUnsharedMakesCorrespondingObjectIdInvalidForReadingWithReadObjectLater() {
        readerAndWriter = new ReaderAndWriter<>(
                oos -> {
                    var object = new IntHolder(42);
                    oos.writeObject(object);
                    oos.writeObject(object);
                },
                ois -> {
                    Object o1 = ois.readUnshared();
                    Object o2 = ois.readObject();
                    return List.of(o1, o2);
                }
        );

        Throwable ex = assertThrows(UnmarshalException.class, this::marshalAndUnmarshalWithCustomizableOverride);
        assertThat(rootCauseOf(ex), is(instanceOf(InvalidObjectException.class)));
        assertThat(rootCauseOf(ex).getMessage(), is("cannot read back reference to unshared object"));
    }

    @Test
    void putFieldsSupportsUnsharedFields() throws Exception {
        WithPutFieldsAndUnsharedFields unmarshalled = marshalAndUnmarshalNonNull(new WithPutFieldsAndUnsharedFields());

        assertThat(unmarshalled.value1, is(not(sameInstance(unmarshalled.value2))));
    }

    @Test
    void readFieldsSupportsUnsharedFields() {
        Throwable ex = assertThrows(UnmarshalException.class, () -> marshalAndUnmarshalNonNull(new WithReadFieldsAndReadUnsharedFields()));
        assertThat(rootCauseOf(ex), is(instanceOf(InvalidObjectException.class)));
        assertThat(rootCauseOf(ex).getMessage(), is("cannot read back reference as unshared"));
    }

    @Test
    void supportsFlushInsideWriteObject() {
        readerAndWriter = new ReaderAndWriter<>(ObjectOutputStream::flush, ois -> null);

        assertDoesNotThrow(this::marshalAndUnmarshalWithCustomizableOverride);
    }

    private WithCustomizableOverride<?> marshalAndUnmarshalWithCustomizableOverride() throws MarshalException, UnmarshalException {
        return marshalAndUnmarshalNonNull(new WithCustomizableOverride<>());
    }

    @Test
    void resetErrorsOutInsideWriteObject() {
        readerAndWriter = new ReaderAndWriter<>(ObjectOutputStream::reset, ois -> null);

        assertThrows(MarshalException.class, this::marshalAndUnmarshalWithCustomizableOverride);
    }

    @Test
    void supportsUseProtocolVersionInsideWriteObject() {
        readerAndWriter = new ReaderAndWriter<>(oos -> oos.useProtocolVersion(1), ois -> null);

        assertDoesNotThrow(this::marshalAndUnmarshalWithCustomizableOverride);
    }

    @Test
    void supportsSkipInsideReadObject() throws Exception {
        readerAndWriter = new ReaderAndWriter<>(oos -> oos.write(new byte[]{42, 43}), ois -> {
            assertThat(ois.skip(1), is(1L));
            return ois.readByte();
        });

        WithCustomizableOverride<Byte> unmarshalled = this.marshalAndUnmarshalNonNull(new WithCustomizableOverride<>());
        assertThat(unmarshalled.value, is((byte) 43));
    }

    @Test
    void supportsSkipBytesInsideReadObject() throws Exception {
        readerAndWriter = new ReaderAndWriter<>(oos -> oos.write(new byte[]{42, 43}), ois -> {
            assertThat(ois.skipBytes(1), is(1));
            return ois.readByte();
        });

        WithCustomizableOverride<Byte> unmarshalled = marshalAndUnmarshalNonNull(new WithCustomizableOverride<>());
        assertThat(unmarshalled.value, is((byte) 43));
    }

    @Test
    void supportsAvailableInsideReadObject() {
        readerAndWriter = new ReaderAndWriter<>(oos -> {}, ObjectInputStream::available);

        assertDoesNotThrow(this::marshalAndUnmarshalWithCustomizableOverride);
    }

    @Test
    void supportsMarkAndResetInsideReadObject() {
        readerAndWriter = new ReaderAndWriter<>(oos -> {}, ois -> {
            assertFalse(ois.markSupported());
            ois.mark(1);
            try {
                ois.reset();
            } catch (IOException e) {
                // ignore mark/reset not supported
            }
            return null;
        });

        assertDoesNotThrow(() -> marshalAndUnmarshalNonNull(new WithCustomizableOverride<>()));
    }

    @Test
    void defaultWriteObjectFromWriteObjectWritesUsingOurFormat() throws Exception {
        WithOverrideStillUsingDefaultMechanism original = new WithOverrideStillUsingDefaultMechanism(42);
        MarshalledObject marshalled = marshaller.marshal(original);

        byte[] overrideBytes = readOverrideBytes(marshalled);
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(overrideBytes));

        assertThat(dis.readInt(), is(42));
        assertThatDrained(dis);
    }

    private void assertThatDrained(DataInputStream dis) throws IOException {
        assertThat("Stream is not drained", dis.read(), is(lessThan(0)));
    }

    @Test
    void marshalsAndUnmarshalsSerializableWithReadWriteObjectUsingDefaultMechanism() throws Exception {
        WithOverrideStillUsingDefaultMechanism result = marshalAndUnmarshalNonNull(new WithOverrideStillUsingDefaultMechanism(42));

        assertThat(result.value, is(42));
    }

    @Test
    void marshalsAndUnmarshalsNestedSerializablesWithReadWriteObjectUsingDefaultMechanism() throws Exception {
        NestHostUsingDefaultMechanism result = marshalAndUnmarshalNonNull(
                new NestHostUsingDefaultMechanism(42, new NestedUsingDefaultMechanism(100))
        );

        assertThat(result.value, is(42));
        assertThat(result.nested.value, is(100));
    }

    @Test
    void passesUnmarshalExceptionTriggeredInsideReadObjectToTheCaller() throws Exception {
        readerAndWriter = new ReaderAndWriter<>(oos -> oos.writeObject(new ThrowingFromReadObject()), ObjectInputStream::readObject);

        MarshalledObject marshalled = marshaller.marshal(new WithCustomizableOverride<>());

        assertThrows(UnmarshalException.class, () -> unmarshalNonNull(marshalled));
    }

    private interface ObjectWriter {
        void writeTo(ObjectOutputStream stream) throws IOException;
    }

    private interface ObjectReader<T> {
        T readFrom(ObjectInputStream stream) throws IOException, ClassNotFoundException;
    }

    private interface DataReader<T> {
        T readFrom(DataInputStream stream) throws IOException;
    }

    private interface InputReader<T> {
        T readFrom(DataInput input) throws IOException;
    }

    private static class ReaderAndWriter<T> {
        private final ObjectWriter writer;
        private final ObjectReader<T> reader;

        private ReaderAndWriter(ObjectWriter writer, ObjectReader<T> reader) {
            this.writer = writer;
            this.reader = reader;
        }
    }

    private static class ReadWriteSpec<T> {
        private final String name;
        private final ObjectWriter writer;
        private final ObjectReader<T> reader;
        private final DataReader<T> dataReader;
        private final T expectedValue;

        private ReadWriteSpec(String name, ObjectWriter writer, InputReader<T> inputReader, T expectedValue) {
            this(name, writer, inputReader::readFrom, inputReader::readFrom, expectedValue);
        }

        private ReadWriteSpec(String name, ObjectWriter writer, ObjectReader<T> reader, DataReader<T> dataReader, T expectedValue) {
            this.name = name;
            this.writer = writer;
            this.reader = reader;
            this.dataReader = dataReader;
            this.expectedValue = expectedValue;
        }

        private T parseOverrideValue(byte[] overrideBytes) throws IOException {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(overrideBytes));
            return dataReader.readFrom(dis);
        }

        private void assertUnmarshalledValue(T value) {
            assertThat(value, is(equalTo(expectedValue)));
        }

        /** {@inheritDoc} */
        @Override
        public String toString() {
            return "ReadWriteSpec (" + name + ")";
        }
    }

    private static class WithCustomizableOverride<T> implements Serializable {
        private T value;

        private void writeObject(ObjectOutputStream oos) throws IOException {
            readerAndWriter.writer.writeTo(oos);
        }

        @SuppressWarnings("unchecked")
        private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
            value = (T) readerAndWriter.reader.readFrom(ois);
        }
    }

    private interface FieldPutter {
        void putWith(ObjectOutputStream.PutField putField);
    }

    @SuppressWarnings("FieldMayBeFinal")
    private static class WithPutFields implements Serializable {
        private byte byteVal = 1;
        private short shortVal = 2;
        private int intVal = 3;
        private long longVal = 4;
        private float floatVal = 5;
        private double doubleVal = 6;
        private char charVal = 'a';
        private boolean booleanVal = true;
        private Object objectVal = new IntHolder(42);

        private void writeObject(ObjectOutputStream oos) throws IOException {
            ObjectOutputStream.PutField putField = oos.putFields();
            fieldPutter.putWith(putField);
            oos.writeFields();
        }

        private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
            ois.defaultReadObject();
        }
    }

    private interface FieldFiller {
        void fillWith(ObjectInputStream.GetField getField, WithReadFields target) throws IOException;
    }

    private static class WithReadFields implements Serializable {
        private byte byteVal = 1;
        private short shortVal = 2;
        private int intVal = 3;
        private long longVal = 4;
        private float floatVal = 5;
        private double doubleVal = 6;
        private char charVal = 'a';
        private boolean booleanVal = true;
        private Object objectVal = new IntHolder(42);

        private void writeObject(ObjectOutputStream oos) throws IOException {
            oos.defaultWriteObject();
        }

        private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
            ObjectInputStream.GetField getField = ois.readFields();
            fieldFiller.fillWith(getField, this);
        }
    }

    private static class NestedWithPutFields implements Serializable {
        private int intValue = 1;
        private Object objectValue = 11;

        private void writeObject(ObjectOutputStream stream) throws IOException {
            ObjectOutputStream.PutField putField = stream.putFields();

            putField.put("intValue", intValue);
            putField.put("objectValue", objectValue);

            stream.writeFields();
        }

        private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
            ObjectInputStream.GetField getField = stream.readFields();

            intValue = getField.get("intValue", -1);
            objectValue = getField.get("objectValue", null);
        }
    }

    private static class NestHostWithPutGetFields implements Serializable {
        private int intValue = 2;
        private Object objectValue = 12;
        private NestedWithPutFields nested = new NestedWithPutFields();

        private void writeObject(ObjectOutputStream stream) throws IOException {
            ObjectOutputStream.PutField putField = stream.putFields();

            putField.put("intValue", intValue);
            putField.put("objectValue", objectValue);
            putField.put("nested", nested);

            stream.writeFields();
        }

        private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
            ObjectInputStream.GetField getField = stream.readFields();

            intValue = getField.get("intValue", -1);
            objectValue = getField.get("objectValue", null);
            nested = (NestedWithPutFields) getField.get("nested", null);
        }
    }

    private static class WithPutFieldsAndUnsharedFields implements Serializable {
        private static final IntHolder VALUE = new IntHolder(42);

        private IntHolder value1 = VALUE;
        private IntHolder value2 = VALUE;

        /** Both serializable fields are marked as unshared. */
        private static final ObjectStreamField[] serialPersistentFields = {
                new ObjectStreamField("value1", IntHolder.class, true),
                new ObjectStreamField("value2", IntHolder.class, true)
        };

        private void writeObject(ObjectOutputStream stream) throws IOException {
            ObjectOutputStream.PutField putField = stream.putFields();

            putField.put("value1", value1);
            putField.put("value2", value2);

            stream.writeFields();
        }

        private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
            value1 = (IntHolder) stream.readObject();
            value2 = (IntHolder) stream.readObject();
        }
    }

    private static class WithReadFieldsAndReadUnsharedFields implements Serializable {
        private static final IntHolder VALUE = new IntHolder(42);

        private IntHolder value1 = VALUE;
        private IntHolder value2 = VALUE;

        /** Both serializable fields are marked as unshared. */
        private static final ObjectStreamField[] serialPersistentFields = {
                new ObjectStreamField("value1", IntHolder.class, true),
                new ObjectStreamField("value2", IntHolder.class, true)
        };

        private void writeObject(ObjectOutputStream stream) throws IOException {
            stream.writeObject(value1);
            stream.writeObject(value2);
        }

        private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
            ObjectInputStream.GetField getField = stream.readFields();

            value1 = (IntHolder) getField.get("value1", null);
            value2 = (IntHolder) getField.get("value2", null);
        }
    }

    private static class SimpleNonSerializable {
        private int value;

        @SuppressWarnings("unused")
        public SimpleNonSerializable() {
        }

        public SimpleNonSerializable(int value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SimpleNonSerializable that = (SimpleNonSerializable) o;
            return value == that.value;
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }
    }

    private static class WithOverrideStillUsingDefaultMechanism implements Serializable {
        private final int value;

        private WithOverrideStillUsingDefaultMechanism(int value) {
            this.value = value;
        }

        private void writeObject(ObjectOutputStream stream) throws IOException {
            stream.defaultWriteObject();
        }

        private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
            stream.defaultReadObject();
        }
    }

    private static class NestedUsingDefaultMechanism implements Serializable {
        private final int value;

        private NestedUsingDefaultMechanism(int value) {
            this.value = value;
        }

        private void writeObject(ObjectOutputStream stream) throws IOException {
            stream.defaultWriteObject();
        }

        private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
            stream.defaultReadObject();
        }
    }

    private static class NestHostUsingDefaultMechanism implements Serializable {
        private final int value;
        private final NestedUsingDefaultMechanism nested;

        private NestHostUsingDefaultMechanism(int value, NestedUsingDefaultMechanism nested) {
            this.value = value;
            this.nested = nested;
        }

        private void writeObject(ObjectOutputStream stream) throws IOException {
            stream.defaultWriteObject();
        }

        private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
            stream.defaultReadObject();
        }
    }

    private static class ThrowingFromReadObject implements Serializable {
        private void writeObject(ObjectOutputStream stream) {
            // no-op
        }

        private void readObject(ObjectInputStream stream) {
            throw new RuntimeException("Oops");
        }
    }
}
