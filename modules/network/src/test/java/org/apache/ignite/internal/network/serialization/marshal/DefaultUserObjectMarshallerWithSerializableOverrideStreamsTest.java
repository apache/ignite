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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.ignite.internal.network.serialization.BuiltinType;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactoryContext;
import org.apache.ignite.internal.network.serialization.IdIndexedDescriptors;
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
    private final ClassDescriptorFactoryContext descriptorRegistry = new ClassDescriptorFactoryContext();
    private final ClassDescriptorFactory descriptorFactory = new ClassDescriptorFactory(descriptorRegistry);
    private final IdIndexedDescriptors descriptors = new ContextBasedIdIndexedDescriptors(descriptorRegistry);

    private final DefaultUserObjectMarshaller marshaller = new DefaultUserObjectMarshaller(descriptorRegistry, descriptorFactory);

    /** This is static so that writeObject()/readObject() can easily find it. */
    private static ReaderAndWriter<?> readerAndWriter;

    /** Static access to the marshaller (for using in parameterized tests). */
    private static UserObjectMarshaller staticMarshaller;
    /** Static access to the registry (for using in parameterized tests). */
    private static ClassDescriptorFactoryContext staticDescriptorRegistry;

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
        T unmarshalled = marshaller.unmarshal(marshalled.bytes(), descriptors);

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

    // TODO: IGNITE-16240 - implement putFields()/writeFields()
    // TODO: IGNITE-16240 - implement readFields()

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
            return staticMarshaller.unmarshal(baos.toByteArray(), new ContextBasedIdIndexedDescriptors(staticDescriptorRegistry));
        } catch (UnmarshalException e) {
            throw new RuntimeException("Unmarshalling failed", e);
        }
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
    void resetThrowsInsideWriteObject() {
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

        assertThat(ProtocolMarshalling.readDescriptorOrCommandId(dis), is(BuiltinType.INT.descriptorId()));
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
