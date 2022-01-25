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

import static org.apache.ignite.internal.network.serialization.marshal.TestDescriptors.MIN_CUSTOM_DESCRIPTOR_ID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import org.apache.ignite.internal.network.serialization.BuiltInType;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorRegistry;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

class DefaultUserObjectMarshallerConcreteTypesKnownUpfrontOptimizationTest {
    private final ClassDescriptorRegistry descriptorRegistry = new ClassDescriptorRegistry();
    private final ClassDescriptorFactory descriptorFactory = new ClassDescriptorFactory(descriptorRegistry);

    private final DefaultUserObjectMarshaller marshaller = new DefaultUserObjectMarshaller(descriptorRegistry, descriptorFactory);

    private static final int SECOND_USER_OBJECT_ID = 1;
    private static final int THIRD_USER_OBJECT_ID = 2;

    @Test
    void writesOnlyThePrimitiveValueForPrimitiveFields() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new WithPrimitiveField((byte) 123));

        DataInputStream dis = openDataStreamAndSkipRootObjectHeader(marshalled);

        assertThat(dis.readAllBytes(), is(new byte[]{123}));
    }

    private DataInputStream openDataStreamAndSkipRootObjectHeader(MarshalledObject marshalled) throws IOException {
        DataInputStream dis = openDataStream(marshalled);
        skipTillRootObjectData(dis);
        return dis;
    }

    @NotNull
    private DataInputStream openDataStream(MarshalledObject marshalled) {
        return new DataInputStream(new ByteArrayInputStream(marshalled.bytes()));
    }

    private void skipTillRootObjectData(DataInputStream dos) throws IOException {
        ProtocolMarshalling.readDescriptorOrCommandId(dos);
        ProtocolMarshalling.readObjectId(dos);
    }

    @Test
    void treatsPrimitiveArrayFieldsAsFieldsWithTypeKnownUpfront() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new WithPrimitiveArrayField(new byte[]{123}));

        DataInputStream dis = openDataStreamAndSkipRootObjectHeader(marshalled);

        skipOneByteEmptyNullBitMask(dis);
        assertThat(ProtocolMarshalling.readObjectId(dis), is(SECOND_USER_OBJECT_ID));
        assertThat(ProtocolMarshalling.readLength(dis), is(1));
        assertThat(dis.readAllBytes(), is(new byte[]{123}));
    }

    @Test
    void doesNotWriteNullsBitmapForPrimitiveArrays() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new byte[]{123});

        DataInputStream dis = openDataStreamAndSkipRootObjectHeader(marshalled);

        assertThat(ProtocolMarshalling.readLength(dis), is(1));
        assertThat(dis.readAllBytes(), is(new byte[]{123}));
    }

    @Test
    void writesNullsBitmapAndNoDescriptorIdForNonNullPrimitiveWrapperFields() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new WithPrimitiveWrapperField((byte) 123));

        DataInputStream dis = openDataStreamAndSkipRootObjectHeader(marshalled);

        skipOneByteEmptyNullBitMask(dis);
        assertThat(ProtocolMarshalling.readObjectId(dis), is(SECOND_USER_OBJECT_ID));

        assertThat(dis.readAllBytes(), is(new byte[]{123}));
    }

    private void skipOneByteEmptyNullBitMask(DataInputStream dis) throws IOException {
        assertThat(dis.readByte(), is((byte) 0));
    }

    @Test
    void writesOnlyNullsBitmapForNullPrimitiveWrapperFields() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new WithPrimitiveWrapperField(null));

        DataInputStream dis = openDataStreamAndSkipRootObjectHeader(marshalled);

        final byte nullsBitmapWithOneBitSet = 1;
        assertThat(dis.readAllBytes(), is(new byte[]{nullsBitmapWithOneBitSet}));
    }

    @Test
    void writesNullsBitmapAndNoDescriptorIdForNonNullValueOfFieldWhichTypeIsKnownUpfront() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new WithFieldOfTypeKnownUpfront(new FinalClass((byte) 123)));

        DataInputStream dis = openDataStreamAndSkipRootObjectHeader(marshalled);

        skipOneByteEmptyNullBitMask(dis);
        assertThat(ProtocolMarshalling.readObjectId(dis), is(SECOND_USER_OBJECT_ID));

        assertThat(dis.readAllBytes(), is(new byte[]{123}));
    }

    @Test
    void writesOnlyNullsBitmapForNullValueOfFieldWhichTypeIsKnownUpfront() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new WithFieldOfTypeKnownUpfront(null));

        DataInputStream dis = openDataStreamAndSkipRootObjectHeader(marshalled);

        final byte nullsBitmapWithOneBitSet = (byte) 1;
        assertThat(dis.readAllBytes(), is(new byte[]{nullsBitmapWithOneBitSet}));
    }

    @Test
    void writesDescriptorIdAndObjectIdAndTheValueForNonNullValueOfFieldWhichTypeIsNotKnownUpfront() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new WithFieldOfTypeNotKnownUpfront(new NonFinalClass((byte) 123)));

        DataInputStream dis = openDataStreamAndSkipRootObjectHeader(marshalled);

        assertThat(ProtocolMarshalling.readDescriptorOrCommandId(dis), greaterThanOrEqualTo(MIN_CUSTOM_DESCRIPTOR_ID));
        assertThat(ProtocolMarshalling.readObjectId(dis), is(SECOND_USER_OBJECT_ID));

        assertThat(dis.readAllBytes(), is(new byte[]{123}));
    }

    @Test
    void writesOnlyNullMarkerForNullValueOfFieldWhichTypeIsNotKnownUpfront() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new WithFieldOfTypeNotKnownUpfront(null));

        DataInputStream dis = openDataStreamAndSkipRootObjectHeader(marshalled);

        assertThat(dis.readAllBytes(), is(new byte[]{(byte) BuiltInType.NULL.descriptorId()}));
    }

    @Test
    void writesNullsBitmapAndNoDescriptorIdForNonNullValueOfArrayFieldWhichComponentTypeIsKnownUpfront() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(
                new WithArrayFieldOfTypeKnownUpfront(new FinalClass[]{new FinalClass((byte) 123)})
        );

        DataInputStream dis = openDataStreamAndSkipRootObjectHeader(marshalled);

        // beginning of array of FinalClass
        skipOneByteEmptyNullBitMask(dis);
        assertThat(ProtocolMarshalling.readObjectId(dis), is(SECOND_USER_OBJECT_ID));
        assertThat(dis.readUTF(), is(FinalClass.class.getName()));
        assertThat(ProtocolMarshalling.readLength(dis), is(1));

        // first element of the array
        skipOneByteEmptyNullBitMask(dis);
        assertThat(ProtocolMarshalling.readObjectId(dis), is(THIRD_USER_OBJECT_ID));

        byte[] remainingBytes = dis.readAllBytes();
        assertThat(remainingBytes, is(new byte[]{123}));
    }

    @Test
    void writesDescriptorIdAndObjectIdAndTheValueForNonNullValueOfArrayFieldWhichComponentTypeIsNotKnownUpfront() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(
                new WithArrayFieldOfTypeNotKnownUpfront(new NonFinalClass[]{new NonFinalClass((byte) 123)})
        );

        DataInputStream dis = openDataStreamAndSkipRootObjectHeader(marshalled);

        // beginning of array of NonFinalClass
        assertThat(ProtocolMarshalling.readDescriptorOrCommandId(dis), greaterThanOrEqualTo(MIN_CUSTOM_DESCRIPTOR_ID));
        assertThat(ProtocolMarshalling.readObjectId(dis), is(SECOND_USER_OBJECT_ID));
        assertThat(dis.readUTF(), is(NonFinalClass.class.getName()));
        assertThat(ProtocolMarshalling.readLength(dis), is(1));

        // first element of the array
        assertThat(ProtocolMarshalling.readDescriptorOrCommandId(dis), greaterThanOrEqualTo(MIN_CUSTOM_DESCRIPTOR_ID));
        assertThat(ProtocolMarshalling.readObjectId(dis), is(THIRD_USER_OBJECT_ID));

        byte[] remainingBytes = dis.readAllBytes();
        assertThat(remainingBytes, is(new byte[]{123}));
    }

    @Test
    void writesNullsBitmapAndNoDescriptorIdForNonNullValueOfSimpleEnumField() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new WithSimpleEnumField(SimpleEnum.FIRST));

        DataInputStream dis = openDataStreamAndSkipRootObjectHeader(marshalled);

        skipOneByteEmptyNullBitMask(dis);
        assertThat(ProtocolMarshalling.readObjectId(dis), is(SECOND_USER_OBJECT_ID));
        assertThat(dis.readUTF(), is(SimpleEnum.FIRST.name()));
        assertThat(dis.available(), is(0));
    }

    @Test
    void writesNullsBitmapAndNoDescriptorIdForNonNullValueOfEnumWithAnonClassForMemberField() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new WithEnumWithAnonClassesForMembersField(EnumWithAnonClassesForMembers.FIRST));

        DataInputStream dis = openDataStreamAndSkipRootObjectHeader(marshalled);

        skipOneByteEmptyNullBitMask(dis);
        assertThat(ProtocolMarshalling.readObjectId(dis), is(SECOND_USER_OBJECT_ID));
        assertThat(dis.readUTF(), is(EnumWithAnonClassesForMembers.FIRST.name()));
        assertThat(dis.available(), is(0));
    }

    @Test
    void writesNullsBitmapAndNoDescriptorIdForNonNullSimpleEnumArrayElement() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new SimpleEnum[]{SimpleEnum.FIRST});

        DataInputStream dis = openDataStream(marshalled);

        assertThat(ProtocolMarshalling.readDescriptorOrCommandId(dis), greaterThanOrEqualTo(MIN_CUSTOM_DESCRIPTOR_ID));
        ProtocolMarshalling.readObjectId(dis);

        assertThat(dis.readUTF(), is(SimpleEnum.class.getName()));
        assertThat(ProtocolMarshalling.readLength(dis), is(1));
        skipOneByteEmptyNullBitMask(dis);
        ProtocolMarshalling.readObjectId(dis);
        assertThat(dis.readUTF(), is(SimpleEnum.FIRST.name()));
        assertThat(dis.available(), is(0));
    }

    @Test
    void writesNullsBitmapAndNoDescriptorIdForNonNullEnumWithAnonClassForMemberArrayElement() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new EnumWithAnonClassesForMembers[]{EnumWithAnonClassesForMembers.FIRST});

        DataInputStream dis = openDataStream(marshalled);

        assertThat(ProtocolMarshalling.readDescriptorOrCommandId(dis), greaterThanOrEqualTo(MIN_CUSTOM_DESCRIPTOR_ID));
        ProtocolMarshalling.readObjectId(dis);

        assertThat(dis.readUTF(), is(EnumWithAnonClassesForMembers.class.getName()));
        assertThat(ProtocolMarshalling.readLength(dis), is(1));
        skipOneByteEmptyNullBitMask(dis);
        ProtocolMarshalling.readObjectId(dis);
        assertThat(dis.readUTF(), is(EnumWithAnonClassesForMembers.FIRST.name()));
        assertThat(dis.available(), is(0));
    }

    @Test
    void writesFullDescriptorIdsForEnumsInAbstractEnumArray() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new Enum[]{SimpleEnum.FIRST});

        DataInputStream dis = openDataStream(marshalled);

        assertThat(ProtocolMarshalling.readDescriptorOrCommandId(dis), greaterThanOrEqualTo(MIN_CUSTOM_DESCRIPTOR_ID));
        ProtocolMarshalling.readObjectId(dis);

        assertThat(dis.readUTF(), is(Enum.class.getName()));
        assertThat(ProtocolMarshalling.readLength(dis), is(1));
        assertThat(ProtocolMarshalling.readDescriptorOrCommandId(dis), greaterThanOrEqualTo(MIN_CUSTOM_DESCRIPTOR_ID));
        ProtocolMarshalling.readObjectId(dis);
        assertThat(dis.readUTF(), is(SimpleEnum.FIRST.name()));
        assertThat(dis.available(), is(0));
    }

    @Test
    void supportsFinalClassOptimizationInPutFields() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new WithPutFieldsReadFields());

        DataInputStream dis = openDataStreamAndSkipRootObjectHeader(marshalled);

        skipOneByteEmptyNullBitMask(dis);
        assertThat(ProtocolMarshalling.readObjectId(dis), is(SECOND_USER_OBJECT_ID));

        byte[] remainingBytes = dis.readAllBytes();
        assertThat(remainingBytes, is(new byte[]{42}));
    }

    @Test
    void marshalsAndUnmarshalsCaseWithFinalClassOptimizationUsingPutAndReadFields() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new WithPutFieldsReadFields());
        WithPutFieldsReadFields unmarshalled = marshaller.unmarshal(marshalled.bytes(), descriptorRegistry);

        assertThat(unmarshalled, is(notNullValue()));
        assertThat(unmarshalled.value.byteValue, is((byte) 42));
    }

    private static class WithPrimitiveField {
        @SuppressWarnings({"unused", "FieldCanBeLocal"})
        private final byte value;

        private WithPrimitiveField(byte value) {
            this.value = value;
        }
    }

    private static class WithPrimitiveArrayField {
        @SuppressWarnings({"unused", "FieldCanBeLocal"})
        private final byte[] value;

        private WithPrimitiveArrayField(byte[] value) {
            this.value = value;
        }
    }

    private static class WithPrimitiveWrapperField {
        @SuppressWarnings({"unused", "FieldCanBeLocal"})
        private final Byte value;

        private WithPrimitiveWrapperField(Byte value) {
            this.value = value;
        }
    }

    private static final class FinalClass {
        @SuppressWarnings({"FieldCanBeLocal", "unused"})
        private final byte byteValue;

        private FinalClass(byte byteValue) {
            this.byteValue = byteValue;
        }
    }

    private static class WithFieldOfTypeKnownUpfront {
        @SuppressWarnings({"unused", "FieldCanBeLocal"})
        private final FinalClass value;

        private WithFieldOfTypeKnownUpfront(FinalClass value) {
            this.value = value;
        }
    }

    private static class NonFinalClass {
        @SuppressWarnings({"FieldCanBeLocal", "unused"})
        private final byte byteValue;

        private NonFinalClass(byte byteValue) {
            this.byteValue = byteValue;
        }
    }

    private static class WithFieldOfTypeNotKnownUpfront {
        @SuppressWarnings({"unused", "FieldCanBeLocal"})
        private final NonFinalClass value;

        private WithFieldOfTypeNotKnownUpfront(NonFinalClass value) {
            this.value = value;
        }
    }

    private static class WithArrayFieldOfTypeKnownUpfront {
        @SuppressWarnings({"unused", "FieldCanBeLocal"})
        private final FinalClass[] value;

        private WithArrayFieldOfTypeKnownUpfront(FinalClass[] value) {
            this.value = value;
        }
    }

    private static class WithArrayFieldOfTypeNotKnownUpfront {
        @SuppressWarnings({"unused", "FieldCanBeLocal"})
        private final NonFinalClass[] value;

        private WithArrayFieldOfTypeNotKnownUpfront(NonFinalClass[] value) {
            this.value = value;
        }
    }

    private static class WithSimpleEnumField {
        @SuppressWarnings({"FieldCanBeLocal", "unused"})
        private final SimpleEnum value;

        private WithSimpleEnumField(SimpleEnum value) {
            this.value = value;
        }
    }

    private static class WithEnumWithAnonClassesForMembersField {
        @SuppressWarnings({"FieldCanBeLocal", "unused"})
        private final EnumWithAnonClassesForMembers value;

        private WithEnumWithAnonClassesForMembersField(EnumWithAnonClassesForMembers value) {
            this.value = value;
        }
    }

    private static class WithPutFieldsReadFields implements Serializable {
        private FinalClass value = new FinalClass((byte) 42);

        private void writeObject(ObjectOutputStream stream) throws IOException {
            ObjectOutputStream.PutField putField = stream.putFields();
            putField.put("value", value);
            stream.writeFields();
        }

        private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
            ObjectInputStream.GetField getField = stream.readFields();
            value = (FinalClass) getField.get("value", null);
        }
    }
}
