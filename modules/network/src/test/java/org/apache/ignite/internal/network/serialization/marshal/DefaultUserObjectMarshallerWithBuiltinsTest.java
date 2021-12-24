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

import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assumptions.assumingThat;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.internal.network.serialization.BuiltinType;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactoryContext;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for how {@link DefaultUserObjectMarshaller} handles primitives.
 */
class DefaultUserObjectMarshallerWithBuiltinsTest {
    private final ClassDescriptorFactoryContext descriptorRegistry = new ClassDescriptorFactoryContext();
    private final ClassDescriptorFactory descriptorFactory = new ClassDescriptorFactory(descriptorRegistry);

    private final DefaultUserObjectMarshaller marshaller = new DefaultUserObjectMarshaller(descriptorRegistry, descriptorFactory);

    @Test
    void marshalsAndUnmarshalsNull() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(null);

        Object unmarshalled = marshaller.unmarshal(marshalled.bytes(), descriptorRegistry);

        assertThat(unmarshalled, is(nullValue()));
    }

    @Test
    void marshalsNullUsingOnlyNullDescriptor() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(null);

        assertThat(marshalled.usedDescriptors(), equalTo(Set.of(descriptorRegistry.getNullDescriptor())));
    }

    @Test
    void marshalsNullWithCorrectDescriptorIdInMarshalledRepresentation() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(null);

        assertThat(readType(marshalled), is(BuiltinType.NULL.descriptorId()));
    }

    private int readType(MarshalledObject marshalled) throws IOException {
        try (var dis = new DataInputStream(new ByteArrayInputStream(marshalled.bytes()))) {
            return dis.readInt();
        }
    }

    @Test
    void marshalsAndUnmarshalsBareObject() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new Object());

        Object unmarshalled = unmarshalNonNull(marshalled);

        assertThat(unmarshalled.getClass(), is(Object.class));
    }

    private <T> T unmarshalNonNull(MarshalledObject marshalled) throws UnmarshalException {
        T unmarshalled = marshaller.unmarshal(marshalled.bytes(), descriptorRegistry);

        assertThat(unmarshalled, is(notNullValue()));

        return unmarshalled;
    }

    @Test
    void marshalsBareObjectUsingOnlyBareObjectDescriptor() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new Object());

        assertThat(marshalled.usedDescriptors(), equalTo(Set.of(descriptorRegistry.getRequiredDescriptor(Object.class))));
    }

    @Test
    void marshalsBareObjectWithCorrectDescriptorIdInMarshalledRepresentation() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new Object());

        assertThat(readType(marshalled), is(BuiltinType.BARE_OBJECT.descriptorId()));
    }

    @Test
    void marshalsObjectArrayUsingExactlyDescriptorsOfObjectArrayAndComponents() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new Object[]{42, "abc"});

        assertThat(marshalled.usedDescriptors(), containsInAnyOrder(
                descriptorRegistry.getRequiredDescriptor(Object[].class),
                descriptorRegistry.getRequiredDescriptor(Integer.class),
                descriptorRegistry.getRequiredDescriptor(String.class)
        ));
    }

    @Test
    void marshalsEnumUsingOnlyEnumDescriptor() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(SimpleEnum.FIRST);

        assertThat(marshalled.usedDescriptors(), equalTo(Set.of(descriptorRegistry.getRequiredDescriptor(Enum.class))));
    }

    @ParameterizedTest
    @MethodSource("builtInNonCollectionTypes")
    void marshalsAndUnmarshalsBuiltInNonCollectionTypes(BuiltInTypeValue typeValue) throws Exception {
        MarshalledObject marshalled = marshaller.marshal(typeValue.value, typeValue.valueClass);

        Object unmarshalled = marshaller.unmarshal(marshalled.bytes(), descriptorRegistry);

        assertThat(unmarshalled, is(equalTo(typeValue.value)));
        if (typeValue.builtinType != BuiltinType.VOID && typeValue.value.getClass().isArray()) {
            assertThat(unmarshalled, is(notNullValue()));
            assertThat(unmarshalled.getClass().getComponentType(), is(typeValue.value.getClass().getComponentType()));
        }
    }

    @ParameterizedTest
    @MethodSource("builtInNonCollectionTypes")
    void marshalsUsingOnlyCorrespondingDescriptorForBuiltInNonCollectionTypes(BuiltInTypeValue typeValue) {
        // #marshalsObjectArrayUsingExactlyDescriptorsOfObjectArrayAndComponents() checks the same for OBJECT_ARRAY

        assumingThat(typeValue.builtinType != BuiltinType.OBJECT_ARRAY, () -> {
            MarshalledObject marshalled = marshaller.marshal(typeValue.value, typeValue.valueClass);

            ClassDescriptor expectedDescriptor = descriptorRegistry.getBuiltInDescriptor(typeValue.builtinType);
            assertThat(marshalled.usedDescriptors(), equalTo(Set.of(expectedDescriptor)));
        });
    }

    static Stream<Arguments> builtInNonCollectionTypes() {
        return Stream.of(
                builtInTypeValueArg((byte) 42, byte.class, BuiltinType.BYTE),
                builtInTypeValueArg((byte) 42, Byte.class, BuiltinType.BYTE_BOXED),
                builtInTypeValueArg((short) 42, short.class, BuiltinType.SHORT),
                builtInTypeValueArg((short) 42, Short.class, BuiltinType.SHORT_BOXED),
                builtInTypeValueArg(42, int.class, BuiltinType.INT),
                builtInTypeValueArg(42, Integer.class, BuiltinType.INT_BOXED),
                builtInTypeValueArg(42.0f, float.class, BuiltinType.FLOAT),
                builtInTypeValueArg(42.0f, Float.class, BuiltinType.FLOAT_BOXED),
                builtInTypeValueArg((long) 42, long.class, BuiltinType.LONG),
                builtInTypeValueArg((long) 42, Long.class, BuiltinType.LONG_BOXED),
                builtInTypeValueArg(42.0, double.class, BuiltinType.DOUBLE),
                builtInTypeValueArg(42.0, Double.class, BuiltinType.DOUBLE_BOXED),
                builtInTypeValueArg(true, boolean.class, BuiltinType.BOOLEAN),
                builtInTypeValueArg(true, Boolean.class, BuiltinType.BOOLEAN_BOXED),
                builtInTypeValueArg('a', char.class, BuiltinType.CHAR),
                builtInTypeValueArg('a', Character.class, BuiltinType.CHAR_BOXED),
                // BARE_OBJECT is handled separately
                builtInTypeValueArg("abc", String.class, BuiltinType.STRING),
                builtInTypeValueArg(UUID.fromString("c6f57d4a-619f-11ec-add6-73bc97c3c49e"), UUID.class, BuiltinType.UUID),
                builtInTypeValueArg(IgniteUuid.fromString("1234-c6f57d4a-619f-11ec-add6-73bc97c3c49e"), IgniteUuid.class,
                        BuiltinType.IGNITE_UUID),
                builtInTypeValueArg(new Date(42), Date.class, BuiltinType.DATE),
                builtInTypeValueArg(new byte[]{1, 2, 3}, byte[].class, BuiltinType.BYTE_ARRAY),
                builtInTypeValueArg(new short[]{1, 2, 3}, short[].class, BuiltinType.SHORT_ARRAY),
                builtInTypeValueArg(new int[]{1, 2, 3}, int[].class, BuiltinType.INT_ARRAY),
                builtInTypeValueArg(new float[]{1.0f, 2.0f, 3.0f}, float[].class, BuiltinType.FLOAT_ARRAY),
                builtInTypeValueArg(new long[]{1, 2, 3}, long[].class, BuiltinType.LONG_ARRAY),
                builtInTypeValueArg(new double[]{1.0, 2.0, 3.0}, double[].class, BuiltinType.DOUBLE_ARRAY),
                builtInTypeValueArg(new boolean[]{true, false}, boolean[].class, BuiltinType.BOOLEAN_ARRAY),
                builtInTypeValueArg(new char[]{'a', 'b'}, char[].class, BuiltinType.CHAR_ARRAY),
                builtInTypeValueArg(new Object[]{42, "123", null}, Object[].class, BuiltinType.OBJECT_ARRAY),
                builtInTypeValueArg(new BitSet[]{BitSet.valueOf(new long[]{42, 43}), BitSet.valueOf(new long[]{1, 2}), null},
                        BitSet[].class, BuiltinType.OBJECT_ARRAY),
                builtInTypeValueArg(new String[]{"Ignite", "rulez"}, String[].class, BuiltinType.STRING_ARRAY),
                builtInTypeValueArg(new BigDecimal(42), BigDecimal.class, BuiltinType.DECIMAL),
                builtInTypeValueArg(new BigDecimal[]{new BigDecimal(42), new BigDecimal(43)}, BigDecimal[].class,
                        BuiltinType.DECIMAL_ARRAY),
                builtInTypeValueArg(SimpleEnum.FIRST, SimpleEnum.class, BuiltinType.ENUM),
                builtInTypeValueArg(new Enum[]{SimpleEnum.FIRST, SimpleEnum.SECOND}, Enum[].class, BuiltinType.ENUM_ARRAY),
                builtInTypeValueArg(new SimpleEnum[]{SimpleEnum.FIRST, SimpleEnum.SECOND}, SimpleEnum[].class, BuiltinType.ENUM_ARRAY),
                builtInTypeValueArg(EnumWithAnonClassesForMembers.FIRST, EnumWithAnonClassesForMembers.class, BuiltinType.ENUM),
                builtInTypeValueArg(new Enum[]{EnumWithAnonClassesForMembers.FIRST, EnumWithAnonClassesForMembers.SECOND}, Enum[].class,
                        BuiltinType.ENUM_ARRAY),
                builtInTypeValueArg(
                        new EnumWithAnonClassesForMembers[]{EnumWithAnonClassesForMembers.FIRST, EnumWithAnonClassesForMembers.SECOND},
                        EnumWithAnonClassesForMembers[].class,
                        BuiltinType.ENUM_ARRAY
                ),
                builtInTypeValueArg(BitSet.valueOf(new long[]{42, 43}), BitSet.class, BuiltinType.BIT_SET),
                builtInTypeValueArg(null, Void.class, BuiltinType.VOID)
        );
    }

    @ParameterizedTest
    @MethodSource("builtInCollectionTypes")
    void marshalsAndUnmarshalsBuiltInCollectionTypes(BuiltInTypeValue typeValue) throws Exception {
        MarshalledObject marshalled = marshaller.marshal(typeValue.value, typeValue.valueClass);

        Object unmarshalled = unmarshalNonNull(marshalled);

        assertThat(unmarshalled, is(equalTo(typeValue.value)));
    }

    @ParameterizedTest
    @MethodSource("builtInCollectionTypes")
    void marshalsAndUnmarshalsBuiltInCollectionTypesToCollectionsOfOriginalTypes(BuiltInTypeValue typeValue) throws Exception {
        MarshalledObject marshalled = marshaller.marshal(typeValue.value, typeValue.valueClass);

        Object unmarshalled = unmarshalNonNull(marshalled);

        assertThat(unmarshalled.getClass(), is(equalTo(typeValue.value.getClass())));
    }

    @ParameterizedTest
    @MethodSource("builtInCollectionTypes")
    void marshalsUsingOnlyCorrespondingDescriptorsForBuiltInCollectionTypes(BuiltInTypeValue typeValue) throws Exception {
        MarshalledObject marshalled = marshaller.marshal(typeValue.value, typeValue.valueClass);

        assertThat(marshalled.usedDescriptors(), containsInAnyOrder(
                descriptorRegistry.getBuiltInDescriptor(typeValue.builtinType),
                descriptorRegistry.getBuiltInDescriptor(BuiltinType.INT_BOXED)
        ));
    }

    static Stream<Arguments> builtInCollectionTypes() {
        return Stream.of(
                builtInTypeValueArg(new ArrayList<>(List.of(42, 43)), ArrayList.class, BuiltinType.ARRAY_LIST),
                builtInTypeValueArg(new LinkedList<>(List.of(42, 43)), LinkedList.class, BuiltinType.LINKED_LIST),
                builtInTypeValueArg(new HashSet<>(Set.of(42, 43)), HashSet.class, BuiltinType.HASH_SET),
                builtInTypeValueArg(new LinkedHashSet<>(Set.of(42, 43)), LinkedHashSet.class, BuiltinType.LINKED_HASH_SET),
                builtInTypeValueArg(singletonList(42), BuiltinType.SINGLETON_LIST.clazz(), BuiltinType.SINGLETON_LIST),
                builtInTypeValueArg(new HashMap<>(Map.of(42, 43)), HashMap.class, BuiltinType.HASH_MAP),
                builtInTypeValueArg(new LinkedHashMap<>(Map.of(42, 43)), LinkedHashMap.class, BuiltinType.LINKED_HASH_MAP)
        );
    }

    @ParameterizedTest
    @MethodSource("builtInTypes")
    void marshalsBuiltInTypesWithCorrectDescriptorIdsInMarshalledRepresentation(BuiltInTypeValue typeValue) throws Exception {
        MarshalledObject marshalled = marshaller.marshal(typeValue.value, typeValue.valueClass);

        assertThat(readType(marshalled), is(equalTo(typeValue.builtinType.descriptorId())));
    }

    static Stream<Arguments> builtInTypes() {
        return Stream.concat(builtInNonCollectionTypes(), builtInCollectionTypes());
    }

    @NotNull
    private static Arguments builtInTypeValueArg(Object value, Class<?> valueClass, BuiltinType type) {
        return Arguments.of(new BuiltInTypeValue(value, valueClass, type));
    }

    private enum SimpleEnum {
        FIRST,
        SECOND
    }

    private enum EnumWithAnonClassesForMembers {
        FIRST {
        },
        SECOND {
        }
    }

    private static class BuiltInTypeValue {
        private final Object value;
        private final Class<?> valueClass;
        private final BuiltinType builtinType;

        private BuiltInTypeValue(Object value, Class<?> valueClass, BuiltinType builtinType) {
            this.value = value;
            this.valueClass = valueClass;
            this.builtinType = builtinType;
        }

        @Override
        public String toString() {
            return "BuiltInTypeValue{"
                    + "value=" + value
                    + ", valueClass=" + valueClass
                    + ", builtinType=" + builtinType
                    + '}';
        }
    }
}
