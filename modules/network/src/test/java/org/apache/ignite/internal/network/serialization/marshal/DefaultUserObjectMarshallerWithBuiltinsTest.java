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
import static org.hamcrest.Matchers.sameInstance;
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
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.ignite.internal.network.serialization.BuiltinType;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactoryContext;
import org.apache.ignite.internal.network.serialization.IdIndexedDescriptors;
import org.apache.ignite.internal.network.serialization.Null;
import org.apache.ignite.lang.IgniteUuid;
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
    private final IdIndexedDescriptors descriptors = new ContextBasedIdIndexedDescriptors(descriptorRegistry);

    private final DefaultUserObjectMarshaller marshaller = new DefaultUserObjectMarshaller(descriptorRegistry, descriptorFactory);

    @Test
    void marshalsAndUnmarshalsBareObject() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new Object());

        Object unmarshalled = unmarshalNonNull(marshalled);

        assertThat(unmarshalled.getClass(), is(Object.class));
    }

    private <T> T unmarshalNonNull(MarshalledObject marshalled) throws UnmarshalException {
        T unmarshalled = marshaller.unmarshal(marshalled.bytes(), descriptors);

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

        assertThat(readDescriptorId(marshalled), is(BuiltinType.BARE_OBJECT.descriptorId()));
    }

    private int readDescriptorId(MarshalledObject marshalled) throws IOException {
        try (var dis = new DataInputStream(new ByteArrayInputStream(marshalled.bytes()))) {
            return ProtocolMarshalling.readDescriptorOrCommandId(dis);
        }
    }

    @Test
    void marshalsAndUnmarshalsNullThrowable() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(null, Throwable.class);

        Throwable unmarshalled = marshaller.unmarshal(marshalled.bytes(), descriptors);

        assertThat(unmarshalled, is(nullValue()));
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

        Object unmarshalled = marshaller.unmarshal(marshalled.bytes(), descriptors);

        assertThat(unmarshalled, is(equalTo(typeValue.value)));
        if (typeValue.builtinType != BuiltinType.NULL && typeValue.value.getClass().isArray()) {
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
                builtInTypeValue((byte) 42, byte.class, BuiltinType.BYTE),
                builtInTypeValue((byte) 42, Byte.class, BuiltinType.BYTE_BOXED),
                builtInTypeValue((short) 42, short.class, BuiltinType.SHORT),
                builtInTypeValue((short) 42, Short.class, BuiltinType.SHORT_BOXED),
                builtInTypeValue(42, int.class, BuiltinType.INT),
                builtInTypeValue(42, Integer.class, BuiltinType.INT_BOXED),
                builtInTypeValue(42.0f, float.class, BuiltinType.FLOAT),
                builtInTypeValue(42.0f, Float.class, BuiltinType.FLOAT_BOXED),
                builtInTypeValue((long) 42, long.class, BuiltinType.LONG),
                builtInTypeValue((long) 42, Long.class, BuiltinType.LONG_BOXED),
                builtInTypeValue(42.0, double.class, BuiltinType.DOUBLE),
                builtInTypeValue(42.0, Double.class, BuiltinType.DOUBLE_BOXED),
                builtInTypeValue(true, boolean.class, BuiltinType.BOOLEAN),
                builtInTypeValue(true, Boolean.class, BuiltinType.BOOLEAN_BOXED),
                builtInTypeValue('a', char.class, BuiltinType.CHAR),
                builtInTypeValue('a', Character.class, BuiltinType.CHAR_BOXED),
                // BARE_OBJECT is handled separately
                builtInTypeValue("abc", String.class, BuiltinType.STRING),
                builtInTypeValue(UUID.fromString("c6f57d4a-619f-11ec-add6-73bc97c3c49e"), UUID.class, BuiltinType.UUID),
                builtInTypeValue(IgniteUuid.fromString("1234-c6f57d4a-619f-11ec-add6-73bc97c3c49e"), IgniteUuid.class,
                        BuiltinType.IGNITE_UUID),
                builtInTypeValue(new Date(42), Date.class, BuiltinType.DATE),
                builtInTypeValue(new byte[]{1, 2, 3}, byte[].class, BuiltinType.BYTE_ARRAY),
                builtInTypeValue(new short[]{1, 2, 3}, short[].class, BuiltinType.SHORT_ARRAY),
                builtInTypeValue(new int[]{1, 2, 3}, int[].class, BuiltinType.INT_ARRAY),
                builtInTypeValue(new float[]{1.0f, 2.0f, 3.0f}, float[].class, BuiltinType.FLOAT_ARRAY),
                builtInTypeValue(new long[]{1, 2, 3}, long[].class, BuiltinType.LONG_ARRAY),
                builtInTypeValue(new double[]{1.0, 2.0, 3.0}, double[].class, BuiltinType.DOUBLE_ARRAY),
                builtInTypeValue(new boolean[]{true, false}, boolean[].class, BuiltinType.BOOLEAN_ARRAY),
                builtInTypeValue(new char[]{'a', 'b'}, char[].class, BuiltinType.CHAR_ARRAY),
                builtInTypeValue(new Object[]{42, "123", null}, Object[].class, BuiltinType.OBJECT_ARRAY),
                builtInTypeValue(new BitSet[]{BitSet.valueOf(new long[]{42, 43}), BitSet.valueOf(new long[]{1, 2}), null},
                        BitSet[].class, BuiltinType.OBJECT_ARRAY),
                builtInTypeValue(new String[]{"Ignite", "rulez"}, String[].class, BuiltinType.STRING_ARRAY),
                builtInTypeValue(new BigDecimal(42), BigDecimal.class, BuiltinType.DECIMAL),
                builtInTypeValue(new BigDecimal[]{new BigDecimal(42), new BigDecimal(43)}, BigDecimal[].class,
                        BuiltinType.DECIMAL_ARRAY),
                builtInTypeValue(SimpleEnum.FIRST, SimpleEnum.class, BuiltinType.ENUM),
                builtInTypeValue(new Enum[]{SimpleEnum.FIRST, SimpleEnum.SECOND}, Enum[].class, BuiltinType.ENUM_ARRAY),
                builtInTypeValue(new SimpleEnum[]{SimpleEnum.FIRST, SimpleEnum.SECOND}, SimpleEnum[].class, BuiltinType.ENUM_ARRAY),
                builtInTypeValue(EnumWithAnonClassesForMembers.FIRST, EnumWithAnonClassesForMembers.class, BuiltinType.ENUM),
                builtInTypeValue(new Enum[]{EnumWithAnonClassesForMembers.FIRST, EnumWithAnonClassesForMembers.SECOND}, Enum[].class,
                        BuiltinType.ENUM_ARRAY),
                builtInTypeValue(
                        new EnumWithAnonClassesForMembers[]{EnumWithAnonClassesForMembers.FIRST, EnumWithAnonClassesForMembers.SECOND},
                        EnumWithAnonClassesForMembers[].class,
                        BuiltinType.ENUM_ARRAY
                ),
                builtInTypeValue(BitSet.valueOf(new long[]{42, 43}), BitSet.class, BuiltinType.BIT_SET),
                builtInTypeValue(null, Null.class, BuiltinType.NULL)
        ).map(Arguments::of);
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
                builtInTypeValue(new ArrayList<>(List.of(42, 43)), ArrayList.class, BuiltinType.ARRAY_LIST),
                builtInTypeValue(new LinkedList<>(List.of(42, 43)), LinkedList.class, BuiltinType.LINKED_LIST),
                builtInTypeValue(new HashSet<>(Set.of(42, 43)), HashSet.class, BuiltinType.HASH_SET),
                builtInTypeValue(new LinkedHashSet<>(Set.of(42, 43)), LinkedHashSet.class, BuiltinType.LINKED_HASH_SET),
                builtInTypeValue(singletonList(42), BuiltinType.SINGLETON_LIST.clazz(), BuiltinType.SINGLETON_LIST),
                builtInTypeValue(new HashMap<>(Map.of(42, 43)), HashMap.class, BuiltinType.HASH_MAP),
                builtInTypeValue(new LinkedHashMap<>(Map.of(42, 43)), LinkedHashMap.class, BuiltinType.LINKED_HASH_MAP)
        ).map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("builtInTypes")
    void marshalsBuiltInTypesWithCorrectDescriptorIdsInMarshalledRepresentation(BuiltInTypeValue typeValue) throws Exception {
        MarshalledObject marshalled = marshaller.marshal(typeValue.value, typeValue.valueClass);

        assertThat(readDescriptorId(marshalled), is(equalTo(typeValue.builtinType.descriptorId())));
    }

    static Stream<Arguments> builtInTypes() {
        return Stream.concat(builtInNonCollectionTypes(), builtInCollectionTypes());
    }

    private static BuiltInTypeValue builtInTypeValue(Object value, Class<?> valueClass, BuiltinType type) {
        return new BuiltInTypeValue(value, valueClass, type);
    }

    @Test
    void unmarshalsObjectGraphWithCycleStartingWithSingletonList() throws Exception {
        List<List<?>> mutableList = new ArrayList<>();
        List<List<?>> singletonList = singletonList(mutableList);
        mutableList.add(singletonList);

        List<List<?>> unmarshalled = marshalAndUnmarshal(singletonList);

        assertThat(unmarshalled.get(0).get(0), is(sameInstance(unmarshalled)));
    }

    private <T> T marshalAndUnmarshal(T object) throws MarshalException, UnmarshalException {
        MarshalledObject marshalled = marshaller.marshal(object);
        return unmarshalNonNull(marshalled);
    }

    @Test
    void unmarshalsObjectGraphWithCycleContainingWithSingletonList() throws Exception {
        List<List<?>> mutableList = new ArrayList<>();
        List<List<?>> singletonList = singletonList(mutableList);
        mutableList.add(singletonList);

        List<List<?>> unmarshalled = marshalAndUnmarshal(mutableList);

        assertThat(unmarshalled.get(0).get(0), is(sameInstance(unmarshalled)));
    }

    @ParameterizedTest
    @MethodSource("mutableContainerSelfAssignments")
    <T> void unmarshalsObjectGraphWithSelfCycleViaMutableContainers(MutableContainerSelfAssignment<T> item) throws Exception {
        T container = item.factory.get();
        item.assignment.accept(container, container);

        T unmarshalled = marshalAndUnmarshal(container);
        T element = item.elementAccess.apply(unmarshalled);

        assertThat(element, is(sameInstance(unmarshalled)));
    }

    @SuppressWarnings("unchecked")
    private static Stream<Arguments> mutableContainerSelfAssignments() {
        return Stream.of(
                new MutableContainerSelfAssignment<>(Object[].class, () -> new Object[1], (a, b) -> a[0] = b, array -> (Object[]) array[0]),
                new MutableContainerSelfAssignment<>(ArrayList.class, ArrayList::new, ArrayList::add, list -> (ArrayList<?>) list.get(0)),
                new MutableContainerSelfAssignment<>(LinkedList.class, LinkedList::new, LinkedList::add,
                        list -> (LinkedList<?>) list.get(0)),
                new MutableContainerSelfAssignment<>(HashSet.class, HashSet::new, HashSet::add, set -> (HashSet<?>) set.iterator().next()),
                new MutableContainerSelfAssignment<>(LinkedHashSet.class, LinkedHashSet::new, LinkedHashSet::add,
                        set -> (LinkedHashSet<?>) set.iterator().next()),
                new MutableContainerSelfAssignment<>(HashMap.class, HashMap::new, (map, el) -> map.put(el, el),
                        map -> (HashMap<?, ?>) map.values().iterator().next()),
                new MutableContainerSelfAssignment<>(LinkedHashMap.class, LinkedHashMap::new, (map, el) -> map.put(el, el),
                        map -> (LinkedHashMap<?, ?>) map.values().iterator().next())
        ).map(Arguments::of);
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

    private static class MutableContainerSelfAssignment<T> {
        private final Class<T> clazz;
        private final Supplier<T> factory;
        private final BiConsumer<T, T> assignment;
        private final Function<T, T> elementAccess;

        private MutableContainerSelfAssignment(
                Class<T> clazz,
                Supplier<T> factory,
                BiConsumer<T, T> assignment,
                Function<T, T> elementAccess
        ) {
            this.clazz = clazz;
            this.factory = factory;
            this.assignment = assignment;
            this.elementAccess = elementAccess;
        }

        @Override
        public String toString() {
            return "ContainerSelfCycle{"
                    + "clazz=" + clazz
                    + '}';
        }
    }
}
