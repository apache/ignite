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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.network.serialization.BuiltinType;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactoryContext;
import org.apache.ignite.internal.network.serialization.IdIndexedDescriptors;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests for how {@link DefaultUserObjectMarshaller} handles arbitrary objects.
 * An arbitrary object is an object of a class that is not built-in, not a {@link Serializable} and not an {@link java.io.Externalizable}.
 */
class DefaultUserObjectMarshallerWithArbitraryObjectsTest {
    private final ClassDescriptorFactoryContext descriptorRegistry = new ClassDescriptorFactoryContext();
    private final ClassDescriptorFactory descriptorFactory = new ClassDescriptorFactory(descriptorRegistry);
    private final IdIndexedDescriptors descriptors = new ContextBasedIdIndexedDescriptors(descriptorRegistry);

    private final DefaultUserObjectMarshaller marshaller = new DefaultUserObjectMarshaller(descriptorRegistry, descriptorFactory);

    private static boolean constructorCalled;

    @Test
    void marshalsAndUnmarshalsSimpleClassInstances() throws Exception {
        Simple unmarshalled = marshalAndUnmarshalNonNull(new Simple(42));

        assertThat(unmarshalled.value, is(42));
    }

    @NotNull
    private <T> T marshalAndUnmarshalNonNull(Object object) throws MarshalException, UnmarshalException {
        MarshalledObject marshalled = marshaller.marshal(object);
        return unmarshalNonNull(marshalled);
    }

    @NotNull
    private <T> T unmarshalNonNull(MarshalledObject marshalled) throws UnmarshalException {
        T unmarshalled = marshaller.unmarshal(marshalled.bytes(), descriptors);

        assertThat(unmarshalled, is(notNullValue()));

        return unmarshalled;
    }

    @Test
    void marshalsArbitraryObjectsUsingDescriptorsOfThemAndTheirContents() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new Simple(42));

        assertThat(marshalled.usedDescriptors(), equalTo(Set.of(
                descriptorRegistry.getRequiredDescriptor(Simple.class),
                descriptorRegistry.getBuiltInDescriptor(BuiltinType.INT)
        )));
    }

    @Test
    void marshalsArbitraryObjectWithCorrectDescriptorIdInMarshalledRepresentation() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new Simple(42));

        assertThat(readType(marshalled), is(descriptorRegistry.getRequiredDescriptor(Simple.class).descriptorId()));
    }

    private int readType(MarshalledObject marshalled) throws IOException {
        try (var dis = new DataInputStream(new ByteArrayInputStream(marshalled.bytes()))) {
            return ProtocolMarshalling.readDescriptorOrCommandId(dis);
        }
    }

    @Test
    void marshalsAndUnmarshalsClassInstancesInvolvingSuperclasses() throws Exception {
        Child unmarshalled = marshalAndUnmarshalNonNull(new Child("answer", 42));

        assertThat(unmarshalled.parentValue(), is("answer"));
        assertThat(unmarshalled.childValue(), is(42));
    }

    @Test
    void usesDescriptorsOfAllAncestors() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new Child("answer", 42));

        assertThat(marshalled.usedDescriptors(), hasItems(
                descriptorRegistry.getRequiredDescriptor(Parent.class),
                descriptorRegistry.getRequiredDescriptor(Child.class)
        ));
    }

    @Test
    void marshalsAndUnmarshalsClassInstancesHavingNestedArbitraryObjects() throws Exception {
        WithArbitraryClassField unmarshalled = marshalAndUnmarshalNonNull(new WithArbitraryClassField(new Simple(42)));

        assertThat(unmarshalled.nested, is(notNullValue()));
        assertThat(unmarshalled.nested.value, is(42));
    }

    @Test
    void marshalsAndUnmarshalsClassInstancesHavingCollectionsOfArbitraryObjects() throws Exception {
        WithArbitraryObjectInList unmarshalled = marshalAndUnmarshalNonNull(withArbitraryObjectInArrayList(new Simple(42)));

        assertThat(unmarshalled.list, hasSize(1));
        assertThat(unmarshalled.list.get(0).value, is(42));
    }

    private WithArbitraryObjectInList withArbitraryObjectInArrayList(Simple object) {
        List<Simple> list = new ArrayList<>(List.of(object));
        return new WithArbitraryObjectInList(list);
    }

    @Test
    void marshalsAndUnmarshalsClassInstancesHavingPolymorphicNestedArbitraryObjects() throws Exception {
        WithArbitraryClassField unmarshalled = marshalAndUnmarshalNonNull(new WithArbitraryClassField(new ChildOfSimple(42)));

        assertThat(unmarshalled.nested, is(instanceOf(ChildOfSimple.class)));
        assertThat(unmarshalled.nested.value, is(42));
    }

    @Test
    void marshalsAndUnmarshalsClassInstancesHavingCollectionsOfPolymorphicArbitraryObjects() throws Exception {
        WithArbitraryObjectInList object = withArbitraryObjectInArrayList(new ChildOfSimple(42));

        WithArbitraryObjectInList unmarshalled = marshalAndUnmarshalNonNull(object);

        assertThat(unmarshalled.list, hasSize(1));
        assertThat(unmarshalled.list.get(0), is(instanceOf(ChildOfSimple.class)));
        assertThat(unmarshalled.list.get(0).value, is(42));
    }

    @Test
    void restoresConcreteCollectionTypeCorrectlyWhenUnmarshalls() throws Exception {
        WithArbitraryObjectInList unmarshalled = marshalAndUnmarshalNonNull(withArbitraryObjectInArrayList(new Simple(42)));

        assertThat(unmarshalled.list, is(instanceOf(ArrayList.class)));
    }

    @Test
    void ignoresTransientFields() throws Exception {
        WithTransientFields unmarshalled = marshalAndUnmarshalNonNull(new WithTransientFields("Hi"));

        assertThat(unmarshalled.value, is(nullValue()));
    }

    @Test
    void supportsFinalFields() throws Exception {
        WithFinalFields unmarshalled = marshalAndUnmarshalNonNull(new WithFinalFields(42));

        assertThat(unmarshalled.value, is(42));
    }

    @Test
    void supportsNonCapturingAnonymousClassInstances() throws Exception {
        Callable<String> unmarshalled = marshalAndUnmarshalNonNull(nonCapturingAnonymousInstance());

        assertThat(unmarshalled.call(), is("Hi!"));
    }

    @SuppressWarnings("Convert2Lambda")
    private static Callable<String> nonCapturingAnonymousInstance() {
        return new Callable<>() {
            @Override
            public String call() {
                return "Hi!";
            }
        };
    }

    @Test
    void supportsNonCapturingLambdas() throws Exception {
        Callable<String> unmarshalled = marshalAndUnmarshalNonNull(nonCapturingLambda());

        assertThat(unmarshalled.call(), is("Hi!"));
    }

    private static Callable<String> nonCapturingLambda() {
        return () -> "Hi!";
    }

    @Test
    @Disabled("IGNITE-16258")
    // TODO: IGNITE-16258 - enable this test when we are able to work with serializable lambdas
    void supportsNonCapturingSerializableLambdas() throws Exception {
        Callable<String> unmarshalled = marshalAndUnmarshalNonNull(nonCapturingSerializableLambda());

        assertThat(unmarshalled.call(), is("Hi!"));
    }

    private static Callable<String> nonCapturingSerializableLambda() {
        return (Callable<String> & Serializable) () -> "Hi!";
    }

    @Test
    void doesNotSupportInnerClassInstances() {
        Throwable ex = assertThrows(IllegalArgumentException.class, () -> marshaller.marshal(new Inner()));
        assertThat(ex.getMessage(), is("Non-static inner class instances are not supported for marshalling: " + Inner.class));
    }

    @Test
    void doesNotSupportInnerClassInstancesInsideContainers() {
        List<Inner> list = singletonList(new Inner());

        Throwable ex = assertThrows(IllegalArgumentException.class, () -> marshaller.marshal(list));
        assertThat(ex.getMessage(), is("Non-static inner class instances are not supported for marshalling: " + Inner.class));
    }

    @Test
    void doesNotSupportCapturingAnonymousClassInstances() {
        Runnable capturingClosure = capturingAnonymousInstance();

        Throwable ex = assertThrows(IllegalArgumentException.class, () -> marshaller.marshal(capturingClosure));
        assertThat(ex.getMessage(), startsWith("Capturing nested class instances are not supported for marshalling: "));
    }

    private Runnable capturingAnonymousInstance() {
        //noinspection Convert2Lambda
        return new Runnable() {
            @Override
            public void run() {
                System.out.println(DefaultUserObjectMarshallerWithArbitraryObjectsTest.this);
            }
        };
    }

    @Test
    void doesNotSupportCapturingAnonymousClassInstancesInsideContainers() {
        Runnable capturingAnonymousInstance = capturingAnonymousInstance();
        List<Runnable> list = singletonList(capturingAnonymousInstance);

        Throwable ex = assertThrows(IllegalArgumentException.class, () -> marshaller.marshal(list));
        assertThat(ex.getMessage(), startsWith("Capturing nested class instances are not supported for marshalling: "));
    }

    @Test
    void doesNotSupportCapturingLambdas() {
        Runnable capturingClosure = capturingLambda();

        Throwable ex = assertThrows(IllegalArgumentException.class, () -> marshaller.marshal(capturingClosure));
        assertThat(ex.getMessage(), startsWith("Capturing nested class instances are not supported for marshalling: "));
    }

    private Runnable capturingLambda() {
        return () -> System.out.println(DefaultUserObjectMarshallerWithArbitraryObjectsTest.this);
    }

    @Test
    void doesNotSupportCapturingAnonymousLambdasInsideContainers() {
        Runnable capturingLambda = capturingLambda();
        List<Runnable> list = singletonList(capturingLambda);

        Throwable ex = assertThrows(IllegalArgumentException.class, () -> marshaller.marshal(list));
        assertThat(ex.getMessage(), startsWith("Capturing nested class instances are not supported for marshalling: "));
    }

    @Test
    void supportsNonCapturingLocalClassInstances() throws Exception {
        Callable<String> unmarshalled = marshalAndUnmarshalNonNull(nonCapturingLocalClassInstance());

        assertThat(unmarshalled.call(), is("Hi!"));
    }

    private static Object nonCapturingLocalClassInstance() {
        class Local implements Callable<String> {
            /** {@inheritDoc} */
            @Override
            public String call() {
                return "Hi!";
            }
        }

        return new Local();
    }

    @Test
    void doesNotSupportCapturingLocalClassInstances() {
        Object instance = capturingLocalClassInstance();

        Throwable ex = assertThrows(IllegalArgumentException.class, () -> marshaller.marshal(instance));
        assertThat(ex.getMessage(), startsWith("Capturing nested class instances are not supported for marshalling: "));
    }

    private Object capturingLocalClassInstance() {
        class Local {
        }

        return new Local();
    }

    @Test
    void supportsClassesWithoutNoArgConstructor() throws Exception {
        WithoutNoArgConstructor unmarshalled = marshalAndUnmarshalNonNull(new WithoutNoArgConstructor(42));

        assertThat(unmarshalled.value, is(42));
    }

    @Test
    void supportsInstancesDirectlyContainingThemselvesInFields() throws Exception {
        WithInfiniteCycleViaField unmarshalled = marshalAndUnmarshalNonNull(new WithInfiniteCycleViaField(42));

        assertThat(unmarshalled.value, is(42));
        assertThat(unmarshalled.myself, is(sameInstance(unmarshalled)));
    }

    @Test
    void supportsInstancesParticipatingInIndirectInfiniteCyclesViaArbitraryObjects() throws Exception {
        WithFirstCyclePart first = new WithFirstCyclePart();
        WithSecondCyclePart second = new WithSecondCyclePart();
        first.part = second;
        second.part = first;

        WithFirstCyclePart unmarshalled = marshalAndUnmarshalNonNull(first);

        assertThat(unmarshalled.part.part, is(sameInstance(unmarshalled)));
    }

    @Test
    void supportsInstancesParticipatingInIndirectInfiniteCyclesViaMutableContainers() throws Exception {
        WithObjectList object = new WithObjectList();
        List<Object> container = new ArrayList<>();
        object.contents = container;
        container.add(object);

        WithObjectList unmarshalled = marshalAndUnmarshalNonNull(object);

        assertThat(unmarshalled.contents.get(0), is(sameInstance(unmarshalled)));
    }

    @Test
    void doesNotInvokeNoArgConstructorOfArbitraryClassOnUnmarshalling() throws Exception {
        WithSideEffectInConstructor object = new WithSideEffectInConstructor();
        constructorCalled = false;

        marshalAndUnmarshalNonNull(object);

        assertFalse(constructorCalled);
    }

    @Test
    void supportsListOf() throws Exception {
        List<Integer> list = marshalAndUnmarshalNonNull(List.of(1, 2, 3));

        assertThat(list, contains(1, 2, 3));
    }

    @Test
    void supportsSetOf() throws Exception {
        Set<Integer> set = marshalAndUnmarshalNonNull(Set.of(1, 2, 3));

        assertThat(set, containsInAnyOrder(1, 2, 3));
    }

    @Test
    void supportsMapOf() throws Exception {
        Map<?, ?> map = marshalAndUnmarshalNonNull(Map.of(1, 2, 3, 4));

        assertThat(map, is(equalTo(Map.of(1, 2, 3, 4))));
    }

    @Test
    void supportsEnumsInFields() throws Exception {
        WithSimpleEnumField unmarshalled = marshalAndUnmarshalNonNull(new WithSimpleEnumField(SimpleEnum.FIRST));

        assertThat(unmarshalled.value, is(SimpleEnum.FIRST));
    }

    @Test
    void supportsEnumsWithAnonClassesForMembersInFields() throws Exception {
        WithEnumWithAnonClassesForMembersField object = new WithEnumWithAnonClassesForMembersField(EnumWithAnonClassesForMembers.FIRST);

        WithEnumWithAnonClassesForMembersField unmarshalled = marshalAndUnmarshalNonNull(object);

        assertThat(unmarshalled.value, is(EnumWithAnonClassesForMembers.FIRST));
    }

    private static class Simple {
        private int value;

        @SuppressWarnings("unused") // needed for instantiation
        public Simple() {
        }

        public Simple(int value) {
            this.value = value;
        }
    }

    private abstract static class Parent {
        private String value;

        public Parent() {
        }

        public Parent(String value) {
            this.value = value;
        }

        String parentValue() {
            return value;
        }
    }

    private static class Child extends Parent {
        private int value;

        @SuppressWarnings("unused") // needed for instantiation
        public Child() {
        }

        public Child(String parentValue, int childValue) {
            super(parentValue);
            this.value = childValue;
        }

        int childValue() {
            return value;
        }
    }

    private static class WithArbitraryClassField {
        private Simple nested;

        @SuppressWarnings("unused") // used for instantiation
        public WithArbitraryClassField() {
        }

        public WithArbitraryClassField(Simple nested) {
            this.nested = nested;
        }
    }

    private static class WithArbitraryObjectInList {
        private List<Simple> list;

        @SuppressWarnings("unused") // needed for instantiation
        public WithArbitraryObjectInList() {
        }

        public WithArbitraryObjectInList(List<Simple> list) {
            this.list = list;
        }
    }

    private static class ChildOfSimple extends Simple {
        @SuppressWarnings("unused") // needed for instantiation
        public ChildOfSimple() {
        }

        public ChildOfSimple(int value) {
            super(value);
        }
    }

    private static class WithTransientFields {
        private transient String value;

        @SuppressWarnings("unused") // needed for instantiation
        public WithTransientFields() {
        }

        public WithTransientFields(String value) {
            this.value = value;
        }
    }

    private static class WithFinalFields {
        private final int value;

        @SuppressWarnings("unused") // needed for instantiation
        public WithFinalFields() {
            this(0);
        }

        private WithFinalFields(int value) {
            this.value = value;
        }
    }

    @SuppressWarnings("InnerClassMayBeStatic")
    private class Inner {
    }

    private static class WithInfiniteCycleViaField {
        private int value;
        @SuppressWarnings({"FieldCanBeLocal", "unused"})
        private WithInfiniteCycleViaField myself;

        @SuppressWarnings("unused")
        public WithInfiniteCycleViaField() {
        }

        public WithInfiniteCycleViaField(int value) {
            this.value = value;

            this.myself = this;
        }
    }

    private static class WithFirstCyclePart {
        private WithSecondCyclePart part;
    }

    private static class WithSecondCyclePart {
        private WithFirstCyclePart part;
    }

    private static class WithObjectList {
        private List<Object> contents;
    }

    private static class WithSideEffectInConstructor {
        public WithSideEffectInConstructor() {
            constructorCalled = true;
        }
    }

    private static class WithSimpleEnumField {
        private final SimpleEnum value;

        private WithSimpleEnumField(SimpleEnum value) {
            this.value = value;
        }
    }

    private static class WithEnumWithAnonClassesForMembersField {
        private final EnumWithAnonClassesForMembers value;

        private WithEnumWithAnonClassesForMembersField(EnumWithAnonClassesForMembers value) {
            this.value = value;
        }
    }
}
