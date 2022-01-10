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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.network.serialization.BuiltinType;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactoryContext;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests for how {@link DefaultUserObjectMarshaller} handles arbitrary objects.
 */
class DefaultUserObjectMarshallerWithArbitraryObjectsTest {
    private final ClassDescriptorFactoryContext descriptorRegistry = new ClassDescriptorFactoryContext();
    private final ClassDescriptorFactory descriptorFactory = new ClassDescriptorFactory(descriptorRegistry);

    private final DefaultUserObjectMarshaller marshaller = new DefaultUserObjectMarshaller(descriptorRegistry, descriptorFactory);

    @Test
    void marshalsAndUnmarshalsSimpleClassInstances() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new Simple(42));

        Simple unmarshalled = unmarshalNonNull(marshalled);

        assertThat(unmarshalled.value, is(42));
    }

    private <T> T unmarshalNonNull(MarshalledObject marshalled) throws UnmarshalException {
        T unmarshalled = marshaller.unmarshal(marshalled.bytes(), descriptorRegistry);

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
            return dis.readInt();
        }
    }

    @Test
    void marshalsAndUnmarshalsClassInstancesInvolvingSuperclasses() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new SimpleChild("answer", 42));

        SimpleChild unmarshalled = unmarshalNonNull(marshalled);

        assertThat(unmarshalled.parentValue(), is("answer"));
        assertThat(unmarshalled.childValue(), is(42));
    }

    @Test
    void marshalsAndUnmarshalsClassInstancesHavingNestedArbitraryObjects() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new WithArbitraryClassField(new Simple(42)));

        WithArbitraryClassField unmarshalled = unmarshalNonNull(marshalled);

        assertThat(unmarshalled.nested, is(notNullValue()));
        assertThat(unmarshalled.nested.value, is(42));
    }

    @Test
    void marshalsAndUnmarshalsClassInstancesHavingCollectionsOfArbitraryObjects() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(withArbitraryObjectInArrayList(new Simple(42)));

        WithArbitraryObjectInList unmarshalled = unmarshalNonNull(marshalled);

        assertThat(unmarshalled.list, hasSize(1));
        assertThat(unmarshalled.list.get(0).value, is(42));
    }

    private WithArbitraryObjectInList withArbitraryObjectInArrayList(Simple object) {
        List<Simple> list = new ArrayList<>(List.of(object));
        return new WithArbitraryObjectInList(list);
    }

    @Test
    void marshalsAndUnmarshalsClassInstancesHavingPolymorphicNestedArbitraryObjects() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new WithArbitraryClassField(new ChildOfSimple(42)));

        WithArbitraryClassField unmarshalled = unmarshalNonNull(marshalled);

        assertThat(unmarshalled.nested, is(instanceOf(ChildOfSimple.class)));
        assertThat(unmarshalled.nested.value, is(42));
    }

    @Test
    void marshalsAndUnmarshalsClassInstancesHavingCollectionsOfPolymorphicArbitraryObjects() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(withArbitraryObjectInArrayList(new ChildOfSimple(42)));

        WithArbitraryObjectInList unmarshalled = unmarshalNonNull(marshalled);

        assertThat(unmarshalled.list, hasSize(1));
        assertThat(unmarshalled.list.get(0), is(instanceOf(ChildOfSimple.class)));
        assertThat(unmarshalled.list.get(0).value, is(42));
    }

    @Test
    void restoresConcreteCollectionTypeCorrectlyWhenUnmarshalls() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(withArbitraryObjectInArrayList(new Simple(42)));

        WithArbitraryObjectInList unmarshalled = unmarshalNonNull(marshalled);

        assertThat(unmarshalled.list, is(instanceOf(ArrayList.class)));
    }

    @Test
    void ignoresTransientFields() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new WithTransientFields("Hi"));

        WithTransientFields unmarshalled = unmarshalNonNull(marshalled);

        assertThat(unmarshalled.value, is(nullValue()));
    }

    @Test
    void supportsFinalFields() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new WithFinalFields(42));

        WithFinalFields unmarshalled = unmarshalNonNull(marshalled);

        assertThat(unmarshalled.value, is(42));
    }

    @Test
    void supportsNonCapturingAnonymousClassInstances() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(nonCapturingAnonymousInstance());

        Callable<String> unmarshalled = unmarshalNonNull(marshalled);

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
        MarshalledObject marshalled = marshaller.marshal(nonCapturingLambda());

        Callable<String> unmarshalled = unmarshalNonNull(marshalled);

        assertThat(unmarshalled.call(), is("Hi!"));
    }

    private static Callable<String> nonCapturingLambda() {
        return () -> "Hi!";
    }

    @Test
    @Disabled("IGNITE-16165")
    // TODO: IGNITE-16165 - enable this test when we are able to work with serializable lambdas
    void supportsNonCapturingSerializableLambdas() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(nonCapturingSerializableLambda());

        Callable<String> unmarshalled = unmarshalNonNull(marshalled);

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
        MarshalledObject marshalled = marshaller.marshal(nonCapturingLocalClassInstance());

        Callable<String> unmarshalled = unmarshalNonNull(marshalled);

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
        MarshalledObject marshalled = marshaller.marshal(new WithoutNoArgConstructor(42));

        WithoutNoArgConstructor unmarshalled = unmarshalNonNull(marshalled);

        assertThat(unmarshalled.value, is(42));
    }

    @Test
    void supportsInstancesDirectlyContainingThemselvesInFields() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new WithInfiniteCycleViaField(42));

        WithInfiniteCycleViaField unmarshalled = unmarshalNonNull(marshalled);

        assertThat(unmarshalled.value, is(42));
        assertThat(unmarshalled.myself, is(sameInstance(unmarshalled)));
    }

    @Test
    void supportsInstancesParticipatingInIndirectInfiniteCyclesViaArbitraryObjects() throws Exception {
        WithFirstCyclePart first = new WithFirstCyclePart();
        WithSecondCyclePart second = new WithSecondCyclePart();
        first.part = second;
        second.part = first;

        MarshalledObject marshalled = marshaller.marshal(first);

        WithFirstCyclePart unmarshalled = unmarshalNonNull(marshalled);

        assertThat(unmarshalled.part.part, is(sameInstance(unmarshalled)));
    }

    @Test
    void supportsInstancesParticipatingInIndirectInfiniteCyclesViaMutableContainers() throws Exception {
        WithObjectList object = new WithObjectList();
        List<Object> container = new ArrayList<>();
        object.contents = container;
        container.add(object);

        MarshalledObject marshalled = marshaller.marshal(object);

        WithObjectList unmarshalled = unmarshalNonNull(marshalled);

        assertThat(unmarshalled.contents.get(0), is(sameInstance(unmarshalled)));
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

    private static class SimpleChild extends Parent {
        private int value;

        @SuppressWarnings("unused") // needed for instantiation
        public SimpleChild() {
        }

        public SimpleChild(String parentValue, int childValue) {
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
}
