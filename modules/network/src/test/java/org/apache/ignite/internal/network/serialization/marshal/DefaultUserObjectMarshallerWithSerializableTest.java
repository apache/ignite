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
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Set;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactoryContext;
import org.apache.ignite.internal.network.serialization.IdIndexedDescriptors;
import org.junit.jupiter.api.Test;

/**
 * Tests for how {@link DefaultUserObjectMarshaller} handles {@link java.io.Serializable}s (but not {@link Externalizable}s).
 */
class DefaultUserObjectMarshallerWithSerializableTest {
    private final ClassDescriptorFactoryContext descriptorRegistry = new ClassDescriptorFactoryContext();
    private final ClassDescriptorFactory descriptorFactory = new ClassDescriptorFactory(descriptorRegistry);
    private final IdIndexedDescriptors descriptors = new ContextBasedIdIndexedDescriptors(descriptorRegistry);

    private final DefaultUserObjectMarshaller marshaller = new DefaultUserObjectMarshaller(descriptorRegistry, descriptorFactory);

    private static final int WRITE_REPLACE_INCREMENT = 1_000_000;
    private static final int READ_RESOLVE_INCREMENT = 1_000;

    private static final int WRITE_OBJECT_INCREMENT = 10;
    private static final int READ_OBJECT_INCREMENT = 100;

    private static final int CHILD_WRITE_OBJECT_INCREMENT = 3;
    private static final int CHILD_READ_OBJECT_INCREMENT = 6;

    private static boolean nonSerializableParentConstructorCalled;
    private static boolean constructorCalled;

    @Test
    void marshalsAndUnmarshalsSerializable() throws Exception {
        SimpleSerializable unmarshalled = marshalAndUnmarshalNonNull(new SimpleSerializable(42));

        assertThat(unmarshalled.intValue, is(42));
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

    @Test
    void appliesWriteReplaceOnSerializable() throws Exception {
        SimpleSerializable unmarshalled = marshalAndUnmarshalNonNull(new SerializableWithWriteReplace(42));

        assertThat(unmarshalled.intValue, is(equalTo(42 + WRITE_REPLACE_INCREMENT)));
    }

    @Test
    void appliesReadResolveOnSerializable() throws Exception {
        SimpleSerializable unmarshalled = marshalAndUnmarshalNonNull(new SerializableWithReadResolve(42));

        assertThat(unmarshalled.intValue, is(equalTo(42 + READ_RESOLVE_INCREMENT)));
    }

    @Test
    void appliesBothWriteReplaceAndReadResolveOnSerializable() throws Exception {
        SimpleSerializable unmarshalled = marshalAndUnmarshalNonNull(new SerializableWithWriteReplaceReadResolve(42));

        assertThat(unmarshalled.intValue, is(equalTo(42 + WRITE_REPLACE_INCREMENT + READ_RESOLVE_INCREMENT)));
    }

    @Test
    void usesDescriptorOfReplacementWhenSerializableIsReplacedWithSomethingDifferent() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new SerializableWithReplaceWithSimple(42));

        ClassDescriptor originalDescriptor = descriptorRegistry.getRequiredDescriptor(SerializableWithReplaceWithSimple.class);
        assertThat(marshalled.usedDescriptors(), not(hasItem(originalDescriptor)));

        ClassDescriptor replacementDescriptor = descriptorRegistry.getRequiredDescriptor(SimpleSerializable.class);
        assertThat(marshalled.usedDescriptors(), hasItem(replacementDescriptor));
    }

    @Test
    void marshalsSerializableWithReplaceWithNull() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new SerializableWithReplaceWithNull(42));

        SimpleSerializable unmarshalled = marshaller.unmarshal(marshalled.bytes(), descriptors);

        assertThat(unmarshalled, is(nullValue()));
    }

    @Test
    void onlyUsesDescriptorOfNullWhenSerializableIsReplacedWithNull() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new SerializableWithReplaceWithNull(42));

        ClassDescriptor replacementDescriptor = descriptorRegistry.getNullDescriptor();
        assertThat(marshalled.usedDescriptors(), equalTo(Set.of(replacementDescriptor)));
    }

    @Test
    void unmarshalsSerializableWithResolveWithNull() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new SerializableWithResolveWithNull(42));

        SimpleSerializable unmarshalled = marshaller.unmarshal(marshalled.bytes(), descriptors);

        assertThat(unmarshalled, is(nullValue()));
    }

    @Test
    void appliesWriteReplaceOnSerializableRecursively() throws Exception {
        Object result = marshalAndUnmarshalNonNull(new SerializableWithWriteReplaceChain1(0));

        assertThat(result, is(instanceOf(Integer.class)));
        assertThat(result, is(3));
    }

    @Test
    void stopsApplyingWriteReplaceOnSerializableWhenReplacementIsInstanceOfSameClass() throws Exception {
        SerializableWithWriteReplaceWithSameClass result = marshalAndUnmarshalNonNull(new SerializableWithWriteReplaceWithSameClass(0));

        assertThat(result.intValue, is(1));
    }

    @Test
    void causesInfiniteRecursionOnSerializableWithIndirectWriteReplaceCycle() {
        assertThrows(StackOverflowError.class, ()  -> marshalAndUnmarshalNonNull(new SerializableWithWriteReplaceCycle1(0)));
    }

    /**
     * Java Serialization applies writeReplace() repeatedly, but it only applies readResolve() once.
     * So we are emulating this behavior.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    void onlyAppliesFirstReadResolveOnSerializable() throws Exception {
        Object result = marshalAndUnmarshalNonNull(new SerializableWithReadResolveChain1(0));

        assertThat(result, is(instanceOf(SerializableWithReadResolveChain2.class)));
    }

    @Test
    void usesWriteObjectAndReadObject() throws Exception {
        SerializableWithWriteReadOverride result = marshalAndUnmarshalNonNull(new SerializableWithWriteReadOverride(42));

        assertThat(result.value, is(42 + WRITE_OBJECT_INCREMENT + READ_OBJECT_INCREMENT));
    }

    @Test
    void doesNotWriteDefaultFieldValuesDataIfWriteReadOverrideIsPresent() throws Exception {
        SerializableWithNoOpWriteReadOverride result = marshalAndUnmarshalNonNull(new SerializableWithNoOpWriteReadOverride(42));

        assertThat(result.value, is(0));
    }

    @Test
    void supportsWriteObjectAndReadObjectInHierarchy() throws Exception {
        SubclassWithWriteReadOverride result = marshalAndUnmarshalNonNull(new SubclassWithWriteReadOverride(42));

        assertThat(((SerializableWithWriteReadOverride) result).value, is(42 + WRITE_OBJECT_INCREMENT + READ_OBJECT_INCREMENT));
        assertThat(result.childValue, is(42 + CHILD_WRITE_OBJECT_INCREMENT + CHILD_READ_OBJECT_INCREMENT));
    }

    @Test
    void invokesNoArgConstructorOfNonSerializableParentOnUnmarshalling() throws Exception {
        SerializableWithSideEffectInParentConstructor object = new SerializableWithSideEffectInParentConstructor();
        nonSerializableParentConstructorCalled = false;

        marshalAndUnmarshalNonNull(object);

        assertTrue(nonSerializableParentConstructorCalled);
    }

    @Test
    void doesNotInvokeNoArgConstructorOfSerializableOnUnmarshalling() throws Exception {
        SerializableWithSideEffectInConstructor object = new SerializableWithSideEffectInConstructor();
        constructorCalled = false;

        marshalAndUnmarshalNonNull(object);

        assertFalse(constructorCalled);
    }

    @Test
    void resolveObjectWorksCorrectlyWhenInSelfCycle() throws Exception {
        SelfRefWithResolveToSelf deserialized = marshalAndUnmarshalNonNull(new SelfRefWithResolveToSelf(42));

        assertThat(deserialized.value, is(42 + READ_RESOLVE_INCREMENT));
        assertThat(deserialized.self, is(sameInstance(deserialized)));
    }

    @Test
    void resolveObjectProducesUnrolledCyclesAsInJavaSerializationWhenObjectIsInCycleWithLengthOfMoreThanOne() throws Exception {
        IndirectSelfRefWithResolveToSelf first = new IndirectSelfRefWithResolveToSelf(42);
        IndirectSelfRefWithResolveToSelf second = new IndirectSelfRefWithResolveToSelf(43);
        first.ref = second;
        second.ref = first;

        IndirectSelfRefWithResolveToSelf deserialized = marshalAndUnmarshalNonNull(first);

        assertThat(deserialized.value, is(42 + READ_RESOLVE_INCREMENT));
        assertThat(deserialized.ref.value, is(43 + READ_RESOLVE_INCREMENT));
        assertThat(deserialized.ref.ref, is(not(sameInstance(deserialized))));
    }

    /**
     * An {@link Serializable} that does not have {@code writeReplace()}/{@code readResolve()} methods or other customizations.
     */
    private static class SimpleSerializable implements Serializable {
        int intValue;

        public SimpleSerializable(int intValue) {
            this.intValue = intValue;
        }
    }

    private static class SerializableWithWriteReplace extends SimpleSerializable {
        public SerializableWithWriteReplace(int intValue) {
            super(intValue);
        }

        private Object writeReplace() {
            return new SerializableWithWriteReplace(intValue + 1_000_000);
        }
    }

    private static class SerializableWithReadResolve extends SimpleSerializable {
        public SerializableWithReadResolve(int intValue) {
            super(intValue);
        }

        private Object readResolve() {
            return new SerializableWithReadResolve(intValue + 1_000);
        }
    }

    private static class SerializableWithWriteReplaceReadResolve extends SimpleSerializable {
        public SerializableWithWriteReplaceReadResolve(int intValue) {
            super(intValue);
        }

        private Object writeReplace() {
            return new SerializableWithWriteReplaceReadResolve(intValue + 1_000_000);
        }

        private Object readResolve() {
            return new SerializableWithWriteReplaceReadResolve(intValue + 1_000);
        }
    }

    private static class SerializableWithReplaceWithSimple implements Serializable {
        private final int intValue;

        public SerializableWithReplaceWithSimple(int intValue) {
            this.intValue = intValue;
        }

        private Object writeReplace() {
            return new SimpleSerializable(intValue);
        }
    }

    private static class SerializableWithReplaceWithNull extends SimpleSerializable {
        public SerializableWithReplaceWithNull(int intValue) {
            super(intValue);
        }

        private Object writeReplace() {
            return null;
        }
    }

    private static class SerializableWithResolveWithNull extends SimpleSerializable {
        public SerializableWithResolveWithNull(int intValue) {
            super(intValue);
        }

        private Object readResolve() {
            return null;
        }
    }

    private static class SerializableWithWriteReplaceChain1 extends SimpleSerializable {
        public SerializableWithWriteReplaceChain1(int value) {
            super(value);
        }

        private Object writeReplace() {
            return new SerializableWithWriteReplaceChain2(intValue + 1);
        }
    }

    private static class SerializableWithWriteReplaceChain2 extends SimpleSerializable {
        public SerializableWithWriteReplaceChain2(int value) {
            super(value);
        }

        private Object writeReplace() {
            return intValue + 2;
        }
    }

    private static class SerializableWithWriteReplaceWithSameClass extends SimpleSerializable {
        public SerializableWithWriteReplaceWithSameClass(int value) {
            super(value);
        }

        private Object writeReplace() {
            return new SerializableWithWriteReplaceWithSameClass(intValue + 1);
        }
    }

    private static class SerializableWithWriteReplaceCycle1 extends SimpleSerializable {
        public SerializableWithWriteReplaceCycle1(int intValue) {
            super(intValue);
        }

        private Object writeReplace() {
            return new SerializableWithWriteReplaceCycle2(intValue);
        }
    }

    private static class SerializableWithWriteReplaceCycle2 extends SimpleSerializable {
        public SerializableWithWriteReplaceCycle2(int intValue) {
            super(intValue);
        }

        private Object writeReplace() {
            return new SerializableWithWriteReplaceCycle1(intValue);
        }
    }

    private static class SerializableWithReadResolveChain1 extends SimpleSerializable {
        public SerializableWithReadResolveChain1(int value) {
            super(value);
        }

        private Object readResolve() {
            return new SerializableWithReadResolveChain2(intValue + 1);
        }
    }

    private static class SerializableWithReadResolveChain2 extends SimpleSerializable {
        public SerializableWithReadResolveChain2(int value) {
            super(value);
        }

        private Object readResolve() {
            return intValue + 2;
        }
    }

    private static class SerializableWithWriteReadOverride implements Serializable {
        private int value;

        public SerializableWithWriteReadOverride(int value) {
            this.value = value;
        }

        private void writeObject(ObjectOutputStream oos) throws IOException {
            oos.writeInt(value + WRITE_OBJECT_INCREMENT);
        }

        private void readObject(ObjectInputStream ois) throws IOException {
            value = ois.readInt() + READ_OBJECT_INCREMENT;
        }
    }

    private static class SerializableWithNoOpWriteReadOverride implements Serializable {
        private final int value;

        public SerializableWithNoOpWriteReadOverride(int value) {
            this.value = value;
        }

        private void writeObject(ObjectOutputStream oos) throws IOException {
            // no-op
        }

        private void readObject(ObjectInputStream ois) throws IOException {
            // no-op
        }
    }

    private static class SubclassWithWriteReadOverride extends SerializableWithWriteReadOverride {
        private int childValue;

        public SubclassWithWriteReadOverride(int value) {
            super(value);
            this.childValue = value;
        }

        private void writeObject(ObjectOutputStream oos) throws IOException {
            oos.writeInt(childValue + CHILD_WRITE_OBJECT_INCREMENT);
        }

        private void readObject(ObjectInputStream ois) throws IOException {
            childValue = ois.readInt() + CHILD_READ_OBJECT_INCREMENT;
        }
    }

    private static class NonSerializableParentWithSideEffectInConstructor {
        protected NonSerializableParentWithSideEffectInConstructor() {
            nonSerializableParentConstructorCalled = true;
        }
    }

    private static class SerializableWithSideEffectInParentConstructor extends NonSerializableParentWithSideEffectInConstructor
            implements Serializable {
    }

    private static class SerializableWithSideEffectInConstructor implements Serializable {
        public SerializableWithSideEffectInConstructor() {
            constructorCalled = true;
        }
    }

    private static class SelfRefWithResolveToSelf implements Serializable {
        private final int value;
        private final SelfRefWithResolveToSelf self;

        private SelfRefWithResolveToSelf(int value) {
            this.value = value;
            this.self = this;
        }

        private Object readResolve() {
            return new SelfRefWithResolveToSelf(value + READ_RESOLVE_INCREMENT);
        }
    }

    private static class IndirectSelfRefWithResolveToSelf implements Serializable {
        private final int value;
        private IndirectSelfRefWithResolveToSelf ref;

        private IndirectSelfRefWithResolveToSelf(int value) {
            this.value = value;
        }

        private IndirectSelfRefWithResolveToSelf(int value, IndirectSelfRefWithResolveToSelf ref) {
            this.value = value;
            this.ref = ref;
        }

        private Object readResolve() {
            return new IndirectSelfRefWithResolveToSelf(value + READ_RESOLVE_INCREMENT, ref);
        }
    }
}
