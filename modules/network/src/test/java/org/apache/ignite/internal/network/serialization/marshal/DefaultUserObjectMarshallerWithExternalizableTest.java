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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Set;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactoryContext;
import org.apache.ignite.internal.network.serialization.IdIndexedDescriptors;
import org.junit.jupiter.api.Test;

/**
 * Tests for how {@link DefaultUserObjectMarshaller} handles {@link Externalizable}s.
 */
class DefaultUserObjectMarshallerWithExternalizableTest {
    private final ClassDescriptorFactoryContext descriptorRegistry = new ClassDescriptorFactoryContext();
    private final ClassDescriptorFactory descriptorFactory = new ClassDescriptorFactory(descriptorRegistry);
    private final IdIndexedDescriptors descriptors = new ContextBasedIdIndexedDescriptors(descriptorRegistry);

    private final DefaultUserObjectMarshaller marshaller = new DefaultUserObjectMarshaller(descriptorRegistry, descriptorFactory);

    private static final int WRITE_REPLACE_INCREMENT = 1_000_000;
    private static final int READ_RESOLVE_INCREMENT = 1_000;

    private static boolean constructorCalled;

    @Test
    void usesExactlyOneDescriptorWhenMarshallingExternalizable() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new SimpleExternalizable(42));

        ClassDescriptor expectedDescriptor = descriptorRegistry.getDescriptor(SimpleExternalizable.class);
        assertThat(expectedDescriptor, is(notNullValue()));
        assertThat(marshalled.usedDescriptors(), is(equalTo(Set.of(expectedDescriptor))));
    }

    @Test
    void marshalsAndUnmarshalsExternalizable() throws Exception {
        SimpleExternalizable unmarshalled = marshalAndUnmarshalNonNull(new SimpleExternalizable(42));

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
    void appliesWriteReplaceOnExternalizable() throws Exception {
        SimpleExternalizable unmarshalled = marshalAndUnmarshalNonNull(new ExternalizableWithWriteReplace(42));

        assertThat(unmarshalled.intValue, is(equalTo(42 + WRITE_REPLACE_INCREMENT)));
    }

    @Test
    void appliesReadResolveOnExternalizable() throws Exception {
        SimpleExternalizable unmarshalled = marshalAndUnmarshalNonNull(new ExternalizableWithReadResolve(42));

        assertThat(unmarshalled.intValue, is(equalTo(42 + READ_RESOLVE_INCREMENT)));
    }

    @Test
    void appliesBothWriteReplaceAndReadResolveOnExternalizable() throws Exception {
        SimpleExternalizable unmarshalled = marshalAndUnmarshalNonNull(new ExternalizableWithWriteReplaceReadResolve(42));

        assertThat(unmarshalled.intValue, is(equalTo(42 + WRITE_REPLACE_INCREMENT + READ_RESOLVE_INCREMENT)));
    }

    @Test
    void onlyUsesDescriptorOfReplacementWhenExternalizableIsReplacedWithSomethingDifferent() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new ExternalizableWithReplaceWithSimple(42));

        ClassDescriptor replacementDescriptor = descriptorRegistry.getRequiredDescriptor(SimpleExternalizable.class);
        assertThat(marshalled.usedDescriptors(), equalTo(Set.of(replacementDescriptor)));
    }

    @Test
    void marshalsExternalizableWithReplaceWithNull() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new ExternalizableWithReplaceWithNull(42));

        SimpleExternalizable unmarshalled = marshaller.unmarshal(marshalled.bytes(), descriptors);

        assertThat(unmarshalled, is(nullValue()));
    }

    @Test
    void onlyUsesDescriptorOfReplacementWhenExternalizableIsReplacedWithNull() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new ExternalizableWithReplaceWithNull(42));

        ClassDescriptor replacementDescriptor = descriptorRegistry.getNullDescriptor();
        assertThat(marshalled.usedDescriptors(), equalTo(Set.of(replacementDescriptor)));
    }

    @Test
    void unmarshalsExternalizableWithResolveWithNull() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new ExternalizableWithResolveWithNull(42));

        SimpleExternalizable unmarshalled = marshaller.unmarshal(marshalled.bytes(), descriptors);

        assertThat(unmarshalled, is(nullValue()));
    }

    @Test
    void appliesWriteReplaceOnExternalizableRecursively() throws Exception {
        Object result = marshalAndUnmarshalNonNull(new ExternalizableWithWriteReplaceChain1(0));

        assertThat(result, is(instanceOf(Integer.class)));
        assertThat(result, is(3));
    }

    @Test
    void stopsApplyingWriteReplaceOnExternalizableWhenReplacementIsInstanceOfSameClass() throws Exception {
        ExternalizableWithWriteReplaceWithSameClass result = marshalAndUnmarshalNonNull(new ExternalizableWithWriteReplaceWithSameClass(0));

        assertThat(result.intValue, is(1));
    }

    @Test
    void causesInfiniteRecursionOnExternalizableWithIndirectWriteReplaceCycle() {
        assertThrows(StackOverflowError.class, ()  -> marshalAndUnmarshalNonNull(new ExternalizableWithWriteReplaceCycle1(0)));
    }

    /**
     * Java Serialization applies writeReplace() repeatedly, but it only applies readResolve() once.
     * So we are emulating this behavior.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    void onlyAppliesFirstReadResolveOnExternalizable() throws Exception {
        Object result = marshalAndUnmarshalNonNull(new ExternalizableWithReadResolveChain1(0));

        assertThat(result, is(instanceOf(ExternalizableWithReadResolveChain2.class)));
    }

    @Test
    void defaultWriteObjectShouldFailInsideWriteExternal() {
        MarshalException ex = assertThrows(
                MarshalException.class,
                () -> marshaller.marshal(new ExternalizableWithDefaultWriteObjectCallInWriteObjectMethod())
        );
        assertThat(causalChain(ex), hasItem(hasProperty("message", equalTo("not in call to writeObject"))));
    }

    @Test
    void defaultReadObjectShouldFailInsideReadExternal() {
        UnmarshalException ex = assertThrows(
                UnmarshalException.class,
                () -> marshalAndUnmarshalNonNull(new ExternalizableWithDefaultReadObjectCallInReadObjectMethod())
        );
        assertThat(causalChain(ex), hasItem(hasProperty("message", equalTo("not in call to readObject"))));
    }

    @Test
    void defaultWriteObjectShouldFailInsideWriteExternalInsideWriteObject() {
        var payload = new ExternalizableWithDefaultWriteObjectCallInWriteObjectMethod();
        var object = new SerializableWithDefaultReadWriteObjectCallInReadWriteOverride(payload);

        MarshalException ex = assertThrows(MarshalException.class, () -> marshaller.marshal(object));
        assertThat(causalChain(ex), hasItem(hasProperty("message", equalTo("not in call to writeObject"))));
    }

    @Test
    void defaultReadObjectShouldFailInsideReadExternalInsideReadObject() {
        var payload = new ExternalizableWithDefaultReadObjectCallInReadObjectMethod();
        var object = new SerializableWithDefaultReadWriteObjectCallInReadWriteOverride(payload);

        UnmarshalException ex = assertThrows(UnmarshalException.class, () -> marshalAndUnmarshalNonNull(object));
        assertThat(causalChain(ex), hasItem(hasProperty("message", equalTo("not in call to readObject"))));
    }

    @Test
    void writingObjectInsideWriteExternalMarshalsTheObjectInOurFormat() throws Exception {
        MarshalledObject marshalled = marshaller.marshal(new ExternalizableWritingAndReadingObject(new IntHolder(42)));

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(marshalled.bytes()));
        ProtocolMarshalling.readDescriptorOrCommandId(dis);
        ProtocolMarshalling.readObjectId(dis);

        byte[] externalBytes = dis.readAllBytes();

        IntHolder nested = marshaller.unmarshal(externalBytes, descriptors);
        assertThat(nested, is(notNullValue()));
        assertThat(nested.value, is(42));
    }

    @Test
    void marshalsAndUnmarshalsExternalizableWritingReadingObjectInsideWriteReadExternal() throws Exception {
        ExternalizableWritingAndReadingObject object = new ExternalizableWritingAndReadingObject(new IntHolder(42));

        ExternalizableWritingAndReadingObject unmarshalled = marshalAndUnmarshalNonNull(object);

        assertThat(unmarshalled.intHolder.value, is(42));
    }

    @Test
    void invokesDefaultConstructorOnExternalizableUnmarshalling() throws Exception {
        WithSideEffectInConstructor object = new WithSideEffectInConstructor();
        constructorCalled = false;

        marshalAndUnmarshalNonNull(object);

        assertTrue(constructorCalled);
    }

    /**
     * An {@link Externalizable} that does not have {@code writeReplace()}/{@code readResolve()} methods.
     */
    private static class SimpleExternalizable implements Externalizable {
        int intValue;

        public SimpleExternalizable() {
        }

        public SimpleExternalizable(int intValue) {
            this.intValue = intValue;
        }

        /** {@inheritDoc} */
        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(-intValue);
        }

        /** {@inheritDoc} */
        @Override
        public void readExternal(ObjectInput in) throws IOException {
            intValue = -in.readInt();
        }
    }

    private static class ExternalizableWithWriteReplace extends SimpleExternalizable {
        public ExternalizableWithWriteReplace() {
        }

        public ExternalizableWithWriteReplace(int intValue) {
            super(intValue);
        }

        private Object writeReplace() {
            return new ExternalizableWithWriteReplace(intValue + 1_000_000);
        }
    }

    private static class ExternalizableWithReadResolve extends SimpleExternalizable {
        public ExternalizableWithReadResolve() {
        }

        public ExternalizableWithReadResolve(int intValue) {
            super(intValue);
        }

        private Object readResolve() {
            return new ExternalizableWithReadResolve(intValue + 1_000);
        }
    }

    private static class ExternalizableWithWriteReplaceReadResolve extends SimpleExternalizable {
        public ExternalizableWithWriteReplaceReadResolve() {
        }

        public ExternalizableWithWriteReplaceReadResolve(int intValue) {
            super(intValue);
        }

        private Object writeReplace() {
            return new ExternalizableWithWriteReplaceReadResolve(intValue + 1_000_000);
        }

        private Object readResolve() {
            return new ExternalizableWithWriteReplaceReadResolve(intValue + 1_000);
        }
    }

    private static class ExternalizableWithReplaceWithSimple implements Externalizable {
        private int intValue;

        public ExternalizableWithReplaceWithSimple() {
        }

        public ExternalizableWithReplaceWithSimple(int intValue) {
            this.intValue = intValue;
        }

        private Object writeReplace() {
            return new SimpleExternalizable(intValue);
        }

        /** {@inheritDoc} */
        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(intValue);
        }

        /** {@inheritDoc} */
        @Override
        public void readExternal(ObjectInput in) throws IOException {
            intValue = in.readInt();
        }
    }

    private static class ExternalizableWithReplaceWithNull extends SimpleExternalizable {
        public ExternalizableWithReplaceWithNull() {
        }

        public ExternalizableWithReplaceWithNull(int intValue) {
            super(intValue);
        }

        private Object writeReplace() {
            return null;
        }
    }

    private static class ExternalizableWithResolveWithNull extends SimpleExternalizable {
        public ExternalizableWithResolveWithNull() {
        }

        public ExternalizableWithResolveWithNull(int intValue) {
            super(intValue);
        }

        private Object readResolve() {
            return null;
        }
    }

    private static class ExternalizableWithWriteReplaceChain1 extends SimpleExternalizable {
        public ExternalizableWithWriteReplaceChain1() {
        }

        public ExternalizableWithWriteReplaceChain1(int value) {
            super(value);
        }

        private Object writeReplace() {
            return new ExternalizableWithWriteReplaceChain2(intValue + 1);
        }
    }

    private static class ExternalizableWithWriteReplaceChain2 extends SimpleExternalizable {
        public ExternalizableWithWriteReplaceChain2() {
        }

        public ExternalizableWithWriteReplaceChain2(int value) {
            super(value);
        }

        private Object writeReplace() {
            return intValue + 2;
        }
    }

    private static class ExternalizableWithWriteReplaceWithSameClass extends SimpleExternalizable {
        public ExternalizableWithWriteReplaceWithSameClass() {
        }

        public ExternalizableWithWriteReplaceWithSameClass(int value) {
            super(value);
        }

        private Object writeReplace() {
            return new ExternalizableWithWriteReplaceWithSameClass(intValue + 1);
        }
    }

    private static class ExternalizableWithWriteReplaceCycle1 extends SimpleExternalizable {
        public ExternalizableWithWriteReplaceCycle1() {
        }

        public ExternalizableWithWriteReplaceCycle1(int intValue) {
            super(intValue);
        }

        private Object writeReplace() {
            return new ExternalizableWithWriteReplaceCycle2(intValue);
        }
    }

    private static class ExternalizableWithWriteReplaceCycle2 extends SimpleExternalizable {
        public ExternalizableWithWriteReplaceCycle2() {
        }

        public ExternalizableWithWriteReplaceCycle2(int intValue) {
            super(intValue);
        }

        private Object writeReplace() {
            return new ExternalizableWithWriteReplaceCycle1(intValue);
        }
    }

    private static class ExternalizableWithReadResolveChain1 extends SimpleExternalizable {
        public ExternalizableWithReadResolveChain1() {
        }

        public ExternalizableWithReadResolveChain1(int value) {
            super(value);
        }

        private Object readResolve() {
            return new ExternalizableWithReadResolveChain2(intValue + 1);
        }
    }

    private static class ExternalizableWithReadResolveChain2 extends SimpleExternalizable {
        public ExternalizableWithReadResolveChain2() {
        }

        public ExternalizableWithReadResolveChain2(int value) {
            super(value);
        }

        private Object readResolve() {
            return intValue + 2;
        }
    }

    private static class ExternalizableWithDefaultWriteObjectCallInWriteObjectMethod implements Externalizable {
        public ExternalizableWithDefaultWriteObjectCallInWriteObjectMethod() {
        }

        /** {@inheritDoc} */
        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            ObjectOutputStream stream = (ObjectOutputStream) out;
            stream.defaultWriteObject();
        }

        /** {@inheritDoc} */
        @Override
        public void readExternal(ObjectInput in) {
            // no op
        }
    }

    private static class ExternalizableWithDefaultReadObjectCallInReadObjectMethod implements Externalizable {
        public ExternalizableWithDefaultReadObjectCallInReadObjectMethod() {
        }

        /** {@inheritDoc} */
        @Override
        public void writeExternal(ObjectOutput out) {
            // no op
        }

        /** {@inheritDoc} */
        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            ObjectInputStream stream = (ObjectInputStream) in;
            stream.defaultReadObject();
        }
    }

    private static class SerializableWithDefaultReadWriteObjectCallInReadWriteOverride implements Serializable {
        @SuppressWarnings({"FieldCanBeLocal", "unused"})
        private final Object object;

        public SerializableWithDefaultReadWriteObjectCallInReadWriteOverride(Object object) {
            this.object = object;
        }

        private void writeObject(ObjectOutputStream stream) throws IOException {
            stream.defaultWriteObject();
        }

        private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
            stream.defaultReadObject();
        }
    }

    private static class ExternalizableWritingAndReadingObject implements Externalizable {
        private IntHolder intHolder;

        public ExternalizableWritingAndReadingObject() {
        }

        public ExternalizableWritingAndReadingObject(IntHolder intHolder) {
            this.intHolder = intHolder;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(intHolder);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            intHolder = (IntHolder) in.readObject();
        }
    }

    private static class WithSideEffectInConstructor implements Externalizable {
        public WithSideEffectInConstructor() {
            constructorCalled = true;
        }

        @Override
        public void writeExternal(ObjectOutput out) {
            // no-op
        }

        @Override
        public void readExternal(ObjectInput in) {
            // no-op
        }
    }
}
