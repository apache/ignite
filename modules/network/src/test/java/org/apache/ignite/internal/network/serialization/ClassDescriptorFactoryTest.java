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

package org.apache.ignite.internal.network.serialization;

import static org.apache.ignite.internal.network.serialization.SerializationType.ARBITRARY;
import static org.apache.ignite.internal.network.serialization.SerializationType.EXTERNALIZABLE;
import static org.apache.ignite.internal.network.serialization.SerializationType.SERIALIZABLE;
import static org.apache.ignite.internal.network.serialization.SerializationType.SERIALIZABLE_OVERRIDE;
import static org.apache.ignite.internal.network.serialization.SerializationType.SERIALIZABLE_READ_RESOLVE;
import static org.apache.ignite.internal.network.serialization.SerializationType.SERIALIZABLE_WRITE_REPLACE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.Test;

/**
 * Set of tests for the class descriptor factory.
 */
public class ClassDescriptorFactoryTest {
    /**
     * Descriptor parser context.
     */
    private final ClassDescriptorFactoryContext context = new ClassDescriptorFactoryContext();

    /**
     * Descriptor factory.
     */
    private final ClassDescriptorFactory factory = new ClassDescriptorFactory(context);

    private static class SerializableClass implements Serializable {
        private static final long serialVersionUID = 0L;
    }

    @Test
    public void testSerializable() {
        ClassDescriptor descriptor = factory.create(SerializableClass.class);

        checkSerializable(descriptor.serializationType(), false, false, false);
        assertEquals(0, descriptor.fields().size());
    }

    private static class SerializableOverride implements Serializable {
        private static final long serialVersionUID = 0L;

        private void writeObject(ObjectOutputStream out) throws IOException {
            // No-op.
        }

        private void readObject(ObjectInputStream in)
                throws IOException, ClassNotFoundException {
            // No-op.
        }

        private void readObjectNoData() throws ObjectStreamException {
            // No-op.
        }
    }

    @Test
    public void testSerializableOverride() {
        ClassDescriptor descriptor = factory.create(SerializableOverride.class);

        checkSerializable(descriptor.serializationType(), true, false, false);
        assertEquals(0, descriptor.fields().size());
    }

    private static class SerializableOverrideWriteReplace implements Serializable {
        private static final long serialVersionUID = 0L;

        private void writeObject(ObjectOutputStream out) throws IOException {
            // No-op.
        }

        private void readObject(ObjectInputStream in)
                throws IOException, ClassNotFoundException {
            // No-op.
        }

        private void readObjectNoData() throws ObjectStreamException {
            // No-op.
        }

        private Object writeReplace() throws ObjectStreamException {
            // No-op.
            return null;
        }
    }

    @Test
    public void testSerializableOverrideWriteReplace() {
        ClassDescriptor descriptor = factory.create(SerializableOverrideWriteReplace.class);

        checkSerializable(descriptor.serializationType(), true, true, false);
        assertEquals(0, descriptor.fields().size());
    }

    private static class SerializableOverrideReadResolve implements Serializable {
        private static final long serialVersionUID = 0L;

        private void writeObject(ObjectOutputStream out) throws IOException {
            // No-op.
        }

        private void readObject(ObjectInputStream in)
                throws IOException, ClassNotFoundException {
            // No-op.
        }

        private void readObjectNoData() throws ObjectStreamException {
            // No-op.
        }

        private Object readResolve() throws ObjectStreamException {
            // No-op.
            return null;
        }
    }

    @Test
    public void testSerializableOverrideReadResolve() {
        ClassDescriptor descriptor = factory.create(SerializableOverrideReadResolve.class);

        checkSerializable(descriptor.serializationType(), true, false, true);
        assertEquals(0, descriptor.fields().size());
    }

    private static class SerializableOverrideWriteReplaceReadResolve implements Serializable {
        private static final long serialVersionUID = 0L;

        private void writeObject(ObjectOutputStream out) throws IOException {
            // No-op.
        }

        private void readObject(ObjectInputStream in)
                throws IOException, ClassNotFoundException {
            // No-op.
        }

        private void readObjectNoData() throws ObjectStreamException {
            // No-op.
        }

        private Object writeReplace() throws ObjectStreamException {
            // No-op.
            return null;
        }

        private Object readResolve() throws ObjectStreamException {
            // No-op.
            return null;
        }
    }

    @Test
    public void testSerializableOverrideWriteReplaceReadResolve() {
        ClassDescriptor descriptor = factory.create(
                SerializableOverrideWriteReplaceReadResolve.class);

        checkSerializable(descriptor.serializationType(), true, true, true);
        assertEquals(0, descriptor.fields().size());
    }

    private static class SerializableWriteReplace implements Serializable {
        private static final long serialVersionUID = 0L;

        private Object writeReplace() throws ObjectStreamException {
            // No-op.
            return null;
        }
    }

    @Test
    public void testSerializableWriteReplace() {
        ClassDescriptor descriptor = factory.create(SerializableWriteReplace.class);

        checkSerializable(descriptor.serializationType(), false, true, false);
        assertEquals(0, descriptor.fields().size());
    }

    @SuppressWarnings("CheckStyle")
    private static class SerializableReadResolve implements Serializable {
        private static final long serialVersionUID = 0L;

        private Object readResolve() throws ObjectStreamException {
            // No-op.
            return null;
        }
    }

    @Test
    public void testSerializableReadResolve() {
        ClassDescriptor descriptor = factory.create(SerializableReadResolve.class);

        checkSerializable(descriptor.serializationType(), false, false, true);
        assertEquals(0, descriptor.fields().size());
    }

    private static class SerializableWriteReplaceReadResolve implements Serializable {
        private static final long serialVersionUID = 0L;

        private Object writeReplace() throws ObjectStreamException {
            // No-op.
            return null;
        }

        private Object readResolve() throws ObjectStreamException {
            // No-op.
            return null;
        }
    }

    @Test
    public void testSerializableWriteReplaceReadResolve() {
        ClassDescriptor descriptor = factory.create(SerializableWriteReplaceReadResolve.class);

        checkSerializable(descriptor.serializationType(), false, true, true);
        assertEquals(0, descriptor.fields().size());
    }

    @SuppressWarnings("RedundantThrows")
    private static class ExternalizableClass implements Externalizable {
        public ExternalizableClass() {
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            // No-op.
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            // No-op.
        }
    }

    @Test
    public void testExternalizable() {
        ClassDescriptor descriptor = factory.create(ExternalizableClass.class);

        checkExternalizable(descriptor.serializationType());
        assertEquals(0, descriptor.fields().size());
    }

    private static class ArbitraryClass {
    }

    @Test
    public void testArbitrary() {
        ClassDescriptor descriptor = factory.create(ArbitraryClass.class);

        checkArbitraryType(descriptor.serializationType());
        assertEquals(0, descriptor.fields().size());
    }

    @SuppressWarnings({"ExternalizableWithoutPublicNoArgConstructor", "RedundantThrows"})
    private static class ExternalizableWithoutPublicNoArgConstructor implements Externalizable {
        public ExternalizableWithoutPublicNoArgConstructor(@SuppressWarnings("unused") int unused) {
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            // No-op.
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            // No-op.
        }
    }

    @Test
    public void testExternalizableWithoutConstructor() {
        assertThrows(IgniteException.class,
                () -> factory.create(ExternalizableWithoutPublicNoArgConstructor.class));
    }

    /** Class that holds a set of all types parseable by the class descriptor parser. */
    @SuppressWarnings("unused")
    private static class Holder {
        private SerializableClass field1;

        private ExternalizableClass field2;

        private ArbitraryClass field3;

        private int primitiveField;
    }

    @Test
    public void testHolderClass() {
        ClassDescriptor holderDescriptor = factory.create(Holder.class);

        ClassDescriptor serializableDescriptor = context.getDescriptor(SerializableClass.class);
        assertNotNull(serializableDescriptor);

        ClassDescriptor externalizableDescriptor = context.getDescriptor(ExternalizableClass.class);
        assertNotNull(externalizableDescriptor);

        ClassDescriptor arbitraryDescriptor = context.getDescriptor(ArbitraryClass.class);
        assertNotNull(arbitraryDescriptor);

        ClassDescriptor intDescriptor = context.getDescriptor(DefaultType.INT.descriptorId());
        assertNotNull(intDescriptor);

        List<FieldDescriptor> fields = holderDescriptor.fields();
        assertEquals(4, fields.size());

        Map<String, FieldDescriptor> fieldsMap = fields.stream().collect(Collectors.toMap(FieldDescriptor::name, Function.identity()));

        FieldDescriptor field1 = fieldsMap.get("field1");
        assertNotNull(field1);
        assertEquals(serializableDescriptor.descriptorId(), field1.typeDescriptorId());

        FieldDescriptor field2 = fieldsMap.get("field2");
        assertNotNull(field2);
        assertEquals(externalizableDescriptor.descriptorId(), field2.typeDescriptorId());

        FieldDescriptor field3 = fieldsMap.get("field3");
        assertNotNull(field3);
        assertEquals(arbitraryDescriptor.descriptorId(), field3.typeDescriptorId());

        FieldDescriptor primitiveField = fieldsMap.get("primitiveField");
        assertNotNull(primitiveField);
        assertEquals(intDescriptor.descriptorId(), primitiveField.typeDescriptorId());
    }

    @Test
    public void testDefaultType() {
        ClassDescriptor descriptor = context.getDescriptor(int.class);

        assertNotNull(descriptor);
        checkDefaultType(descriptor.serializationType());
    }

    private static class SerializableWithWrongSignatures implements Serializable {
        private static final long serialVersionUID = 0L;

        private void writeObject(ObjectOutputStream out) {
            // No-op.
        }

        private void readObject(ObjectInputStream in) {
            // No-op.
        }

        private void readObjectNoData() {
            // No-op.
        }

        private Object writeReplace() {
            // No-op.
            return null;
        }

        private Object readResolve() {
            // No-op.
            return null;
        }
    }

    @Test
    public void testSerializableWithIncorrectMethodSignatures() {
        ClassDescriptor descriptor = factory.create(SerializableWithWrongSignatures.class);

        assertNotNull(descriptor);
        checkSerializable(descriptor.serializationType(), false, false, false);
    }

    /**
     * Checks that serialization type is {@link SerializationType#ARBITRARY}.
     *
     * @param type Serialization type.
     */
    private void checkArbitraryType(int type) {
        assertEquals(ARBITRARY, type & ARBITRARY);
        assertEquals(0, type & SERIALIZABLE);
        assertEquals(0, type & SERIALIZABLE_OVERRIDE);
        assertEquals(0, type & SERIALIZABLE_WRITE_REPLACE);
        assertEquals(0, type & SERIALIZABLE_READ_RESOLVE);
        assertEquals(0, type & EXTERNALIZABLE);
    }

    /**
     * Checks that serialization type is {@link SerializationType#DEFAULT}.
     *
     * @param type Serialization type.
     */
    private void checkDefaultType(int type) {
        assertEquals(0, type);
    }

    /**
     * Checks that serialization type is {@link SerializationType#EXTERNALIZABLE}.
     *
     * @param type Serialization type.
     */
    private void checkExternalizable(int type) {
        assertEquals(0, type & ARBITRARY);
        assertEquals(0, type & SERIALIZABLE);
        assertEquals(0, type & SERIALIZABLE_OVERRIDE);
        assertEquals(0, type & SERIALIZABLE_WRITE_REPLACE);
        assertEquals(0, type & SERIALIZABLE_READ_RESOLVE);
        assertEquals(EXTERNALIZABLE, type & EXTERNALIZABLE);
    }

    /**
     * Checks that serialization type is {@link SerializationType#SERIALIZABLE} and has specific flags.
     *
     * @param type         Serialization type.
     * @param override     Has readObject/writeObject/readObjectNoData methods.
     * @param writeReplace Has writeReplace method.
     * @param readResolve  Has readResolve method.
     */
    private void checkSerializable(int type, boolean override, boolean writeReplace, boolean readResolve) {
        assertEquals(0, type & ARBITRARY);
        assertEquals(SERIALIZABLE, type & SERIALIZABLE);
        assertEquals(override ? SERIALIZABLE_OVERRIDE : 0, type & SERIALIZABLE_OVERRIDE);
        assertEquals(writeReplace ? SERIALIZABLE_WRITE_REPLACE : 0, type & SERIALIZABLE_WRITE_REPLACE);
        assertEquals(readResolve ? SERIALIZABLE_READ_RESOLVE : 0, type & SERIALIZABLE_READ_RESOLVE);
        assertEquals(0, type & EXTERNALIZABLE);
    }
}
