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

package org.apache.ignite.internal.marshaller;

import java.util.Objects;
import org.apache.ignite.internal.util.Factory;
import org.apache.ignite.internal.util.ObjectFactory;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.mapper.OneColumnMapper;
import org.apache.ignite.table.mapper.PojoMapper;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Marshaller.
 * TODO: Reuse this code in ignite-schema module (IGNITE-16088).
 */
public abstract class Marshaller {
    /**
     * Creates a marshaller for class.
     *
     * @param cols             Columns.
     * @param mapper           Mapper.
     * @param requireAllFields If specified class should contain fields for all columns.
     * @return Marshaller.
     */
    public static <T> Marshaller createMarshaller(MarshallerColumn[] cols, @NotNull Mapper<T> mapper, boolean requireAllFields) {
        if (mapper instanceof OneColumnMapper) {
            return simpleMarshaller(cols, (OneColumnMapper<T>) mapper);
        } else if (mapper instanceof PojoMapper) {
            return pojoMarshaller(cols, (PojoMapper<T>) mapper, requireAllFields);
        } else {
            throw new IllegalArgumentException("Mapper of unsupported type: " + mapper.getClass());
        }
    }

    /**
     * Creates a marshaller for class.
     *
     * @param cols   Columns.
     * @param mapper Mapper.
     * @return Marshaller.
     */
    static <T> SimpleMarshaller simpleMarshaller(MarshallerColumn[] cols, @NotNull OneColumnMapper<T> mapper) {
        final BinaryMode mode = MarshallerUtil.mode(mapper.targetType());

        final MarshallerColumn col = cols[0];

        assert cols.length == 1;
        assert mode == col.type() : "Target type is not compatible.";
        assert !mapper.targetType().isPrimitive() : "Non-nullable types are not allowed.";

        return new SimpleMarshaller(FieldAccessor.createIdentityAccessor(col.name(), 0, mode));
    }

    /**
     * Creates a pojo marshaller for class.
     *
     * @param cols             Columns.
     * @param mapper           Mapper.
     * @param requireAllFields If specified class should contain fields for all columns.
     * @return Pojo marshaller.
     */
    static <T> PojoMarshaller pojoMarshaller(MarshallerColumn[] cols, @NotNull PojoMapper<T> mapper, boolean requireAllFields) {
        FieldAccessor[] fieldAccessors = new FieldAccessor[cols.length];

        // Build handlers.
        for (int i = 0; i < cols.length; i++) {
            final MarshallerColumn col = cols[i];

            String fieldName = mapper.fieldForColumn(col.name());

            if (requireAllFields && fieldName == null) {
                throw new IllegalArgumentException("No field found for column " + col.name());
            }

            fieldAccessors[i] = (fieldName == null) ? FieldAccessor.noopAccessor(col) :
                    FieldAccessor.create(mapper.targetType(), fieldName, col, i);
        }

        return new PojoMarshaller(new ObjectFactory<>(mapper.targetType()), fieldAccessors);
    }

    /**
     * Reads object field value.
     *
     * @param obj    Object to read from.
     * @param fldIdx Field index.
     * @return Field value.
     */
    public abstract @Nullable Object value(Object obj, int fldIdx);

    /**
     * Reads object from a row.
     *
     * @param reader Row reader.
     * @param target Optional target object. When not specified, a new object will be created.
     * @return Object.
     * @throws MarshallerException If failed.
     */
    public abstract Object readObject(MarshallerReader reader, @Nullable Object target) throws MarshallerException;

    /**
     * Copies field values from one object to another.
     *
     * @param source Source.
     * @param target Target.
     * @throws MarshallerException If failed.
     */
    public abstract void copyObject(Object source, Object target) throws MarshallerException;

    /**
     * Write an object to a row.
     *
     * @param obj    Object.
     * @param writer Row writer.
     * @throws MarshallerException If failed.
     */
    public abstract void writeObject(Object obj, MarshallerWriter writer) throws MarshallerException;

    /**
     * Marshaller for objects of natively supported types.
     */
    private static class SimpleMarshaller extends Marshaller {
        /** Identity accessor. */
        private final FieldAccessor fieldAccessor;

        /**
         * Creates a marshaller for objects of natively supported type.
         *
         * @param fieldAccessor Identity field accessor for objects of natively supported type.
         */
        SimpleMarshaller(FieldAccessor fieldAccessor) {
            this.fieldAccessor = fieldAccessor;
        }

        /** {@inheritDoc} */
        @Override
        public @Nullable
        Object value(Object obj, int fldIdx) {
            assert fldIdx == 0;

            return fieldAccessor.value(obj);
        }

        /** {@inheritDoc} */
        @Override
        public Object readObject(MarshallerReader reader, Object target) {
            return fieldAccessor.read(reader);
        }

        /** {@inheritDoc} */
        @Override
        public void copyObject(Object source, Object target) throws MarshallerException {
            throw new UnsupportedOperationException("SimpleMarshaller can't copy objects.");
        }

        /** {@inheritDoc} */
        @Override
        public void writeObject(Object obj, MarshallerWriter writer) throws MarshallerException {
            fieldAccessor.write(writer, obj);
        }
    }

    /**
     * Marshaller for POJOs.
     */
    private static class PojoMarshaller extends Marshaller {
        /** Field accessors for mapped columns. Array has same size and order as columns. */
        private final FieldAccessor[] fieldAccessors;

        /** Object factory. */
        private final Factory<?> factory;

        /**
         * Creates a marshaller for POJOs.
         *
         * @param factory        Object factory.
         * @param fieldAccessors Object field accessors for mapped columns.
         */
        PojoMarshaller(Factory<?> factory, FieldAccessor[] fieldAccessors) {
            this.fieldAccessors = fieldAccessors;
            this.factory = Objects.requireNonNull(factory);
        }

        /** {@inheritDoc} */
        @Override
        public @Nullable
        Object value(Object obj, int fldIdx) {
            return fieldAccessors[fldIdx].value(obj);
        }

        /** {@inheritDoc} */
        @Override
        public Object readObject(MarshallerReader reader, Object target) throws MarshallerException {
            final Object obj = target == null ? factory.create() : target;

            for (int fldIdx = 0; fldIdx < fieldAccessors.length; fldIdx++) {
                fieldAccessors[fldIdx].read(reader, obj);
            }

            return obj;
        }

        /** {@inheritDoc} */
        @Override
        public void copyObject(Object source, Object target) throws MarshallerException {
            for (int fldIdx = 0; fldIdx < fieldAccessors.length; fldIdx++) {
                FieldAccessor fieldAccessor = fieldAccessors[fldIdx];
                var val = fieldAccessor.get(source);
                fieldAccessor.set(target, val);
            }
        }

        /** {@inheritDoc} */
        @Override
        public void writeObject(Object obj, MarshallerWriter writer) throws MarshallerException {
            for (int fldIdx = 0; fldIdx < fieldAccessors.length; fldIdx++) {
                fieldAccessors[fldIdx].write(writer, obj);
            }
        }
    }
}
