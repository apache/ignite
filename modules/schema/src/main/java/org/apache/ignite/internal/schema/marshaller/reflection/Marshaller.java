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

package org.apache.ignite.internal.schema.marshaller.reflection;

import java.util.Objects;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.marshaller.BinaryMode;
import org.apache.ignite.internal.schema.marshaller.MarshallerException;
import org.apache.ignite.internal.schema.marshaller.MarshallerUtil;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.util.Factory;
import org.apache.ignite.internal.util.ObjectFactory;
import org.apache.ignite.table.mapper.Mapper;
import org.jetbrains.annotations.Nullable;

/**
 * Marshaller.
 */
public abstract class Marshaller {
    /**
     * Creates a marshaller for class.
     *
     * @param cols   Columns.
     * @param mapper Mapper.
     * @return Marshaller.
     */
    public static <T> Marshaller createMarshaller(Column[] cols, Mapper<T> mapper) {
        final BinaryMode mode = MarshallerUtil.mode(mapper.targetType());
        
        if (mode != null) {
            final Column col = cols[0];
            
            assert cols.length == 1;
            assert mode.typeSpec() == col.type().spec() : "Target type is not compatible.";
            assert !mapper.targetType().isPrimitive() : "Non-nullable types are not allowed.";
            
            return new SimpleMarshaller(FieldAccessor.createIdentityAccessor(col, col.schemaIndex(), mode));
        }
        
        FieldAccessor[] fieldAccessors = new FieldAccessor[cols.length];
        
        // Build handlers.
        for (int i = 0; i < cols.length; i++) {
            final Column col = cols[i];
            
            String fieldName = mapper.columnToField(col.name());
            
            // TODO: IGNITE-15785 validate key marshaller has no NoopAccessors.
            fieldAccessors[i] = (fieldName == null) ? FieldAccessor.noopAccessor(col) :
                    FieldAccessor.create(mapper.targetType(), fieldName, col, col.schemaIndex());
        }
        
        return new PojoMarshaller(new ObjectFactory<>(mapper.targetType()), fieldAccessors);
    }
    
    /**
     * Creates a marshaller for class.
     *
     * @param cols Columns.
     * @param cls  Type.
     * @return Marshaller.
     */
    //TODO: IGNITE-15907 drop
    @Deprecated
    public static Marshaller createMarshaller(Columns cols, Class<? extends Object> cls) {
        final BinaryMode mode = MarshallerUtil.mode(cls);
        
        if (mode != null) {
            final Column col = cols.column(0);
            
            assert cols.length() == 1;
            assert mode.typeSpec() == col.type().spec() : "Target type is not compatible.";
            assert !cls.isPrimitive() : "Non-nullable types are not allowed.";
            
            return new SimpleMarshaller(FieldAccessor.createIdentityAccessor(col, col.schemaIndex(), mode));
        }
        
        FieldAccessor[] fieldAccessors = new FieldAccessor[cols.length()];
        
        // Build accessors
        for (int i = 0; i < cols.length(); i++) {
            final Column col = cols.column(i);
            
            fieldAccessors[i] = FieldAccessor.create(cls, col.name(), col, col.schemaIndex());
        }
        
        return new PojoMarshaller(new ObjectFactory<>(cls), fieldAccessors);
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
     * @return Object.
     * @throws MarshallerException If failed.
     */
    public abstract Object readObject(Row reader) throws MarshallerException;
    
    /**
     * Write an object to a row.
     *
     * @param obj    Object.
     * @param writer Row writer.
     * @throws MarshallerException If failed.
     */
    public abstract void writeObject(Object obj, RowAssembler writer) throws MarshallerException;
    
    /**
     * Marshaller for objects of natively supported types.
     */
    static class SimpleMarshaller extends Marshaller {
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
        public Object readObject(Row reader) {
            return fieldAccessor.read(reader);
        }
        
        
        /** {@inheritDoc} */
        @Override
        public void writeObject(Object obj, RowAssembler writer) throws MarshallerException {
            fieldAccessor.write(writer, obj);
        }
    }
    
    /**
     * Marshaller for POJOs.
     */
    static class PojoMarshaller extends Marshaller {
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
        @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
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
        public Object readObject(Row reader) throws MarshallerException {
            final Object obj = factory.create();
            
            for (int fldIdx = 0; fldIdx < fieldAccessors.length; fldIdx++) {
                fieldAccessors[fldIdx].read(reader, obj);
            }
            
            return obj;
        }
        
        /** {@inheritDoc} */
        @Override
        public void writeObject(Object obj, RowAssembler writer)
                throws MarshallerException {
            for (int fldIdx = 0; fldIdx < fieldAccessors.length; fldIdx++) {
                fieldAccessors[fldIdx].write(writer, obj);
            }
        }
    }
}
