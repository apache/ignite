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
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.schema.marshaller.BinaryMode;
import org.apache.ignite.internal.schema.marshaller.MarshallerUtil;
import org.apache.ignite.internal.schema.marshaller.SerializationException;
import org.apache.ignite.internal.util.Factory;
import org.apache.ignite.internal.util.ObjectFactory;
import org.jetbrains.annotations.Nullable;

/**
 * Marshaller.
 */
class Marshaller {
    /**
     * Creates marshaller for class.
     *
     * @param cols Columns.
     * @param firstColId First column position in schema.
     * @param aClass Type.
     * @return Marshaller.
     */
    static Marshaller createMarshaller(Columns cols, int firstColId, Class<? extends Object> aClass) {
        final BinaryMode mode = MarshallerUtil.mode(aClass);

        if (mode != null) {
            final Column col = cols.column(0);

            assert cols.length() == 1;
            assert mode.typeSpec() == col.type().spec() : "Target type is not compatible.";
            assert !aClass.isPrimitive() : "Non-nullable types are not allowed.";

            return new Marshaller(FieldAccessor.createIdentityAccessor(col, firstColId, mode));
        }

        FieldAccessor[] fieldAccessors = new FieldAccessor[cols.length()];

        // Build accessors
        for (int i = 0; i < cols.length(); i++) {
            final Column col = cols.column(i);

            final int colIdx = firstColId + i; /* Absolute column idx in schema. */
            fieldAccessors[i] = FieldAccessor.create(aClass, col, colIdx);
        }

        return new Marshaller(new ObjectFactory<>(aClass), fieldAccessors);
    }

    /**
     * Field accessors for mapped columns.
     * Array has same size and order as columns.
     */
    private final FieldAccessor[] fieldAccessors;

    /**
     * Object factory for complex types or {@code null} for basic type.
     */
    private final Factory<?> factory;

    /**
     * Constructor.
     * Creates marshaller for complex types.
     *
     * @param factory Object factory.
     * @param fieldAccessors Object field accessors for mapped columns.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    Marshaller(Factory<?> factory, FieldAccessor[] fieldAccessors) {
        this.fieldAccessors = fieldAccessors;
        this.factory = Objects.requireNonNull(factory);
    }

    /**
     * Constructor.
     * Creates marshaller for basic types.
     *
     * @param fieldAccessor Identity field accessor for object of basic type.
     */
     Marshaller(FieldAccessor fieldAccessor) {
        fieldAccessors = new FieldAccessor[] {fieldAccessor};
        factory = null;
    }

    /**
     * Reads object field.
     *
     * @param obj Object.
     * @param fldIdx Field index.
     * @return Field value.
     */
    public @Nullable Object value(Object obj, int fldIdx) {
        return fieldAccessors[fldIdx].value(obj);
    }

    /**
     * Reads object from row.
     *
     * @param reader Row reader.
     * @return Object.
     * @throws SerializationException If failed.
     */
    public Object readObject(Row reader) throws SerializationException {
        if (isSimpleTypeMarshaller())
            return fieldAccessors[0].read(reader);

        final Object obj = factory.create();

        for (int fldIdx = 0; fldIdx < fieldAccessors.length; fldIdx++)
            fieldAccessors[fldIdx].read(reader, obj);

        return obj;
    }

    /**
     * Write an object to row.
     *
     * @param obj Object.
     * @param writer Row writer.
     * @throws SerializationException If failed.
     */
    public void writeObject(Object obj, RowAssembler writer) throws SerializationException {
        for (int fldIdx = 0; fldIdx < fieldAccessors.length; fldIdx++)
            fieldAccessors[fldIdx].write(writer, obj);
    }

    /**
     * @return {@code true} if it is marshaller for simple type, {@code false} otherwise.
     */
    private boolean isSimpleTypeMarshaller() {
        return factory == null;
    }
}
