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

package org.apache.ignite.internal.binary;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryWriter;
import org.jetbrains.annotations.Nullable;

/**
 * Writer for meta data collection.
 */
class BinaryMetadataCollector implements BinaryWriter {
    /** Type ID. */
    private final int typeId;

    /** Type name. */
    private final String typeName;

    /** Name mapper. */
    private final BinaryInternalMapper mapper;

    /** Collected metadata. */
    private final Map<String, BinaryFieldMetadata> meta = new HashMap<>();

    /** Schema builder. */
    private BinarySchema.Builder schemaBuilder = BinarySchema.Builder.newBuilder();

    /**
     * Constructor.
     *
     * @param typeId Type ID.
     * @param typeName Type name.
     * @param mapper Name mapper.
     */
    BinaryMetadataCollector(int typeId, String typeName, BinaryInternalMapper mapper) {
        this.typeId = typeId;
        this.typeName = typeName;
        this.mapper = mapper;
    }

    /**
     * @return Field meta data.
     */
    Map<String, BinaryFieldMetadata> meta() {
        return meta;
    }

    /**
     * @return Schemas.
     */
    BinarySchema schema() {
        return schemaBuilder.build();
    }

    /** {@inheritDoc} */
    @Override public void writeByte(String fieldName, byte val) throws BinaryObjectException {
        add(fieldName, BinaryWriteMode.BYTE);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(String fieldName, short val) throws BinaryObjectException {
        add(fieldName, BinaryWriteMode.SHORT);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(String fieldName, int val) throws BinaryObjectException {
        add(fieldName, BinaryWriteMode.INT);
    }

    /** {@inheritDoc} */
    @Override public void writeLong(String fieldName, long val) throws BinaryObjectException {
        add(fieldName, BinaryWriteMode.LONG);
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(String fieldName, float val) throws BinaryObjectException {
        add(fieldName, BinaryWriteMode.FLOAT);
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(String fieldName, double val) throws BinaryObjectException {
        add(fieldName, BinaryWriteMode.DOUBLE);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(String fieldName, char val) throws BinaryObjectException {
        add(fieldName, BinaryWriteMode.CHAR);
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(String fieldName, boolean val) throws BinaryObjectException {
        add(fieldName, BinaryWriteMode.BOOLEAN);
    }

    /** {@inheritDoc} */
    @Override public void writeDecimal(String fieldName, @Nullable BigDecimal val) throws BinaryObjectException {
        add(fieldName, BinaryWriteMode.DECIMAL);
    }

    /** {@inheritDoc} */
    @Override public void writeString(String fieldName, @Nullable String val) throws BinaryObjectException {
        add(fieldName, BinaryWriteMode.STRING);
    }

    /** {@inheritDoc} */
    @Override public void writeUuid(String fieldName, @Nullable UUID val) throws BinaryObjectException {
        add(fieldName, BinaryWriteMode.UUID);
    }

    /** {@inheritDoc} */
    @Override public void writeDate(String fieldName, @Nullable Date val) throws BinaryObjectException {
        add(fieldName, BinaryWriteMode.DATE);
    }

    /** {@inheritDoc} */
    @Override public void writeTimestamp(String fieldName, @Nullable Timestamp val) throws BinaryObjectException {
        add(fieldName, BinaryWriteMode.TIMESTAMP);
    }

    /** {@inheritDoc} */
    @Override public void writeTime(String fieldName, @Nullable Time val) throws BinaryObjectException {
        add(fieldName, BinaryWriteMode.TIME);
    }

    /** {@inheritDoc} */
    @Override public <T extends Enum<?>> void writeEnum(String fieldName, T val) throws BinaryObjectException {
        add(fieldName, BinaryWriteMode.ENUM);
    }

    /** {@inheritDoc} */
    @Override public <T extends Enum<?>> void writeEnumArray(String fieldName, T[] val) throws BinaryObjectException {
        add(fieldName, BinaryWriteMode.ENUM_ARR);
    }

    /** {@inheritDoc} */
    @Override public void writeObject(String fieldName, @Nullable Object obj) throws BinaryObjectException {
        add(fieldName, BinaryWriteMode.OBJECT);
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(String fieldName, @Nullable byte[] val) throws BinaryObjectException {
        add(fieldName, BinaryWriteMode.BYTE_ARR);
    }

    /** {@inheritDoc} */
    @Override public void writeShortArray(String fieldName, @Nullable short[] val) throws BinaryObjectException {
        add(fieldName, BinaryWriteMode.SHORT_ARR);
    }

    /** {@inheritDoc} */
    @Override public void writeIntArray(String fieldName, @Nullable int[] val) throws BinaryObjectException {
        add(fieldName, BinaryWriteMode.INT_ARR);
    }

    /** {@inheritDoc} */
    @Override public void writeLongArray(String fieldName, @Nullable long[] val) throws BinaryObjectException {
        add(fieldName, BinaryWriteMode.LONG_ARR);
    }

    /** {@inheritDoc} */
    @Override public void writeFloatArray(String fieldName, @Nullable float[] val) throws BinaryObjectException {
        add(fieldName, BinaryWriteMode.FLOAT_ARR);
    }

    /** {@inheritDoc} */
    @Override public void writeDoubleArray(String fieldName, @Nullable double[] val) throws BinaryObjectException {
        add(fieldName, BinaryWriteMode.DOUBLE_ARR);
    }

    /** {@inheritDoc} */
    @Override public void writeCharArray(String fieldName, @Nullable char[] val) throws BinaryObjectException {
        add(fieldName, BinaryWriteMode.CHAR_ARR);
    }

    /** {@inheritDoc} */
    @Override public void writeBooleanArray(String fieldName, @Nullable boolean[] val) throws BinaryObjectException {
        add(fieldName, BinaryWriteMode.BOOLEAN_ARR);
    }

    /** {@inheritDoc} */
    @Override public void writeDecimalArray(String fieldName, @Nullable BigDecimal[] val) throws BinaryObjectException {
        add(fieldName, BinaryWriteMode.DECIMAL_ARR);
    }

    /** {@inheritDoc} */
    @Override public void writeStringArray(String fieldName, @Nullable String[] val) throws BinaryObjectException {
        add(fieldName, BinaryWriteMode.STRING_ARR);
    }

    /** {@inheritDoc} */
    @Override public void writeUuidArray(String fieldName, @Nullable UUID[] val) throws BinaryObjectException {
        add(fieldName, BinaryWriteMode.UUID_ARR);
    }

    /** {@inheritDoc} */
    @Override public void writeDateArray(String fieldName, @Nullable Date[] val) throws BinaryObjectException {
        add(fieldName, BinaryWriteMode.DATE_ARR);
    }

    /** {@inheritDoc} */
    @Override public void writeTimestampArray(String fieldName, @Nullable Timestamp[] val) throws BinaryObjectException {
        add(fieldName, BinaryWriteMode.TIMESTAMP_ARR);
    }

    /** {@inheritDoc} */
    @Override public void writeTimeArray(String fieldName, @Nullable Time[] val) throws BinaryObjectException {
        add(fieldName, BinaryWriteMode.TIME_ARR);
    }

    /** {@inheritDoc} */
    @Override public void writeObjectArray(String fieldName, @Nullable Object[] val) throws BinaryObjectException {
        add(fieldName, BinaryWriteMode.OBJECT_ARR);
    }

    /** {@inheritDoc} */
    @Override public <T> void writeCollection(String fieldName, @Nullable Collection<T> col)
        throws BinaryObjectException {
        add(fieldName, BinaryWriteMode.COL);
    }

    /** {@inheritDoc} */
    @Override public <K, V> void writeMap(String fieldName, @Nullable Map<K, V> map) throws BinaryObjectException {
        add(fieldName, BinaryWriteMode.MAP);
    }

    /** {@inheritDoc} */
    @Override public BinaryRawWriter rawWriter() {
        return (BinaryRawWriter)Proxy.newProxyInstance(getClass().getClassLoader(),
            new Class<?>[] { BinaryRawWriterEx.class },
            new InvocationHandler() {
                @Override public Object invoke(Object proxy, Method mtd, Object[] args) throws Throwable {
                    return null;
                }
            });
    }

    /**
     * @param name Field name.
     * @param mode Field mode.
     * @throws BinaryObjectException In case of error.
     */
    private void add(String name, BinaryWriteMode mode) throws BinaryObjectException {
        assert name != null;

        int typeId = mode.typeId();
        int fieldId = mapper.fieldId(typeId, name);

        BinaryFieldMetadata oldFieldMeta = meta.put(name, new BinaryFieldMetadata(typeId, fieldId));

        if (oldFieldMeta != null && oldFieldMeta.typeId() != typeId) {
            throw new BinaryObjectException(
                "Field is written twice with different types [" + "typeName=" + typeName + ", fieldName=" + name +
                ", fieldTypeName1=" + BinaryUtils.fieldTypeName(oldFieldMeta.typeId()) +
                ", fieldTypeName2=" + BinaryUtils.fieldTypeName(typeId) + ']'
            );
        }

        schemaBuilder.addField(fieldId);
    }
}
