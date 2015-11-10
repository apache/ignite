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

package org.apache.ignite.internal.portable;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
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
    /** */
    private final Map<String, Integer> meta = new HashMap<>();

    /** */
    private final String typeName;

    /**
     * @param typeName Type name.
     */
    BinaryMetadataCollector(String typeName) {
        this.typeName = typeName;
    }

    /**
     * @return Field meta data.
     */
    Map<String, Integer> meta() {
        return meta;
    }

    /** {@inheritDoc} */
    @Override public void writeByte(String fieldName, byte val) throws BinaryObjectException {
        add(fieldName, PortableClassDescriptor.Mode.BYTE);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(String fieldName, short val) throws BinaryObjectException {
        add(fieldName, PortableClassDescriptor.Mode.SHORT);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(String fieldName, int val) throws BinaryObjectException {
        add(fieldName, PortableClassDescriptor.Mode.INT);
    }

    /** {@inheritDoc} */
    @Override public void writeLong(String fieldName, long val) throws BinaryObjectException {
        add(fieldName, PortableClassDescriptor.Mode.LONG);
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(String fieldName, float val) throws BinaryObjectException {
        add(fieldName, PortableClassDescriptor.Mode.FLOAT);
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(String fieldName, double val) throws BinaryObjectException {
        add(fieldName, PortableClassDescriptor.Mode.DOUBLE);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(String fieldName, char val) throws BinaryObjectException {
        add(fieldName, PortableClassDescriptor.Mode.CHAR);
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(String fieldName, boolean val) throws BinaryObjectException {
        add(fieldName, PortableClassDescriptor.Mode.BOOLEAN);
    }

    /** {@inheritDoc} */
    @Override public void writeDecimal(String fieldName, @Nullable BigDecimal val) throws BinaryObjectException {
        add(fieldName, PortableClassDescriptor.Mode.DECIMAL);
    }

    /** {@inheritDoc} */
    @Override public void writeString(String fieldName, @Nullable String val) throws BinaryObjectException {
        add(fieldName, PortableClassDescriptor.Mode.STRING);
    }

    /** {@inheritDoc} */
    @Override public void writeUuid(String fieldName, @Nullable UUID val) throws BinaryObjectException {
        add(fieldName, PortableClassDescriptor.Mode.UUID);
    }

    /** {@inheritDoc} */
    @Override public void writeDate(String fieldName, @Nullable Date val) throws BinaryObjectException {
        add(fieldName, PortableClassDescriptor.Mode.DATE);
    }

    /** {@inheritDoc} */
    @Override public void writeTimestamp(String fieldName, @Nullable Timestamp val) throws BinaryObjectException {
        add(fieldName, PortableClassDescriptor.Mode.TIMESTAMP);
    }

    /** {@inheritDoc} */
    @Override public <T extends Enum<?>> void writeEnum(String fieldName, T val) throws BinaryObjectException {
        add(fieldName, PortableClassDescriptor.Mode.ENUM);
    }

    /** {@inheritDoc} */
    @Override public <T extends Enum<?>> void writeEnumArray(String fieldName, T[] val) throws BinaryObjectException {
        add(fieldName, PortableClassDescriptor.Mode.ENUM_ARR);
    }

    /** {@inheritDoc} */
    @Override public void writeObject(String fieldName, @Nullable Object obj) throws BinaryObjectException {
        add(fieldName, PortableClassDescriptor.Mode.OBJECT);
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(String fieldName, @Nullable byte[] val) throws BinaryObjectException {
        add(fieldName, PortableClassDescriptor.Mode.BYTE_ARR);
    }

    /** {@inheritDoc} */
    @Override public void writeShortArray(String fieldName, @Nullable short[] val) throws BinaryObjectException {
        add(fieldName, PortableClassDescriptor.Mode.SHORT_ARR);
    }

    /** {@inheritDoc} */
    @Override public void writeIntArray(String fieldName, @Nullable int[] val) throws BinaryObjectException {
        add(fieldName, PortableClassDescriptor.Mode.INT_ARR);
    }

    /** {@inheritDoc} */
    @Override public void writeLongArray(String fieldName, @Nullable long[] val) throws BinaryObjectException {
        add(fieldName, PortableClassDescriptor.Mode.LONG_ARR);
    }

    /** {@inheritDoc} */
    @Override public void writeFloatArray(String fieldName, @Nullable float[] val) throws BinaryObjectException {
        add(fieldName, PortableClassDescriptor.Mode.FLOAT_ARR);
    }

    /** {@inheritDoc} */
    @Override public void writeDoubleArray(String fieldName, @Nullable double[] val) throws BinaryObjectException {
        add(fieldName, PortableClassDescriptor.Mode.DOUBLE_ARR);
    }

    /** {@inheritDoc} */
    @Override public void writeCharArray(String fieldName, @Nullable char[] val) throws BinaryObjectException {
        add(fieldName, PortableClassDescriptor.Mode.CHAR_ARR);
    }

    /** {@inheritDoc} */
    @Override public void writeBooleanArray(String fieldName, @Nullable boolean[] val) throws BinaryObjectException {
        add(fieldName, PortableClassDescriptor.Mode.BOOLEAN_ARR);
    }

    /** {@inheritDoc} */
    @Override public void writeDecimalArray(String fieldName, @Nullable BigDecimal[] val) throws BinaryObjectException {
        add(fieldName, PortableClassDescriptor.Mode.DECIMAL_ARR);
    }

    /** {@inheritDoc} */
    @Override public void writeStringArray(String fieldName, @Nullable String[] val) throws BinaryObjectException {
        add(fieldName, PortableClassDescriptor.Mode.STRING_ARR);
    }

    /** {@inheritDoc} */
    @Override public void writeUuidArray(String fieldName, @Nullable UUID[] val) throws BinaryObjectException {
        add(fieldName, PortableClassDescriptor.Mode.UUID_ARR);
    }

    /** {@inheritDoc} */
    @Override public void writeDateArray(String fieldName, @Nullable Date[] val) throws BinaryObjectException {
        add(fieldName, PortableClassDescriptor.Mode.DATE_ARR);
    }

    /** {@inheritDoc} */
    @Override public void writeTimestampArray(String fieldName, @Nullable Timestamp[] val) throws BinaryObjectException {
        add(fieldName, PortableClassDescriptor.Mode.TIMESTAMP_ARR);
    }

    /** {@inheritDoc} */
    @Override public void writeObjectArray(String fieldName, @Nullable Object[] val) throws BinaryObjectException {
        add(fieldName, PortableClassDescriptor.Mode.OBJECT_ARR);
    }

    /** {@inheritDoc} */
    @Override public <T> void writeCollection(String fieldName, @Nullable Collection<T> col)
        throws BinaryObjectException {
        add(fieldName, PortableClassDescriptor.Mode.COL);
    }

    /** {@inheritDoc} */
    @Override public <K, V> void writeMap(String fieldName, @Nullable Map<K, V> map) throws BinaryObjectException {
        add(fieldName, PortableClassDescriptor.Mode.MAP);
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
    private void add(String name, PortableClassDescriptor.Mode mode) throws BinaryObjectException {
        assert name != null;

        int fieldTypeId = mode.typeId();

        Integer oldFieldTypeId = meta.put(name, fieldTypeId);

        if (oldFieldTypeId != null && !oldFieldTypeId.equals(fieldTypeId)) {
            throw new BinaryObjectException(
                "Field is written twice with different types [" +
                "typeName=" + typeName +
                ", fieldName=" + name +
                ", fieldTypeName1=" + PortableUtils.fieldTypeName(oldFieldTypeId) +
                ", fieldTypeName2=" + PortableUtils.fieldTypeName(fieldTypeId) +
                ']'
            );
        }
    }
}