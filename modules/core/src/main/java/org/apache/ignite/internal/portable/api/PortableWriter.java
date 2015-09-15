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

package org.apache.ignite.internal.portable.api;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/**
 * Writer for portable object used in {@link PortableMarshalAware} implementations.
 * Useful for the cases when user wants a fine-grained control over serialization.
 * <p>
 * Note that Ignite never writes full strings for field or type names. Instead,
 * for performance reasons, Ignite writes integer hash codes for type and field names.
 * It has been tested that hash code conflicts for the type names or the field names
 * within the same type are virtually non-existent and, to gain performance, it is safe
 * to work with hash codes. For the cases when hash codes for different types or fields
 * actually do collide, Ignite provides {@link PortableIdMapper} which
 * allows to override the automatically generated hash code IDs for the type and field names.
 */
public interface PortableWriter {
    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeByte(String fieldName, byte val) throws PortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeShort(String fieldName, short val) throws PortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeInt(String fieldName, int val) throws PortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeLong(String fieldName, long val) throws PortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeFloat(String fieldName, float val) throws PortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeDouble(String fieldName, double val) throws PortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeChar(String fieldName, char val) throws PortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeBoolean(String fieldName, boolean val) throws PortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeDecimal(String fieldName, @Nullable BigDecimal val) throws PortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeString(String fieldName, @Nullable String val) throws PortableException;

    /**
     * @param fieldName Field name.
     * @param val UUID to write.
     * @throws PortableException In case of error.
     */
    public void writeUuid(String fieldName, @Nullable UUID val) throws PortableException;

    /**
     * @param fieldName Field name.
     * @param val Date to write.
     * @throws PortableException In case of error.
     */
    public void writeDate(String fieldName, @Nullable Date val) throws PortableException;

    /**
     * @param fieldName Field name.
     * @param val Timestamp to write.
     * @throws PortableException In case of error.
     */
    public void writeTimestamp(String fieldName, @Nullable Timestamp val) throws PortableException;

    /**
     * @param fieldName Field name.
     * @param obj Value to write.
     * @throws PortableException In case of error.
     */
    public void writeObject(String fieldName, @Nullable Object obj) throws PortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeByteArray(String fieldName, @Nullable byte[] val) throws PortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeShortArray(String fieldName, @Nullable short[] val) throws PortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeIntArray(String fieldName, @Nullable int[] val) throws PortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeLongArray(String fieldName, @Nullable long[] val) throws PortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeFloatArray(String fieldName, @Nullable float[] val) throws PortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeDoubleArray(String fieldName, @Nullable double[] val) throws PortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeCharArray(String fieldName, @Nullable char[] val) throws PortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeBooleanArray(String fieldName, @Nullable boolean[] val) throws PortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeDecimalArray(String fieldName, @Nullable BigDecimal[] val) throws PortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeStringArray(String fieldName, @Nullable String[] val) throws PortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeUuidArray(String fieldName, @Nullable UUID[] val) throws PortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeDateArray(String fieldName, @Nullable Date[] val) throws PortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeObjectArray(String fieldName, @Nullable Object[] val) throws PortableException;

    /**
     * @param fieldName Field name.
     * @param col Collection to write.
     * @throws PortableException In case of error.
     */
    public <T> void writeCollection(String fieldName, @Nullable Collection<T> col) throws PortableException;

    /**
     * @param fieldName Field name.
     * @param map Map to write.
     * @throws PortableException In case of error.
     */
    public <K, V> void writeMap(String fieldName, @Nullable Map<K, V> map) throws PortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public <T extends Enum<?>> void writeEnum(String fieldName, T val) throws PortableException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public <T extends Enum<?>> void writeEnumArray(String fieldName, T[] val) throws PortableException;

    /**
     * Gets raw writer. Raw writer does not write field name hash codes, therefore,
     * making the format even more compact. However, if the raw writer is used,
     * dynamic structure changes to the portable objects are not supported.
     *
     * @return Raw writer.
     */
    public PortableRawWriter rawWriter();
}