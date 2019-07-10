/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.binary;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

/**
 * Writer for binary object used in {@link Binarylizable} implementations.
 * Useful for the cases when user wants a fine-grained control over serialization.
 * <p>
 * Note that Ignite never writes full strings for field or type names. Instead,
 * for performance reasons, Ignite writes integer hash codes for type and field names.
 * It has been tested that hash code conflicts for the type names or the field names
 * within the same type are virtually non-existent and, to gain performance, it is safe
 * to work with hash codes. For the cases when hash codes for different types or fields
 * actually do collide, Ignite provides {@link BinaryIdMapper} which
 * allows to override the automatically generated hash code IDs for the type and field names.
 */
public interface BinaryWriter {
    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeByte(String fieldName, byte val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeShort(String fieldName, short val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeInt(String fieldName, int val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeLong(String fieldName, long val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeFloat(String fieldName, float val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeDouble(String fieldName, double val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeChar(String fieldName, char val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeBoolean(String fieldName, boolean val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeDecimal(String fieldName, BigDecimal val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeString(String fieldName, String val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val UUID to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeUuid(String fieldName, UUID val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Date to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeDate(String fieldName, Date val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Timestamp to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeTimestamp(String fieldName, Timestamp val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Time to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeTime(String fieldName, Time val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param obj Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeObject(String fieldName, Object obj) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeByteArray(String fieldName, byte[] val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeShortArray(String fieldName, short[] val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeIntArray(String fieldName, int[] val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeLongArray(String fieldName, long[] val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeFloatArray(String fieldName, float[] val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeDoubleArray(String fieldName, double[] val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeCharArray(String fieldName, char[] val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeBooleanArray(String fieldName, boolean[] val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeDecimalArray(String fieldName, BigDecimal[] val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeStringArray(String fieldName, String[] val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeUuidArray(String fieldName, UUID[] val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeDateArray(String fieldName, Date[] val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeTimestampArray(String fieldName, Timestamp[] val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeTimeArray(String fieldName, Time[] val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeObjectArray(String fieldName, Object[] val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param col Collection to write.
     * @throws BinaryObjectException In case of error.
     */
    public <T> void writeCollection(String fieldName, Collection<T> col) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param map Map to write.
     * @throws BinaryObjectException In case of error.
     */
    public <K, V> void writeMap(String fieldName, Map<K, V> map) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public <T extends Enum<?>> void writeEnum(String fieldName, T val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public <T extends Enum<?>> void writeEnumArray(String fieldName, T[] val) throws BinaryObjectException;

    /**
     * Gets raw writer. Raw writer does not write field name hash codes, therefore,
     * making the format even more compact. However, if the raw writer is used,
     * dynamic structure changes to the binary objects are not supported.
     *
     * @return Raw writer.
     */
    public BinaryRawWriter rawWriter();
}