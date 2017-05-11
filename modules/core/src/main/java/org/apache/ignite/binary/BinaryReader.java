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

package org.apache.ignite.binary;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

/**
 * Reader for binary objects used in {@link Binarylizable} implementations.
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
public interface BinaryReader {
    /**
     * @param fieldName Field name.
     * @return Byte value.
     * @throws BinaryObjectException In case of error.
     */
    public byte readByte(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @return Short value.
     * @throws BinaryObjectException In case of error.
     */
    public short readShort(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @return Integer value.
     * @throws BinaryObjectException In case of error.
     */
    public int readInt(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @return Long value.
     * @throws BinaryObjectException In case of error.
     */
    public long readLong(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @throws BinaryObjectException In case of error.
     * @return Float value.
     */
    public float readFloat(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @return Double value.
     * @throws BinaryObjectException In case of error.
     */
    public double readDouble(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @return Char value.
     * @throws BinaryObjectException In case of error.
     */
    public char readChar(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @return Boolean value.
     * @throws BinaryObjectException In case of error.
     */
    public boolean readBoolean(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @return Decimal value.
     * @throws BinaryObjectException In case of error.
     */
    public BigDecimal readDecimal(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @return String value.
     * @throws BinaryObjectException In case of error.
     */
    public String readString(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @return UUID.
     * @throws BinaryObjectException In case of error.
     */
    public UUID readUuid(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @return Date.
     * @throws BinaryObjectException In case of error.
     */
    public Date readDate(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @return Timestamp.
     * @throws BinaryObjectException In case of error.
     */
    public Timestamp readTimestamp(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @return Time.
     * @throws BinaryObjectException In case of error.
     */
    public Time readTime(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @return Object.
     * @throws BinaryObjectException In case of error.
     */
    public <T> T readObject(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @return Byte array.
     * @throws BinaryObjectException In case of error.
     */
    public byte[] readByteArray(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @return Short array.
     * @throws BinaryObjectException In case of error.
     */
    public short[] readShortArray(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @return Integer array.
     * @throws BinaryObjectException In case of error.
     */
    public int[] readIntArray(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @return Long array.
     * @throws BinaryObjectException In case of error.
     */
    public long[] readLongArray(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @return Float array.
     * @throws BinaryObjectException In case of error.
     */
    public float[] readFloatArray(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @return Byte array.
     * @throws BinaryObjectException In case of error.
     */
    public double[] readDoubleArray(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @return Char array.
     * @throws BinaryObjectException In case of error.
     */
    public char[] readCharArray(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @return Boolean array.
     * @throws BinaryObjectException In case of error.
     */
    public boolean[] readBooleanArray(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @return Decimal array.
     * @throws BinaryObjectException In case of error.
     */
    public BigDecimal[] readDecimalArray(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @return String array.
     * @throws BinaryObjectException In case of error.
     */
    public String[] readStringArray(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @return UUID array.
     * @throws BinaryObjectException In case of error.
     */
    public UUID[] readUuidArray(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @return Date array.
     * @throws BinaryObjectException In case of error.
     */
    public Date[] readDateArray(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @return Timestamp array.
     * @throws BinaryObjectException In case of error.
     */
    public Timestamp[] readTimestampArray(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @return Time array.
     * @throws BinaryObjectException In case of error.
     */
    public Time[] readTimeArray(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @return Object array.
     * @throws BinaryObjectException In case of error.
     */
    public Object[] readObjectArray(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @return Collection.
     * @throws BinaryObjectException In case of error.
     */
    public <T> Collection<T> readCollection(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param factory Collection factory.
     * @return Collection.
     * @throws BinaryObjectException In case of error.
     */
    public <T> Collection<T> readCollection(String fieldName, BinaryCollectionFactory<T> factory)
        throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @return Map.
     * @throws BinaryObjectException In case of error.
     */
    public <K, V> Map<K, V> readMap(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param factory Map factory.
     * @return Map.
     * @throws BinaryObjectException In case of error.
     */
    public <K, V> Map<K, V> readMap(String fieldName, BinaryMapFactory<K, V> factory) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    public <T extends Enum<?>> T readEnum(String fieldName) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    public <T extends Enum<?>> T[] readEnumArray(String fieldName) throws BinaryObjectException;

    /**
     * Gets raw reader. Raw reader does not use field name hash codes, therefore,
     * making the format even more compact. However, if the raw reader is used,
     * dynamic structure changes to the binary objects are not supported.
     *
     * @return Raw reader.
     */
    public BinaryRawReader rawReader();
}
