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

package org.apache.ignite.igniteobject;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/**
 * Reader for portable objects used in {@link IgniteObjectMarshalAware} implementations.
 * Useful for the cases when user wants a fine-grained control over serialization.
 * <p>
 * Note that Ignite never writes full strings for field or type names. Instead,
 * for performance reasons, Ignite writes integer hash codes for type and field names.
 * It has been tested that hash code conflicts for the type names or the field names
 * within the same type are virtually non-existent and, to gain performance, it is safe
 * to work with hash codes. For the cases when hash codes for different types or fields
 * actually do collide, Ignite provides {@link IgniteObjectIdMapper} which
 * allows to override the automatically generated hash code IDs for the type and field names.
 */
public interface IgniteObjectReader {
    /**
     * @param fieldName Field name.
     * @return Byte value.
     * @throws IgniteObjectException In case of error.
     */
    public byte readByte(String fieldName) throws IgniteObjectException;

    /**
     * @param fieldName Field name.
     * @return Short value.
     * @throws IgniteObjectException In case of error.
     */
    public short readShort(String fieldName) throws IgniteObjectException;

    /**
     * @param fieldName Field name.
     * @return Integer value.
     * @throws IgniteObjectException In case of error.
     */
    public int readInt(String fieldName) throws IgniteObjectException;

    /**
     * @param fieldName Field name.
     * @return Long value.
     * @throws IgniteObjectException In case of error.
     */
    public long readLong(String fieldName) throws IgniteObjectException;

    /**
     * @param fieldName Field name.
     * @throws IgniteObjectException In case of error.
     * @return Float value.
     */
    public float readFloat(String fieldName) throws IgniteObjectException;

    /**
     * @param fieldName Field name.
     * @return Double value.
     * @throws IgniteObjectException In case of error.
     */
    public double readDouble(String fieldName) throws IgniteObjectException;

    /**
     * @param fieldName Field name.
     * @return Char value.
     * @throws IgniteObjectException In case of error.
     */
    public char readChar(String fieldName) throws IgniteObjectException;

    /**
     * @param fieldName Field name.
     * @return Boolean value.
     * @throws IgniteObjectException In case of error.
     */
    public boolean readBoolean(String fieldName) throws IgniteObjectException;

    /**
     * @param fieldName Field name.
     * @return Decimal value.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public BigDecimal readDecimal(String fieldName) throws IgniteObjectException;

    /**
     * @param fieldName Field name.
     * @return String value.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public String readString(String fieldName) throws IgniteObjectException;

    /**
     * @param fieldName Field name.
     * @return UUID.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public UUID readUuid(String fieldName) throws IgniteObjectException;

    /**
     * @param fieldName Field name.
     * @return Date.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public Date readDate(String fieldName) throws IgniteObjectException;

    /**
     * @param fieldName Field name.
     * @return Timestamp.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public Timestamp readTimestamp(String fieldName) throws IgniteObjectException;

    /**
     * @param fieldName Field name.
     * @return Object.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public <T> T readObject(String fieldName) throws IgniteObjectException;

    /**
     * @param fieldName Field name.
     * @return Byte array.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public byte[] readByteArray(String fieldName) throws IgniteObjectException;

    /**
     * @param fieldName Field name.
     * @return Short array.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public short[] readShortArray(String fieldName) throws IgniteObjectException;

    /**
     * @param fieldName Field name.
     * @return Integer array.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public int[] readIntArray(String fieldName) throws IgniteObjectException;

    /**
     * @param fieldName Field name.
     * @return Long array.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public long[] readLongArray(String fieldName) throws IgniteObjectException;

    /**
     * @param fieldName Field name.
     * @return Float array.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public float[] readFloatArray(String fieldName) throws IgniteObjectException;

    /**
     * @param fieldName Field name.
     * @return Byte array.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public double[] readDoubleArray(String fieldName) throws IgniteObjectException;

    /**
     * @param fieldName Field name.
     * @return Char array.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public char[] readCharArray(String fieldName) throws IgniteObjectException;

    /**
     * @param fieldName Field name.
     * @return Boolean array.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public boolean[] readBooleanArray(String fieldName) throws IgniteObjectException;

    /**
     * @param fieldName Field name.
     * @return Decimal array.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public BigDecimal[] readDecimalArray(String fieldName) throws IgniteObjectException;

    /**
     * @param fieldName Field name.
     * @return String array.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public String[] readStringArray(String fieldName) throws IgniteObjectException;

    /**
     * @param fieldName Field name.
     * @return UUID array.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public UUID[] readUuidArray(String fieldName) throws IgniteObjectException;

    /**
     * @param fieldName Field name.
     * @return Date array.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public Date[] readDateArray(String fieldName) throws IgniteObjectException;

    /**
     * @param fieldName Field name.
     * @return Timestamp array.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public Timestamp[] readTimestampArray(String fieldName) throws IgniteObjectException;

    /**
     * @param fieldName Field name.
     * @return Object array.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public Object[] readObjectArray(String fieldName) throws IgniteObjectException;

    /**
     * @param fieldName Field name.
     * @return Collection.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public <T> Collection<T> readCollection(String fieldName) throws IgniteObjectException;

    /**
     * @param fieldName Field name.
     * @param colCls Collection class.
     * @return Collection.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public <T> Collection<T> readCollection(String fieldName, Class<? extends Collection<T>> colCls)
        throws IgniteObjectException;

    /**
     * @param fieldName Field name.
     * @return Map.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public <K, V> Map<K, V> readMap(String fieldName) throws IgniteObjectException;

    /**
     * @param fieldName Field name.
     * @param mapCls Map class.
     * @return Map.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public <K, V> Map<K, V> readMap(String fieldName, Class<? extends Map<K, V>> mapCls)
        throws IgniteObjectException;

    /**
     * @param fieldName Field name.
     * @return Value.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public <T extends Enum<?>> T readEnum(String fieldName) throws IgniteObjectException;

    /**
     * @param fieldName Field name.
     * @return Value.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public <T extends Enum<?>> T[] readEnumArray(String fieldName) throws IgniteObjectException;

    /**
     * Gets raw reader. Raw reader does not use field name hash codes, therefore,
     * making the format even more compact. However, if the raw reader is used,
     * dynamic structure changes to the portable objects are not supported.
     *
     * @return Raw reader.
     */
    public IgniteObjectRawReader rawReader();
}