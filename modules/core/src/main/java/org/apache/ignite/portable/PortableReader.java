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

package org.apache.ignite.portable;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/**
 * Reader for portable objects used in {@link PortableMarshalAware} implementations.
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
public interface PortableReader {
    /**
     * @param fieldName Field name.
     * @return Byte value.
     * @throws PortableException In case of error.
     */
    public byte readByte(String fieldName) throws PortableException;

    /**
     * @param fieldName Field name.
     * @return Short value.
     * @throws PortableException In case of error.
     */
    public short readShort(String fieldName) throws PortableException;

    /**
     * @param fieldName Field name.
     * @return Integer value.
     * @throws PortableException In case of error.
     */
    public int readInt(String fieldName) throws PortableException;

    /**
     * @param fieldName Field name.
     * @return Long value.
     * @throws PortableException In case of error.
     */
    public long readLong(String fieldName) throws PortableException;

    /**
     * @param fieldName Field name.
     * @throws PortableException In case of error.
     * @return Float value.
     */
    public float readFloat(String fieldName) throws PortableException;

    /**
     * @param fieldName Field name.
     * @return Double value.
     * @throws PortableException In case of error.
     */
    public double readDouble(String fieldName) throws PortableException;

    /**
     * @param fieldName Field name.
     * @return Char value.
     * @throws PortableException In case of error.
     */
    public char readChar(String fieldName) throws PortableException;

    /**
     * @param fieldName Field name.
     * @return Boolean value.
     * @throws PortableException In case of error.
     */
    public boolean readBoolean(String fieldName) throws PortableException;

    /**
     * @param fieldName Field name.
     * @return Decimal value.
     * @throws PortableException In case of error.
     */
    @Nullable public BigDecimal readDecimal(String fieldName) throws PortableException;

    /**
     * @param fieldName Field name.
     * @return String value.
     * @throws PortableException In case of error.
     */
    @Nullable public String readString(String fieldName) throws PortableException;

    /**
     * @param fieldName Field name.
     * @return UUID.
     * @throws PortableException In case of error.
     */
    @Nullable public UUID readUuid(String fieldName) throws PortableException;

    /**
     * @param fieldName Field name.
     * @return Date.
     * @throws PortableException In case of error.
     */
    @Nullable public Date readDate(String fieldName) throws PortableException;

    /**
     * @param fieldName Field name.
     * @return Timestamp.
     * @throws PortableException In case of error.
     */
    @Nullable public Timestamp readTimestamp(String fieldName) throws PortableException;

    /**
     * @param fieldName Field name.
     * @return Object.
     * @throws PortableException In case of error.
     */
    @Nullable public <T> T readObject(String fieldName) throws PortableException;

    /**
     * @param fieldName Field name.
     * @return Byte array.
     * @throws PortableException In case of error.
     */
    @Nullable public byte[] readByteArray(String fieldName) throws PortableException;

    /**
     * @param fieldName Field name.
     * @return Short array.
     * @throws PortableException In case of error.
     */
    @Nullable public short[] readShortArray(String fieldName) throws PortableException;

    /**
     * @param fieldName Field name.
     * @return Integer array.
     * @throws PortableException In case of error.
     */
    @Nullable public int[] readIntArray(String fieldName) throws PortableException;

    /**
     * @param fieldName Field name.
     * @return Long array.
     * @throws PortableException In case of error.
     */
    @Nullable public long[] readLongArray(String fieldName) throws PortableException;

    /**
     * @param fieldName Field name.
     * @return Float array.
     * @throws PortableException In case of error.
     */
    @Nullable public float[] readFloatArray(String fieldName) throws PortableException;

    /**
     * @param fieldName Field name.
     * @return Byte array.
     * @throws PortableException In case of error.
     */
    @Nullable public double[] readDoubleArray(String fieldName) throws PortableException;

    /**
     * @param fieldName Field name.
     * @return Char array.
     * @throws PortableException In case of error.
     */
    @Nullable public char[] readCharArray(String fieldName) throws PortableException;

    /**
     * @param fieldName Field name.
     * @return Boolean array.
     * @throws PortableException In case of error.
     */
    @Nullable public boolean[] readBooleanArray(String fieldName) throws PortableException;

    /**
     * @param fieldName Field name.
     * @return Decimal array.
     * @throws PortableException In case of error.
     */
    @Nullable public BigDecimal[] readDecimalArray(String fieldName) throws PortableException;

    /**
     * @param fieldName Field name.
     * @return String array.
     * @throws PortableException In case of error.
     */
    @Nullable public String[] readStringArray(String fieldName) throws PortableException;

    /**
     * @param fieldName Field name.
     * @return UUID array.
     * @throws PortableException In case of error.
     */
    @Nullable public UUID[] readUuidArray(String fieldName) throws PortableException;

    /**
     * @param fieldName Field name.
     * @return Date array.
     * @throws PortableException In case of error.
     */
    @Nullable public Date[] readDateArray(String fieldName) throws PortableException;

    /**
     * @param fieldName Field name.
     * @return Object array.
     * @throws PortableException In case of error.
     */
    @Nullable public Object[] readObjectArray(String fieldName) throws PortableException;

    /**
     * @param fieldName Field name.
     * @return Collection.
     * @throws PortableException In case of error.
     */
    @Nullable public <T> Collection<T> readCollection(String fieldName) throws PortableException;

    /**
     * @param fieldName Field name.
     * @param colCls Collection class.
     * @return Collection.
     * @throws PortableException In case of error.
     */
    @Nullable public <T> Collection<T> readCollection(String fieldName, Class<? extends Collection<T>> colCls)
        throws PortableException;

    /**
     * @param fieldName Field name.
     * @return Map.
     * @throws PortableException In case of error.
     */
    @Nullable public <K, V> Map<K, V> readMap(String fieldName) throws PortableException;

    /**
     * @param fieldName Field name.
     * @param mapCls Map class.
     * @return Map.
     * @throws PortableException In case of error.
     */
    @Nullable public <K, V> Map<K, V> readMap(String fieldName, Class<? extends Map<K, V>> mapCls)
        throws PortableException;

    /**
     * @param fieldName Field name.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable public <T extends Enum<?>> T readEnum(String fieldName) throws PortableException;

    /**
     * @param fieldName Field name.
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable public <T extends Enum<?>> T[] readEnumArray(String fieldName) throws PortableException;

    /**
     * Gets raw reader. Raw reader does not use field name hash codes, therefore,
     * making the format even more compact. However, if the raw reader is used,
     * dynamic structure changes to the portable objects are not supported.
     *
     * @return Raw reader.
     */
    public PortableRawReader rawReader();
}