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
 * Raw reader for portable objects. Raw reader does not use field name hash codes, therefore,
 * making the format even more compact. However, if the raw reader is used,
 * dynamic structure changes to the portable objects are not supported.
 */
public interface IgniteObjectRawReader {
    /**
     * @return Byte value.
     * @throws IgniteObjectException In case of error.
     */
    public byte readByte() throws IgniteObjectException;

    /**
     * @return Short value.
     * @throws IgniteObjectException In case of error.
     */
    public short readShort() throws IgniteObjectException;

    /**
     * @return Integer value.
     * @throws IgniteObjectException In case of error.
     */
    public int readInt() throws IgniteObjectException;

    /**
     * @return Long value.
     * @throws IgniteObjectException In case of error.
     */
    public long readLong() throws IgniteObjectException;

    /**
     * @return Float value.
     * @throws IgniteObjectException In case of error.
     */
    public float readFloat() throws IgniteObjectException;

    /**
     * @return Double value.
     * @throws IgniteObjectException In case of error.
     */
    public double readDouble() throws IgniteObjectException;

    /**
     * @return Char value.
     * @throws IgniteObjectException In case of error.
     */
    public char readChar() throws IgniteObjectException;

    /**
     * @return Boolean value.
     * @throws IgniteObjectException In case of error.
     */
    public boolean readBoolean() throws IgniteObjectException;

    /**
     * @return Decimal value.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public BigDecimal readDecimal() throws IgniteObjectException;

    /**
     * @return String value.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public String readString() throws IgniteObjectException;

    /**
     * @return UUID.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public UUID readUuid() throws IgniteObjectException;

    /**
     * @return Date.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public Date readDate() throws IgniteObjectException;

    /**
     * @return Timestamp.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public Timestamp readTimestamp() throws IgniteObjectException;

    /**
     * @return Object.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public <T> T readObject() throws IgniteObjectException;

    /**
     * @return Byte array.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public byte[] readByteArray() throws IgniteObjectException;

    /**
     * @return Short array.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public short[] readShortArray() throws IgniteObjectException;

    /**
     * @return Integer array.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public int[] readIntArray() throws IgniteObjectException;

    /**
     * @return Long array.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public long[] readLongArray() throws IgniteObjectException;

    /**
     * @return Float array.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public float[] readFloatArray() throws IgniteObjectException;

    /**
     * @return Byte array.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public double[] readDoubleArray() throws IgniteObjectException;

    /**
     * @return Char array.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public char[] readCharArray() throws IgniteObjectException;

    /**
     * @return Boolean array.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public boolean[] readBooleanArray() throws IgniteObjectException;

    /**
     * @return Decimal array.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public BigDecimal[] readDecimalArray() throws IgniteObjectException;

    /**
     * @return String array.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public String[] readStringArray() throws IgniteObjectException;

    /**
     * @return UUID array.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public UUID[] readUuidArray() throws IgniteObjectException;

    /**
     * @return Date array.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public Date[] readDateArray() throws IgniteObjectException;

    /**
     * @return Timestamp array.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public Timestamp[] readTimestampArray() throws IgniteObjectException;

    /**
     * @return Object array.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public Object[] readObjectArray() throws IgniteObjectException;

    /**
     * @return Collection.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public <T> Collection<T> readCollection() throws IgniteObjectException;

    /**
     * @param colCls Collection class.
     * @return Collection.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public <T> Collection<T> readCollection(Class<? extends Collection<T>> colCls)
        throws IgniteObjectException;

    /**
     * @return Map.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public <K, V> Map<K, V> readMap() throws IgniteObjectException;

    /**
     * @param mapCls Map class.
     * @return Map.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public <K, V> Map<K, V> readMap(Class<? extends Map<K, V>> mapCls) throws IgniteObjectException;

    /**
     * @return Value.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public <T extends Enum<?>> T readEnum() throws IgniteObjectException;

    /**
     * @return Value.
     * @throws IgniteObjectException In case of error.
     */
    @Nullable public <T extends Enum<?>> T[] readEnumArray() throws IgniteObjectException;
}