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
 * Raw reader for portable objects. Raw reader does not use field name hash codes, therefore,
 * making the format even more compact. However, if the raw reader is used,
 * dynamic structure changes to the portable objects are not supported.
 */
public interface PortableRawReader {
    /**
     * @return Byte value.
     * @throws PortableException In case of error.
     */
    public byte readByte() throws PortableException;

    /**
     * @return Short value.
     * @throws PortableException In case of error.
     */
    public short readShort() throws PortableException;

    /**
     * @return Integer value.
     * @throws PortableException In case of error.
     */
    public int readInt() throws PortableException;

    /**
     * @return Long value.
     * @throws PortableException In case of error.
     */
    public long readLong() throws PortableException;

    /**
     * @return Float value.
     * @throws PortableException In case of error.
     */
    public float readFloat() throws PortableException;

    /**
     * @return Double value.
     * @throws PortableException In case of error.
     */
    public double readDouble() throws PortableException;

    /**
     * @return Char value.
     * @throws PortableException In case of error.
     */
    public char readChar() throws PortableException;

    /**
     * @return Boolean value.
     * @throws PortableException In case of error.
     */
    public boolean readBoolean() throws PortableException;

    /**
     * @return Decimal value.
     * @throws PortableException In case of error.
     */
    @Nullable public BigDecimal readDecimal() throws PortableException;

    /**
     * @return String value.
     * @throws PortableException In case of error.
     */
    @Nullable public String readString() throws PortableException;

    /**
     * @return UUID.
     * @throws PortableException In case of error.
     */
    @Nullable public UUID readUuid() throws PortableException;

    /**
     * @return Date.
     * @throws PortableException In case of error.
     */
    @Nullable public Date readDate() throws PortableException;

    /**
     * @return Timestamp.
     * @throws PortableException In case of error.
     */
    @Nullable public Timestamp readTimestamp() throws PortableException;

    /**
     * @return Object.
     * @throws PortableException In case of error.
     */
    @Nullable public <T> T readObject() throws PortableException;

    /**
     * @return Byte array.
     * @throws PortableException In case of error.
     */
    @Nullable public byte[] readByteArray() throws PortableException;

    /**
     * @return Short array.
     * @throws PortableException In case of error.
     */
    @Nullable public short[] readShortArray() throws PortableException;

    /**
     * @return Integer array.
     * @throws PortableException In case of error.
     */
    @Nullable public int[] readIntArray() throws PortableException;

    /**
     * @return Long array.
     * @throws PortableException In case of error.
     */
    @Nullable public long[] readLongArray() throws PortableException;

    /**
     * @return Float array.
     * @throws PortableException In case of error.
     */
    @Nullable public float[] readFloatArray() throws PortableException;

    /**
     * @return Byte array.
     * @throws PortableException In case of error.
     */
    @Nullable public double[] readDoubleArray() throws PortableException;

    /**
     * @return Char array.
     * @throws PortableException In case of error.
     */
    @Nullable public char[] readCharArray() throws PortableException;

    /**
     * @return Boolean array.
     * @throws PortableException In case of error.
     */
    @Nullable public boolean[] readBooleanArray() throws PortableException;

    /**
     * @return Decimal array.
     * @throws PortableException In case of error.
     */
    @Nullable public BigDecimal[] readDecimalArray() throws PortableException;

    /**
     * @return String array.
     * @throws PortableException In case of error.
     */
    @Nullable public String[] readStringArray() throws PortableException;

    /**
     * @return UUID array.
     * @throws PortableException In case of error.
     */
    @Nullable public UUID[] readUuidArray() throws PortableException;

    /**
     * @return Date array.
     * @throws PortableException In case of error.
     */
    @Nullable public Date[] readDateArray() throws PortableException;

    /**
     * @return Object array.
     * @throws PortableException In case of error.
     */
    @Nullable public Object[] readObjectArray() throws PortableException;

    /**
     * @return Collection.
     * @throws PortableException In case of error.
     */
    @Nullable public <T> Collection<T> readCollection() throws PortableException;

    /**
     * @param colCls Collection class.
     * @return Collection.
     * @throws PortableException In case of error.
     */
    @Nullable public <T> Collection<T> readCollection(Class<? extends Collection<T>> colCls)
        throws PortableException;

    /**
     * @return Map.
     * @throws PortableException In case of error.
     */
    @Nullable public <K, V> Map<K, V> readMap() throws PortableException;

    /**
     * @param mapCls Map class.
     * @return Map.
     * @throws PortableException In case of error.
     */
    @Nullable public <K, V> Map<K, V> readMap(Class<? extends Map<K, V>> mapCls) throws PortableException;

    /**
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable public <T extends Enum<?>> T readEnum() throws PortableException;

    /**
     * @return Value.
     * @throws PortableException In case of error.
     */
    @Nullable public <T extends Enum<?>> T[] readEnumArray() throws PortableException;
}