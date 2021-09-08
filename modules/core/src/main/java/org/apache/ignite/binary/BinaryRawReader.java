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
import org.jetbrains.annotations.Nullable;

/**
 * Raw reader for binary objects. Raw reader does not use field name hash codes, therefore,
 * making the format even more compact. However, if the raw reader is used,
 * dynamic structure changes to the binary objects are not supported.
 */
public interface BinaryRawReader {
    /**
     * @return Byte value.
     * @throws BinaryObjectException In case of error.
     */
    public byte readByte() throws BinaryObjectException;

    /**
     * @return Short value.
     * @throws BinaryObjectException In case of error.
     */
    public short readShort() throws BinaryObjectException;

    /**
     * @return Integer value.
     * @throws BinaryObjectException In case of error.
     */
    public int readInt() throws BinaryObjectException;

    /**
     * @return Long value.
     * @throws BinaryObjectException In case of error.
     */
    public long readLong() throws BinaryObjectException;

    /**
     * @return Float value.
     * @throws BinaryObjectException In case of error.
     */
    public float readFloat() throws BinaryObjectException;

    /**
     * @return Double value.
     * @throws BinaryObjectException In case of error.
     */
    public double readDouble() throws BinaryObjectException;

    /**
     * @return Char value.
     * @throws BinaryObjectException In case of error.
     */
    public char readChar() throws BinaryObjectException;

    /**
     * @return Boolean value.
     * @throws BinaryObjectException In case of error.
     */
    public boolean readBoolean() throws BinaryObjectException;

    /**
     * @return Decimal value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public BigDecimal readDecimal() throws BinaryObjectException;

    /**
     * @return String value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public String readString() throws BinaryObjectException;

    /**
     * @return UUID.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public UUID readUuid() throws BinaryObjectException;

    /**
     * @return Date.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public Date readDate() throws BinaryObjectException;

    /**
     * @return Timestamp.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public Timestamp readTimestamp() throws BinaryObjectException;

    /**
     * @return Time.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public Time readTime() throws BinaryObjectException;

    /**
     * @return Object.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public <T> T readObject() throws BinaryObjectException;

    /**
     * @return Byte array.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public byte[] readByteArray() throws BinaryObjectException;

    /**
     * @return Short array.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public short[] readShortArray() throws BinaryObjectException;

    /**
     * @return Integer array.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public int[] readIntArray() throws BinaryObjectException;

    /**
     * @return Long array.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public long[] readLongArray() throws BinaryObjectException;

    /**
     * @return Float array.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public float[] readFloatArray() throws BinaryObjectException;

    /**
     * @return Byte array.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public double[] readDoubleArray() throws BinaryObjectException;

    /**
     * @return Char array.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public char[] readCharArray() throws BinaryObjectException;

    /**
     * @return Boolean array.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public boolean[] readBooleanArray() throws BinaryObjectException;

    /**
     * @return Decimal array.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public BigDecimal[] readDecimalArray() throws BinaryObjectException;

    /**
     * @return String array.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public String[] readStringArray() throws BinaryObjectException;

    /**
     * @return UUID array.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public UUID[] readUuidArray() throws BinaryObjectException;

    /**
     * @return Date array.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public Date[] readDateArray() throws BinaryObjectException;

    /**
     * @return Timestamp array.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public Timestamp[] readTimestampArray() throws BinaryObjectException;

    /**
     * @return Time array.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public Time[] readTimeArray() throws BinaryObjectException;

    /**
     * @return Object array.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public Object[] readObjectArray() throws BinaryObjectException;

    /**
     * @return Collection.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public <T> Collection<T> readCollection() throws BinaryObjectException;

    /**
     * @param factory Collection factory.
     * @return Collection.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public <T> Collection<T> readCollection(BinaryCollectionFactory<T> factory)
        throws BinaryObjectException;

    /**
     * @return Map.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public <K, V> Map<K, V> readMap() throws BinaryObjectException;

    /**
     * @param factory Map factory.
     * @return Map.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public <K, V> Map<K, V> readMap(BinaryMapFactory<K, V> factory) throws BinaryObjectException;

    /**
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public <T extends Enum<?>> T readEnum() throws BinaryObjectException;

    /**
     * @return Value.
     * @throws BinaryObjectException In case of error.
     */
    @Nullable public <T extends Enum<?>> T[] readEnumArray() throws BinaryObjectException;
}
