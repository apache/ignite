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
 * Raw writer for binary object. Raw writer does not write field name hash codes, therefore,
 * making the format even more compact. However, if the raw writer is used,
 * dynamic structure changes to the binary objects are not supported.
 */
public interface BinaryRawWriter {
    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeByte(byte val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeShort(short val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeInt(int val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeLong(long val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeFloat(float val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeDouble(double val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeChar(char val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeBoolean(boolean val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeDecimal(BigDecimal val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeString(String val) throws BinaryObjectException;

    /**
     * @param val UUID to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeUuid(UUID val) throws BinaryObjectException;

    /**
     * @param val Date to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeDate(Date val) throws BinaryObjectException;

    /**
     * @param val Timestamp to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeTimestamp(Timestamp val) throws BinaryObjectException;

    /**
     * @param val Time to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeTime(Time val) throws BinaryObjectException;

    /**
     * @param obj Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeObject(Object obj) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeByteArray(byte[] val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeShortArray(short[] val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeIntArray(int[] val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeLongArray(long[] val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeFloatArray(float[] val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeDoubleArray(double[] val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeCharArray(char[] val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeBooleanArray(boolean[] val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeDecimalArray(BigDecimal[] val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeStringArray(String[] val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeUuidArray(UUID[] val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeDateArray(Date[] val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeTimestampArray(Timestamp[] val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeTimeArray(Time[] val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeObjectArray(Object[] val) throws BinaryObjectException;

    /**
     * @param col Collection to write.
     * @throws BinaryObjectException In case of error.
     */
    public <T> void writeCollection(Collection<T> col) throws BinaryObjectException;

    /**
     * @param map Map to write.
     * @throws BinaryObjectException In case of error.
     */
    public <K, V> void writeMap(Map<K, V> map) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public <T extends Enum<?>> void writeEnum(T val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public <T extends Enum<?>> void writeEnumArray(T[] val) throws BinaryObjectException;
}