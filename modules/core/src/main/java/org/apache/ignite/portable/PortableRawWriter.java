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
 * Raw writer for portable object. Raw writer does not write field name hash codes, therefore,
 * making the format even more compact. However, if the raw writer is used,
 * dynamic structure changes to the portable objects are not supported.
 */
public interface PortableRawWriter {
    /**
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeByte(byte val) throws PortableException;

    /**
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeShort(short val) throws PortableException;

    /**
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeInt(int val) throws PortableException;

    /**
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeLong(long val) throws PortableException;

    /**
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeFloat(float val) throws PortableException;

    /**
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeDouble(double val) throws PortableException;

    /**
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeChar(char val) throws PortableException;

    /**
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeBoolean(boolean val) throws PortableException;

    /**
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeDecimal(@Nullable BigDecimal val) throws PortableException;

    /**
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeString(@Nullable String val) throws PortableException;

    /**
     * @param val UUID to write.
     * @throws PortableException In case of error.
     */
    public void writeUuid(@Nullable UUID val) throws PortableException;

    /**
     * @param val Date to write.
     * @throws PortableException In case of error.
     */
    public void writeDate(@Nullable Date val) throws PortableException;

    /**
     * @param val Timestamp to write.
     * @throws PortableException In case of error.
     */
    public void writeTimestamp(@Nullable Timestamp val) throws PortableException;

    /**
     * @param obj Value to write.
     * @throws PortableException In case of error.
     */
    public void writeObject(@Nullable Object obj) throws PortableException;

    /**
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeByteArray(@Nullable byte[] val) throws PortableException;

    /**
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeShortArray(@Nullable short[] val) throws PortableException;

    /**
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeIntArray(@Nullable int[] val) throws PortableException;

    /**
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeLongArray(@Nullable long[] val) throws PortableException;

    /**
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeFloatArray(@Nullable float[] val) throws PortableException;

    /**
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeDoubleArray(@Nullable double[] val) throws PortableException;

    /**
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeCharArray(@Nullable char[] val) throws PortableException;

    /**
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeBooleanArray(@Nullable boolean[] val) throws PortableException;

    /**
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeDecimalArray(@Nullable BigDecimal[] val) throws PortableException;

    /**
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeStringArray(@Nullable String[] val) throws PortableException;

    /**
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeUuidArray(@Nullable UUID[] val) throws PortableException;

    /**
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeDateArray(@Nullable Date[] val) throws PortableException;

    /**
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public void writeObjectArray(@Nullable Object[] val) throws PortableException;

    /**
     * @param col Collection to write.
     * @throws PortableException In case of error.
     */
    public <T> void writeCollection(@Nullable Collection<T> col) throws PortableException;

    /**
     * @param map Map to write.
     * @throws PortableException In case of error.
     */
    public <K, V> void writeMap(@Nullable Map<K, V> map) throws PortableException;

    /**
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public <T extends Enum<?>> void writeEnum(T val) throws PortableException;

    /**
     * @param val Value to write.
     * @throws PortableException In case of error.
     */
    public <T extends Enum<?>> void writeEnumArray(T[] val) throws PortableException;
}