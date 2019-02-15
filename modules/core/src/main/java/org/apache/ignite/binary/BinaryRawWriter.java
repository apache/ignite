/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
    public void writeDecimal(@Nullable BigDecimal val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeString(@Nullable String val) throws BinaryObjectException;

    /**
     * @param val UUID to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeUuid(@Nullable UUID val) throws BinaryObjectException;

    /**
     * @param val Date to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeDate(@Nullable Date val) throws BinaryObjectException;

    /**
     * @param val Timestamp to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeTimestamp(@Nullable Timestamp val) throws BinaryObjectException;

    /**
     * @param val Time to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeTime(@Nullable Time val) throws BinaryObjectException;

    /**
     * @param obj Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeObject(@Nullable Object obj) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeByteArray(@Nullable byte[] val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeShortArray(@Nullable short[] val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeIntArray(@Nullable int[] val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeLongArray(@Nullable long[] val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeFloatArray(@Nullable float[] val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeDoubleArray(@Nullable double[] val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeCharArray(@Nullable char[] val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeBooleanArray(@Nullable boolean[] val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeDecimalArray(@Nullable BigDecimal[] val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeStringArray(@Nullable String[] val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeUuidArray(@Nullable UUID[] val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeDateArray(@Nullable Date[] val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeTimestampArray(@Nullable Timestamp[] val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeTimeArray(@Nullable Time[] val) throws BinaryObjectException;

    /**
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeObjectArray(@Nullable Object[] val) throws BinaryObjectException;

    /**
     * @param col Collection to write.
     * @throws BinaryObjectException In case of error.
     */
    public <T> void writeCollection(@Nullable Collection<T> col) throws BinaryObjectException;

    /**
     * @param map Map to write.
     * @throws BinaryObjectException In case of error.
     */
    public <K, V> void writeMap(@Nullable Map<K, V> map) throws BinaryObjectException;

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