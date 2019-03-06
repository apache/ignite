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
    public void writeDecimal(String fieldName, @Nullable BigDecimal val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeString(String fieldName, @Nullable String val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val UUID to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeUuid(String fieldName, @Nullable UUID val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Date to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeDate(String fieldName, @Nullable Date val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Timestamp to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeTimestamp(String fieldName, @Nullable Timestamp val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Time to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeTime(String fieldName, @Nullable Time val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param obj Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeObject(String fieldName, @Nullable Object obj) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeByteArray(String fieldName, @Nullable byte[] val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeShortArray(String fieldName, @Nullable short[] val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeIntArray(String fieldName, @Nullable int[] val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeLongArray(String fieldName, @Nullable long[] val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeFloatArray(String fieldName, @Nullable float[] val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeDoubleArray(String fieldName, @Nullable double[] val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeCharArray(String fieldName, @Nullable char[] val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeBooleanArray(String fieldName, @Nullable boolean[] val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeDecimalArray(String fieldName, @Nullable BigDecimal[] val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeStringArray(String fieldName, @Nullable String[] val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeUuidArray(String fieldName, @Nullable UUID[] val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeDateArray(String fieldName, @Nullable Date[] val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeTimestampArray(String fieldName, @Nullable Timestamp[] val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeTimeArray(String fieldName, @Nullable Time[] val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param val Value to write.
     * @throws BinaryObjectException In case of error.
     */
    public void writeObjectArray(String fieldName, @Nullable Object[] val) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param col Collection to write.
     * @throws BinaryObjectException In case of error.
     */
    public <T> void writeCollection(String fieldName, @Nullable Collection<T> col) throws BinaryObjectException;

    /**
     * @param fieldName Field name.
     * @param map Map to write.
     * @throws BinaryObjectException In case of error.
     */
    public <K, V> void writeMap(String fieldName, @Nullable Map<K, V> map) throws BinaryObjectException;

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