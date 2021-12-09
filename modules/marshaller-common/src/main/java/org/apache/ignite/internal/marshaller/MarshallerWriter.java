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

package org.apache.ignite.internal.marshaller;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.lang.IgniteException;

/**
 * Binary writer.
 */
public interface MarshallerWriter {
    /**
     * Writes null.
     */
    void writeNull();

    /**
     * Writes absent value indicator.
     *
     * <p>Absent value means that the user has not specified any value, or the column is not mapped. This is different from null.
     */
    void writeAbsentValue();

    /**
     * Writes byte.
     *
     * @param val Value.
     */
    void writeByte(byte val);

    /**
     * Writes short.
     *
     * @param val Value.
     */
    void writeShort(short val);

    /**
     * Writes int.
     *
     * @param val Value.
     */
    void writeInt(int val);

    /**
     * Writes long.
     *
     * @param val Value.
     */
    void writeLong(long val);

    /**
     * Writes float.
     *
     * @param val Value.
     */
    void writeFloat(float val);

    /**
     * Writes double.
     *
     * @param val Value.
     */
    void writeDouble(double val);

    /**
     * Writes string.
     *
     * @param val Value.
     */
    void writeString(String val);

    /**
     * Writes UUI.
     *
     * @param val Value.
     */
    void writeUuid(UUID val);

    /**
     * Writes bytes.
     *
     * @param val Value.
     */
    void writeBytes(byte[] val);

    /**
     * Writes bit set.
     *
     * @param val Value.
     */
    void writeBitSet(BitSet val);

    /**
     * Writes big integer.
     *
     * @param val Value.
     */
    void writeBigInt(BigInteger val);

    /**
     * Writes big decimal.
     *
     * @param val Value.
     */
    void writeBigDecimal(BigDecimal val);

    /**
     * Writes date.
     *
     * @param val Value.
     */
    void writeDate(LocalDate val);

    /**
     * Writes time.
     *
     * @param val Value.
     */
    void writeTime(LocalTime val);

    /**
     * Writes timestamp.
     *
     * @param val Value.
     */
    void writeTimestamp(Instant val);

    /**
     * Writes date and time.
     *
     * @param val Value.
     */
    void writeDateTime(LocalDateTime val);

    /**
     * Writes an object value.
     *
     * @param col Column.
     * @param val Value.
     */
    default void writeValue(MarshallerColumn col, Object val) {
        if (val == null) {
            writeNull();

            return;
        }

        switch (col.type()) {
            case BYTE: {
                writeByte((byte) val);

                break;
            }
            case SHORT: {
                writeShort((short) val);

                break;
            }
            case INT: {
                writeInt((int) val);

                break;
            }
            case LONG: {
                writeLong((long) val);

                break;
            }
            case FLOAT: {
                writeFloat((float) val);

                break;
            }
            case DOUBLE: {
                writeDouble((double) val);

                break;
            }
            case UUID: {
                writeUuid((UUID) val);

                break;
            }
            case TIME: {
                writeTime((LocalTime) val);

                break;
            }
            case DATE: {
                writeDate((LocalDate) val);

                break;
            }
            case DATETIME: {
                writeDateTime((LocalDateTime) val);

                break;
            }
            case TIMESTAMP: {
                writeTimestamp((Instant) val);

                break;
            }
            case STRING: {
                writeString((String) val);

                break;
            }
            case BYTE_ARR: {
                writeBytes((byte[]) val);

                break;
            }
            case BITSET: {
                writeBitSet((BitSet) val);

                break;
            }
            case NUMBER: {
                writeBigInt((BigInteger) val);

                break;
            }
            case DECIMAL: {
                writeBigDecimal((BigDecimal) val);

                break;
            }
            default:
                throw new IgniteException("Unexpected value: " + col.type());
        }
    }
}
