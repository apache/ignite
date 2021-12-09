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

/**
 * Binary reader.
 */
public interface MarshallerReader {
    /**
     * Skips a value.
     */
    void skipValue();

    /**
     * Reads a byte.
     *
     * @return Value.
     */
    byte readByte();

    /**
     * Reads a byte.
     *
     * @return Value.
     */
    Byte readByteBoxed();

    /**
     * Reads a short.
     *
     * @return Value.
     */
    short readShort();

    /**
     * Reads a short.
     *
     * @return Value.
     */
    Short readShortBoxed();

    /**
     * Reads an int.
     *
     * @return Value.
     */
    int readInt();

    /**
     * Reads an int.
     *
     * @return Value.
     */
    Integer readIntBoxed();

    /**
     * Reads a long.
     *
     * @return Value.
     */
    long readLong();

    /**
     * Reads a long.
     *
     * @return Value.
     */
    Long readLongBoxed();

    /**
     * Reads a float.
     *
     * @return Value.
     */
    float readFloat();

    /**
     * Reads a float.
     *
     * @return Value.
     */
    Float readFloatBoxed();

    /**
     * Reads a double.
     *
     * @return Value.
     */
    double readDouble();

    /**
     * Reads a double.
     *
     * @return Value.
     */
    Double readDoubleBoxed();

    /**
     * Reads a string.
     *
     * @return Value.
     */
    String readString();

    /**
     * Reads a UUID.
     *
     * @return Value.
     */
    UUID readUuid();

    /**
     * Reads a byte array.
     *
     * @return Value.
     */
    byte[] readBytes();

    /**
     * Reads a bit set.
     *
     * @return Value.
     */
    BitSet readBitSet();

    /**
     * Reads a big integer.
     *
     * @return Value.
     */
    BigInteger readBigInt();

    /**
     * Reads a big decimal.
     *
     * @return Value.
     */
    BigDecimal readBigDecimal();

    /**
     * Reads a date.
     *
     * @return Value.
     */
    LocalDate readDate();

    /**
     * Reads a time.
     *
     * @return Value.
     */
    LocalTime readTime();

    /**
     * Reads a timestamp.
     *
     * @return Value.
     */
    Instant readTimestamp();

    /**
     * Reads a date with time.
     *
     * @return Value.
     */
    LocalDateTime readDateTime();
}
