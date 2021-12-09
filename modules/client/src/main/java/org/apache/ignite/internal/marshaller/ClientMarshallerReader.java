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
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;

/**
 * Binary reader over {@link ClientMessageUnpacker}.
 */
public class ClientMarshallerReader implements MarshallerReader {
    /** Unpacker. */
    private final ClientMessageUnpacker unpacker;

    /**
     * Constructor.
     *
     * @param unpacker Unpacker.
     */
    public ClientMarshallerReader(ClientMessageUnpacker unpacker) {
        this.unpacker = unpacker;
    }

    /** {@inheritDoc} */
    @Override
    public void skipValue() {
        unpacker.skipValues(1);
    }

    /** {@inheritDoc} */
    @Override
    public byte readByte() {
        return unpacker.unpackByte();
    }

    /** {@inheritDoc} */
    @Override
    public Byte readByteBoxed() {
        return unpacker.tryUnpackNil() ? null : unpacker.unpackByte();
    }

    /** {@inheritDoc} */
    @Override
    public short readShort() {
        return unpacker.unpackShort();
    }

    /** {@inheritDoc} */
    @Override
    public Short readShortBoxed() {
        return unpacker.tryUnpackNil() ? null : unpacker.unpackShort();
    }

    /** {@inheritDoc} */
    @Override
    public int readInt() {
        return unpacker.unpackInt();
    }

    /** {@inheritDoc} */
    @Override
    public Integer readIntBoxed() {
        return unpacker.tryUnpackNil() ? null : unpacker.unpackInt();
    }

    /** {@inheritDoc} */
    @Override
    public long readLong() {
        return unpacker.unpackLong();
    }

    /** {@inheritDoc} */
    @Override
    public Long readLongBoxed() {
        return unpacker.tryUnpackNil() ? null : unpacker.unpackLong();
    }

    /** {@inheritDoc} */
    @Override
    public float readFloat() {
        return unpacker.unpackFloat();
    }

    /** {@inheritDoc} */
    @Override
    public Float readFloatBoxed() {
        return unpacker.tryUnpackNil() ? null : unpacker.unpackFloat();
    }

    /** {@inheritDoc} */
    @Override
    public double readDouble() {
        return unpacker.unpackDouble();
    }

    /** {@inheritDoc} */
    @Override
    public Double readDoubleBoxed() {
        return unpacker.tryUnpackNil() ? null : unpacker.unpackDouble();
    }

    /** {@inheritDoc} */
    @Override
    public String readString() {
        return unpacker.unpackString();
    }

    /** {@inheritDoc} */
    @Override
    public UUID readUuid() {
        return unpacker.unpackUuid();
    }

    /** {@inheritDoc} */
    @Override
    public byte[] readBytes() {
        return unpacker.readPayload(unpacker.unpackBinaryHeader());
    }

    /** {@inheritDoc} */
    @Override
    public BitSet readBitSet() {
        return unpacker.unpackBitSet();
    }

    /** {@inheritDoc} */
    @Override
    public BigInteger readBigInt() {
        return unpacker.unpackNumber();
    }

    /** {@inheritDoc} */
    @Override
    public BigDecimal readBigDecimal() {
        return unpacker.unpackDecimal();
    }

    /** {@inheritDoc} */
    @Override
    public LocalDate readDate() {
        return unpacker.unpackDate();
    }

    /** {@inheritDoc} */
    @Override
    public LocalTime readTime() {
        return unpacker.unpackTime();
    }

    /** {@inheritDoc} */
    @Override
    public Instant readTimestamp() {
        return unpacker.unpackTimestamp();
    }

    /** {@inheritDoc} */
    @Override
    public LocalDateTime readDateTime() {
        return unpacker.unpackDateTime();
    }
}
