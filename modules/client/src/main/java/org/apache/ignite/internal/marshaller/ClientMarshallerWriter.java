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
import org.apache.ignite.internal.client.proto.ClientMessagePacker;

/**
 * Binary writer over {@link ClientMessagePacker}.
 */
public class ClientMarshallerWriter implements MarshallerWriter {
    /** Packer. */
    private final ClientMessagePacker packer;

    /**
     * Constructor.
     *
     * @param packer Packer.
     */
    public ClientMarshallerWriter(ClientMessagePacker packer) {
        this.packer = packer;
    }

    /** {@inheritDoc} */
    @Override
    public void writeNull() {
        packer.packNil();
    }

    /** {@inheritDoc} */
    @Override
    public void writeAbsentValue() {
        // TODO: Handle missing values and null values differently (IGNITE-16093).
        packer.packNil();
    }

    /** {@inheritDoc} */
    @Override
    public void writeByte(byte val) {
        packer.packByte(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeShort(short val) {
        packer.packShort(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeInt(int val) {
        packer.packInt(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeLong(long val) {
        packer.packLong(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeFloat(float val) {
        packer.packFloat(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeDouble(double val) {
        packer.packDouble(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeString(String val) {
        packer.packString(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeUuid(UUID val) {
        packer.packUuid(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeBytes(byte[] val) {
        packer.packBinaryHeader(val.length);
        packer.writePayload(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeBitSet(BitSet val) {
        packer.packBitSet(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeBigInt(BigInteger val) {
        packer.packNumber(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeBigDecimal(BigDecimal val) {
        packer.packDecimal(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeDate(LocalDate val) {
        packer.packDate(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeTime(LocalTime val) {
        packer.packTime(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeTimestamp(Instant val) {
        packer.packTimestamp(val);
    }

    /** {@inheritDoc} */
    @Override
    public void writeDateTime(LocalDateTime val) {
        packer.packDateTime(val);
    }
}
