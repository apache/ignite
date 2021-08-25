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

package org.apache.ignite.client.proto;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import org.apache.ignite.lang.IgniteException;
import org.msgpack.core.ExtensionTypeHeader;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageSizeException;
import org.msgpack.core.MessageTypeException;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.buffer.InputStreamBufferInput;
import org.msgpack.value.ImmutableValue;

import static org.apache.ignite.client.proto.ClientDataType.BITMASK;
import static org.apache.ignite.client.proto.ClientDataType.BYTES;
import static org.apache.ignite.client.proto.ClientDataType.DATE;
import static org.apache.ignite.client.proto.ClientDataType.DATETIME;
import static org.apache.ignite.client.proto.ClientDataType.DECIMAL;
import static org.apache.ignite.client.proto.ClientDataType.DOUBLE;
import static org.apache.ignite.client.proto.ClientDataType.FLOAT;
import static org.apache.ignite.client.proto.ClientDataType.INT16;
import static org.apache.ignite.client.proto.ClientDataType.INT32;
import static org.apache.ignite.client.proto.ClientDataType.INT64;
import static org.apache.ignite.client.proto.ClientDataType.INT8;
import static org.apache.ignite.client.proto.ClientDataType.STRING;
import static org.apache.ignite.client.proto.ClientDataType.TIME;
import static org.apache.ignite.client.proto.ClientDataType.TIMESTAMP;

/**
 * Ignite-specific MsgPack extension based on Netty ByteBuf.
 * <p>
 * Releases wrapped buffer on {@link #close()} .
 */
public class ClientMessageUnpacker extends MessageUnpacker {
    /** Underlying buffer. */
    private final ByteBuf buf;

    /** Underlying input. */
    private final InputStreamBufferInput in;

    /** Ref count. */
    private int refCnt = 1;

    /**
     * Constructor.
     *
     * @param buf Input.
     */
    public ClientMessageUnpacker(ByteBuf buf) {
        // TODO: Remove intermediate classes and buffers IGNITE-15234.
        this(new InputStreamBufferInput(new ByteBufInputStream(buf)), buf);
    }

    private ClientMessageUnpacker(InputStreamBufferInput in, ByteBuf buf) {
        super(in, MessagePack.DEFAULT_UNPACKER_CONFIG);

        this.in = in;
        this.buf = buf;
    }

    /** {@inheritDoc} */
    @Override public int unpackInt() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.unpackInt();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public String unpackString() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.unpackString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void unpackNil() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            super.unpackNil();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean unpackBoolean() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.unpackBoolean();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public byte unpackByte() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.unpackByte();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public short unpackShort() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.unpackShort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long unpackLong() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.unpackLong();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public BigInteger unpackBigInteger() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.unpackBigInteger();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public float unpackFloat() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.unpackFloat();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public double unpackDouble() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.unpackDouble();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int unpackArrayHeader() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.unpackArrayHeader();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int unpackMapHeader() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.unpackMapHeader();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public ExtensionTypeHeader unpackExtensionTypeHeader() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.unpackExtensionTypeHeader();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int unpackBinaryHeader() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.unpackBinaryHeader();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean tryUnpackNil() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.tryUnpackNil();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public byte[] readPayload(int length) {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.readPayload(length);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public MessageFormat getNextFormat() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.getNextFormat();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void skipValue(int count) {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            super.skipValue(count);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void skipValue() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            super.skipValue();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.hasNext();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public ImmutableValue unpackValue() {
        assert refCnt > 0 : "Unpacker is closed";

        try {
            return super.unpackValue();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Reads an UUID.
     *
     * @return UUID value.
     * @throws MessageTypeException when type is not UUID.
     * @throws MessageSizeException when size is not correct.
     */
    public UUID unpackUuid() {
        assert refCnt > 0 : "Unpacker is closed";

        var hdr = unpackExtensionTypeHeader();
        var type = hdr.getType();
        var len = hdr.getLength();

        if (type != ClientMsgPackType.UUID)
            throw new MessageTypeException("Expected UUID extension (1), but got " + type);

        if (len != 16)
            throw new MessageSizeException("Expected 16 bytes for UUID extension, but got " + len, len);

        var bytes = readPayload(16);

        ByteBuffer bb = ByteBuffer.wrap(bytes);

        return new UUID(bb.getLong(), bb.getLong());
    }

    /**
     * Reads a decimal.
     *
     * @return Decimal value.
     * @throws UnsupportedOperationException Not supported yet.
     */
    public BigDecimal unpackDecimal() {
        assert refCnt > 0 : "Unpacker is closed";

        throw new UnsupportedOperationException("TODO: IGNITE-15163");
    }

    /**
     * Reads a bit set.
     *
     * @return Bit set.
     * @throws UnsupportedOperationException Not supported yet.
     */
    public BitSet unpackBitSet() {
        assert refCnt > 0 : "Unpacker is closed";

        throw new UnsupportedOperationException("TODO: IGNITE-15163");
    }

    /**
     * Reads a date.
     *
     * @return Date value.
     * @throws UnsupportedOperationException Not supported yet.
     */
    public LocalDate unpackDate() {
        assert refCnt > 0 : "Unpacker is closed";

        throw new UnsupportedOperationException("TODO: IGNITE-15163");
    }

    /**
     * Reads a time.
     *
     * @return Time value.
     * @throws UnsupportedOperationException Not supported yet.
     */
    public LocalTime unpackTime() {
        assert refCnt > 0 : "Unpacker is closed";

        throw new UnsupportedOperationException("TODO: IGNITE-15163");
    }

    /**
     * Reads a datetime.
     *
     * @return Datetime value.
     * @throws UnsupportedOperationException Not supported yet.
     */
    public LocalDateTime unpackDateTime() {
        assert refCnt > 0 : "Unpacker is closed";

        throw new UnsupportedOperationException("TODO: IGNITE-15163");
    }

    /**
     * Reads a timestamp.
     *
     * @return Timestamp value.
     * @throws UnsupportedOperationException Not supported yet.
     */
    public Instant unpackTimestamp() {
        assert refCnt > 0 : "Unpacker is closed";

        throw new UnsupportedOperationException("TODO: IGNITE-15163");
    }

    /**
     * Unpacks an object based on the specified type.
     *
     * @param dataType Data type code.
     *
     * @return Unpacked object.
     * @throws IgniteException when data type is not valid.
     */
    public Object unpackObject(int dataType) {
        if (tryUnpackNil())
            return null;

        switch (dataType) {
            case INT8:
                return unpackByte();

            case INT16:
                return unpackShort();

            case INT32:
                return unpackInt();

            case INT64:
                return unpackLong();

            case FLOAT:
                return unpackFloat();

            case DOUBLE:
                return unpackDouble();

            case ClientDataType.UUID:
                return unpackUuid();

            case STRING:
                return unpackString();

            case BYTES:
                var cnt = unpackBinaryHeader();

                return readPayload(cnt);

            case DECIMAL:
                return unpackDecimal();

            case BITMASK:
                return unpackBitSet();

            case DATE:
                return unpackDate();

            case TIME:
                return unpackTime();

            case DATETIME:
                return unpackDateTime();

            case TIMESTAMP:
                return unpackTimestamp();
        }

        throw new IgniteException("Unknown client data type: " + dataType);
    }

    /**
     * Creates a copy of this unpacker and the underlying buffer.
     *
     * @return Copied unpacker.
     * @throws UncheckedIOException When buffer operation fails.
     */
    public ClientMessageUnpacker copy() {
        try {
            in.reset(new ByteBufInputStream(buf.copy()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return this;
    }

    /**
     * Increases the reference count by {@code 1}.
     *
     * @return This instance.
     */
    public ClientMessageUnpacker retain() {
        refCnt++;

        buf.retain();

        return this;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        if (refCnt == 0)
            return;

        refCnt--;

        if (buf.refCnt() > 0)
            buf.release();
    }
}
