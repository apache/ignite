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

package org.apache.ignite.internal.client.proto;

import static org.apache.ignite.internal.client.proto.ClientMessageCommon.HEADER_SIZE;
import static org.msgpack.core.MessagePack.Code;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.lang.IgniteUuid;

/**
 * ByteBuf-based MsgPack implementation. Replaces {@link org.msgpack.core.MessagePacker} to avoid
 * extra buffers and indirection.
 *
 * <p>Releases wrapped buffer on {@link #close()} .
 */
public class ClientMessagePacker implements AutoCloseable {
    /**
     * Underlying buffer.
     */
    private final ByteBuf buf;

    /**
     * Closed flag.
     */
    private boolean closed;

    /**
     * Constructor.
     *
     * @param buf Buffer.
     */
    public ClientMessagePacker(ByteBuf buf) {
        this.buf = buf.writerIndex(HEADER_SIZE);
    }

    /**
     * Gets the underlying buffer, including 4-byte message length at the beginning.
     *
     * @return Underlying buffer.
     */
    public ByteBuf getBuffer() {
        buf.setInt(0, buf.writerIndex() - HEADER_SIZE);

        return buf;
    }

    /**
     * Writes a Nil value.
     */
    public void packNil() {
        assert !closed : "Packer is closed";

        buf.writeByte(Code.NIL);
    }

    /**
     * Writes a boolean value.
     *
     * @param b the value to be written.
     */
    public void packBoolean(boolean b) {
        assert !closed : "Packer is closed";

        buf.writeByte(b ? Code.TRUE : Code.FALSE);
    }

    /**
     * Writes an Integer value.
     *
     * @param b the value to be written.
     */
    public void packByte(byte b) {
        assert !closed : "Packer is closed";

        if (b < -(1 << 5)) {
            buf.writeByte(Code.INT8);
        }

        buf.writeByte(b);
    }

    /**
     * Writes a short value.
     *
     * @param v the value to be written.
     */
    public void packShort(short v) {
        assert !closed : "Packer is closed";

        if (v < -(1 << 5)) {
            if (v < -(1 << 7)) {
                buf.writeByte(Code.INT16);
                buf.writeShort(v);
            } else {
                buf.writeByte(Code.INT8);
                buf.writeByte(v);
            }
        } else if (v < (1 << 7)) {
            buf.writeByte(v);
        } else {
            if (v < (1 << 8)) {
                buf.writeByte(Code.UINT8);
                buf.writeByte(v);
            } else {
                buf.writeByte(Code.UINT16);
                buf.writeShort(v);
            }
        }
    }

    /**
     * Writes an int value.
     *
     * @param i the value to be written.
     */
    public void packInt(int i) {
        assert !closed : "Packer is closed";

        if (i < -(1 << 5)) {
            if (i < -(1 << 15)) {
                buf.writeByte(Code.INT32);
                buf.writeInt(i);
            } else if (i < -(1 << 7)) {
                buf.writeByte(Code.INT16);
                buf.writeShort(i);
            } else {
                buf.writeByte(Code.INT8);
                buf.writeByte(i);
            }
        } else if (i < (1 << 7)) {
            buf.writeByte(i);
        } else {
            if (i < (1 << 8)) {
                buf.writeByte(Code.UINT8);
                buf.writeByte(i);
            } else if (i < (1 << 16)) {
                buf.writeByte(Code.UINT16);
                buf.writeShort(i);
            } else {
                buf.writeByte(Code.UINT32);
                buf.writeInt(i);
            }
        }
    }

    /**
     * Writes a long value.
     *
     * @param v the value to be written.
     */
    public void packLong(long v) {
        assert !closed : "Packer is closed";

        if (v < -(1L << 5)) {
            if (v < -(1L << 15)) {
                if (v < -(1L << 31)) {
                    buf.writeByte(Code.INT64);
                    buf.writeLong(v);
                } else {
                    buf.writeByte(Code.INT32);
                    buf.writeInt((int) v);
                }
            } else {
                if (v < -(1 << 7)) {
                    buf.writeByte(Code.INT16);
                    buf.writeShort((short) v);
                } else {
                    buf.writeByte(Code.INT8);
                    buf.writeByte((byte) v);
                }
            }
        } else if (v < (1 << 7)) {
            // fixnum
            buf.writeByte((byte) v);
        } else {
            if (v < (1L << 16)) {
                if (v < (1 << 8)) {
                    buf.writeByte(Code.UINT8);
                    buf.writeByte((byte) v);
                } else {
                    buf.writeByte(Code.UINT16);
                    buf.writeShort((short) v);
                }
            } else {
                if (v < (1L << 32)) {
                    buf.writeByte(Code.UINT32);
                    buf.writeInt((int) v);
                } else {
                    buf.writeByte(Code.UINT64);
                    buf.writeLong(v);
                }
            }
        }
    }

    /**
     * Writes a big integer value.
     *
     * @param bi the value to be written.
     */
    public void packBigInteger(BigInteger bi) {
        assert !closed : "Packer is closed";

        if (bi.bitLength() <= 63) {
            packLong(bi.longValue());
        } else if (bi.bitLength() == 64 && bi.signum() == 1) {
            buf.writeByte(Code.UINT64);
            buf.writeLong(bi.longValue());
        } else {
            throw new IllegalArgumentException(
                    "MessagePack cannot serialize BigInteger larger than 2^64-1");
        }
    }

    /**
     * Writes a float value.
     *
     * @param v the value to be written.
     */
    public void packFloat(float v) {
        assert !closed : "Packer is closed";

        buf.writeByte(Code.FLOAT32);
        buf.writeFloat(v);
    }

    /**
     * Writes a double value.
     *
     * @param v the value to be written.
     */
    public void packDouble(double v) {
        assert !closed : "Packer is closed";

        buf.writeByte(Code.FLOAT64);
        buf.writeDouble(v);
    }

    /**
     * Writes a string value.
     *
     * @param s the value to be written.
     */
    public void packString(String s) {
        assert !closed : "Packer is closed";

        // Header is a varint.
        // Use pessimistic utf8MaxBytes to reserve bytes for the header.
        // This may cause an extra byte to be used for the header,
        // but this is cheaper than calculating correct utf8 byte size, which involves full string scan.
        int maxBytes = ByteBufUtil.utf8MaxBytes(s);
        int headerSize = getStringHeaderSize(maxBytes);
        int headerPos = buf.writerIndex();

        buf.writerIndex(headerPos + headerSize);

        int bytesWritten = ByteBufUtil.writeUtf8(buf, s);
        int endPos = buf.writerIndex();

        buf.writerIndex(headerPos);

        if (headerSize == 1) {
            buf.writeByte((byte) (Code.FIXSTR_PREFIX | bytesWritten));
        } else if (headerSize == 2) {
            buf.writeByte(Code.STR8);
            buf.writeByte(bytesWritten);
        } else if (headerSize == 3) {
            buf.writeByte(Code.STR16);
            buf.writeShort(bytesWritten);
        } else {
            assert headerSize == 5 : "headerSize == 5";
            buf.writeByte(Code.STR32);
            buf.writeInt(bytesWritten);
        }

        buf.writerIndex(endPos);
    }

    /**
     * Writes an array header value.
     *
     * @param arraySize array size.
     */
    public void packArrayHeader(int arraySize) {
        assert !closed : "Packer is closed";

        if (arraySize < 0) {
            throw new IllegalArgumentException("array size must be >= 0");
        }

        if (arraySize < (1 << 4)) {
            buf.writeByte((byte) (Code.FIXARRAY_PREFIX | arraySize));
        } else if (arraySize < (1 << 16)) {
            buf.writeByte(Code.ARRAY16);
            buf.writeShort(arraySize);
        } else {
            buf.writeByte(Code.ARRAY32);
            buf.writeInt(arraySize);
        }
    }

    /**
     * Writes a map header value.
     *
     * @param mapSize map size.
     */
    public void packMapHeader(int mapSize) {
        assert !closed : "Packer is closed";

        if (mapSize < 0) {
            throw new IllegalArgumentException("map size must be >= 0");
        }

        if (mapSize < (1 << 4)) {
            buf.writeByte((byte) (Code.FIXMAP_PREFIX | mapSize));
        } else if (mapSize < (1 << 16)) {
            buf.writeByte(Code.MAP16);
            buf.writeShort(mapSize);
        } else {
            buf.writeByte(Code.MAP32);
            buf.writeInt(mapSize);
        }
    }

    /**
     * Writes Extension value header.
     *
     * <p>Should be followed by {@link #writePayload(byte[])} method to write the extension body.
     *
     * @param extType    the extension type tag to be written.
     * @param payloadLen number of bytes of a payload binary to be written.
     */
    public void packExtensionTypeHeader(byte extType, int payloadLen) {
        assert !closed : "Packer is closed";

        if (payloadLen < (1 << 8)) {
            if (payloadLen > 0
                    && (payloadLen & (payloadLen - 1)) == 0) { // check whether dataLen == 2^x
                if (payloadLen == 1) {
                    buf.writeByte(Code.FIXEXT1);
                    buf.writeByte(extType);
                } else if (payloadLen == 2) {
                    buf.writeByte(Code.FIXEXT2);
                    buf.writeByte(extType);
                } else if (payloadLen == 4) {
                    buf.writeByte(Code.FIXEXT4);
                    buf.writeByte(extType);
                } else if (payloadLen == 8) {
                    buf.writeByte(Code.FIXEXT8);
                    buf.writeByte(extType);
                } else if (payloadLen == 16) {
                    buf.writeByte(Code.FIXEXT16);
                    buf.writeByte(extType);
                } else {
                    buf.writeByte(Code.EXT8);
                    buf.writeByte(payloadLen);
                    buf.writeByte(extType);
                }
            } else {
                buf.writeByte(Code.EXT8);
                buf.writeByte(payloadLen);
                buf.writeByte(extType);
            }
        } else if (payloadLen < (1 << 16)) {
            buf.writeByte(Code.EXT16);
            buf.writeShort(payloadLen);
            buf.writeByte(extType);
        } else {
            buf.writeByte(Code.EXT32);
            buf.writeInt(payloadLen);
            buf.writeByte(extType);
        }
    }

    /**
     * Writes a binary header value.
     *
     * @param len binary value size.
     */
    public void packBinaryHeader(int len) {
        assert !closed : "Packer is closed";

        if (len < (1 << 8)) {
            buf.writeByte(Code.BIN8);
            buf.writeByte(len);
        } else if (len < (1 << 16)) {
            buf.writeByte(Code.BIN16);
            buf.writeShort(len);
        } else {
            buf.writeByte(Code.BIN32);
            buf.writeInt(len);
        }
    }

    /**
     * Writes a raw string header value.
     *
     * @param len string value size.
     */
    public void packRawStringHeader(int len) {
        assert !closed : "Packer is closed";

        if (len < (1 << 5)) {
            buf.writeByte((byte) (Code.FIXSTR_PREFIX | len));
        } else if (len < (1 << 8)) {
            buf.writeByte(Code.STR8);
            buf.writeByte(len);
        } else if (len < (1 << 16)) {
            buf.writeByte(Code.STR16);
            buf.writeShort(len);
        } else {
            buf.writeByte(Code.STR32);
            buf.writeInt(len);
        }
    }

    /**
     * Writes a byte array to the output.
     *
     * <p>This method is used with {@link #packRawStringHeader(int)} or {@link #packBinaryHeader(int)}
     * methods.
     *
     * @param src the data to add.
     */
    public void writePayload(byte[] src) {
        assert !closed : "Packer is closed";

        buf.writeBytes(src);
    }

    /**
     * Writes a byte array to the output.
     *
     * <p>This method is used with {@link #packRawStringHeader(int)} or {@link #packBinaryHeader(int)}
     * methods.
     *
     * @param src the data to add.
     * @param off the start offset in the data.
     * @param len the number of bytes to add.
     */
    public void writePayload(byte[] src, int off, int len) {
        assert !closed : "Packer is closed";

        buf.writeBytes(src, off, len);
    }

    /**
     * Writes a UUID.
     *
     * @param val UUID value.
     */
    public void packUuid(UUID val) {
        assert !closed : "Packer is closed";

        packExtensionTypeHeader(ClientMsgPackType.UUID, 16);

        buf.writeLong(val.getMostSignificantBits());
        buf.writeLong(val.getLeastSignificantBits());
    }

    /**
     * Writes an {@link IgniteUuid}.
     *
     * @param val {@link IgniteUuid} value.
     */
    public void packIgniteUuid(IgniteUuid val) {
        assert !closed : "Packer is closed";

        packExtensionTypeHeader(ClientMsgPackType.IGNITE_UUID, 24);

        UUID globalId = val.globalId();

        buf.writeLong(globalId.getMostSignificantBits());
        buf.writeLong(globalId.getLeastSignificantBits());
        buf.writeLong(val.localId());
    }

    /**
     * Writes a decimal.
     *
     * @param val Decimal value.
     */
    public void packDecimal(BigDecimal val) {
        assert !closed : "Packer is closed";

        byte[] unscaledValue = val.unscaledValue().toByteArray();

        packExtensionTypeHeader(ClientMsgPackType.DECIMAL,
                4 + unscaledValue.length); // Scale length + data length

        buf.writeInt(val.scale());
        buf.writeBytes(unscaledValue);
    }

    /**
     * Writes a decimal.
     *
     * @param val Decimal value.
     */
    public void packNumber(BigInteger val) {
        assert !closed : "Packer is closed";

        byte[] data = val.toByteArray();

        packExtensionTypeHeader(ClientMsgPackType.NUMBER, data.length);

        buf.writeBytes(data);
    }

    /**
     * Writes a bit set.
     *
     * @param val Bit set value.
     */
    public void packBitSet(BitSet val) {
        assert !closed : "Packer is closed";

        byte[] data = val.toByteArray();

        packExtensionTypeHeader(ClientMsgPackType.BITMASK, data.length);

        buf.writeBytes(data);
    }

    /**
     * Writes an integer array.
     *
     * @param arr Integer array value.
     */
    public void packIntArray(int[] arr) {
        assert !closed : "Packer is closed";

        if (arr == null) {
            packNil();

            return;
        }

        packArrayHeader(arr.length);

        for (int i : arr) {
            packInt(i);
        }
    }

    /**
     * Writes a date.
     *
     * @param val Date value.
     */
    public void packDate(LocalDate val) {
        assert !closed : "Packer is closed";

        packExtensionTypeHeader(ClientMsgPackType.DATE, 6);

        buf.writeInt(val.getYear());
        buf.writeByte(val.getMonthValue());
        buf.writeByte(val.getDayOfMonth());
    }

    /**
     * Writes a time.
     *
     * @param val Time value.
     */
    public void packTime(LocalTime val) {
        assert !closed : "Packer is closed";

        packExtensionTypeHeader(ClientMsgPackType.TIME, 7);

        buf.writeByte(val.getHour());
        buf.writeByte(val.getMinute());
        buf.writeByte(val.getSecond());
        buf.writeInt(val.getNano());
    }

    /**
     * Writes a datetime.
     *
     * @param val Datetime value.
     */
    public void packDateTime(LocalDateTime val) {
        assert !closed : "Packer is closed";

        packExtensionTypeHeader(ClientMsgPackType.DATETIME, 13);

        buf.writeInt(val.getYear());
        buf.writeByte(val.getMonthValue());
        buf.writeByte(val.getDayOfMonth());
        buf.writeByte(val.getHour());
        buf.writeByte(val.getMinute());
        buf.writeByte(val.getSecond());
        buf.writeInt(val.getNano());
    }

    /**
     * Writes a timestamp.
     *
     * @param val Timestamp value.
     */
    public void packTimestamp(Instant val) {
        assert !closed : "Packer is closed";

        packExtensionTypeHeader(ClientMsgPackType.TIMESTAMP, 12);

        buf.writeLong(val.getEpochSecond());
        buf.writeInt(val.getNano());
    }

    /**
     * Packs an object.
     *
     * @param val Object value.
     * @throws UnsupportedOperationException When type is not supported.
     */
    public void packObject(Object val) {
        if (val == null) {
            packNil();

            return;
        }

        if (val instanceof Byte) {
            packByte((byte) val);

            return;
        }

        if (val instanceof Short) {
            packShort((short) val);

            return;
        }

        if (val instanceof Integer) {
            packInt((int) val);

            return;
        }

        if (val instanceof Long) {
            packLong((long) val);

            return;
        }

        if (val instanceof Float) {
            packFloat((float) val);

            return;
        }

        if (val instanceof Double) {
            packDouble((double) val);

            return;
        }

        if (val instanceof UUID) {
            packUuid((UUID) val);

            return;
        }

        if (val instanceof String) {
            packString((String) val);

            return;
        }

        if (val instanceof byte[]) {
            byte[] bytes = (byte[]) val;
            packBinaryHeader(bytes.length);
            writePayload(bytes);

            return;
        }

        if (val instanceof BigDecimal) {
            packDecimal((BigDecimal) val);

            return;
        }

        if (val instanceof BigInteger) {
            packNumber((BigInteger) val);

            return;
        }

        if (val instanceof BitSet) {
            packBitSet((BitSet) val);

            return;
        }

        if (val instanceof LocalDate) {
            packDate((LocalDate) val);

            return;
        }

        if (val instanceof LocalTime) {
            packTime((LocalTime) val);

            return;
        }

        if (val instanceof LocalDateTime) {
            packDateTime((LocalDateTime) val);

            return;
        }

        if (val instanceof Instant) {
            packTimestamp((Instant) val);

            return;
        }

        throw new UnsupportedOperationException(
                "Unsupported type, can't serialize: " + val.getClass());
    }

    /**
     * Packs an array of different objects.
     *
     * @param args Object array.
     * @throws UnsupportedOperationException in case of unknown type.
     */
    public void packObjectArray(Object[] args) {
        assert !closed : "Packer is closed";

        if (args == null) {
            packNil();

            return;
        }

        packArrayHeader(args.length);

        for (Object arg : args) {
            if (arg == null) {
                packNil();

                continue;
            }

            Class<?> cls = arg.getClass();

            if (cls == Boolean.class) {
                packInt(ClientDataType.BOOLEAN);
                packBoolean((Boolean) arg);
            } else if (cls == Byte.class) {
                packInt(ClientDataType.INT8);
                packByte((Byte) arg);
            } else if (cls == Short.class) {
                packInt(ClientDataType.INT16);
                packShort((Short) arg);
            } else if (cls == Integer.class) {
                packInt(ClientDataType.INT32);
                packInt((Integer) arg);
            } else if (cls == Long.class) {
                packInt(ClientDataType.INT64);
                packLong((Long) arg);
            } else if (cls == Float.class) {
                packInt(ClientDataType.FLOAT);
                packFloat((Float) arg);
            } else if (cls == Double.class) {
                packInt(ClientDataType.DOUBLE);
                packDouble((Double) arg);
            } else if (cls == String.class) {
                packInt(ClientDataType.STRING);
                packString((String) arg);
            } else if (cls == UUID.class) {
                packInt(ClientDataType.UUID);
                packUuid((UUID) arg);
            } else if (cls == LocalDate.class) {
                packInt(ClientDataType.DATE);
                packDate((LocalDate) arg);
            } else if (cls == LocalTime.class) {
                packInt(ClientDataType.TIME);
                packTime((LocalTime) arg);
            } else if (cls == LocalDateTime.class) {
                packInt(ClientDataType.DATETIME);
                packDateTime((LocalDateTime) arg);
            } else if (cls == Instant.class) {
                packInt(ClientDataType.TIMESTAMP);
                packTimestamp((Instant) arg);
            } else if (cls == byte[].class) {
                packInt(ClientDataType.BYTES);

                packBinaryHeader(((byte[]) arg).length);
                writePayload((byte[]) arg);
            } else if (cls == Date.class) {
                packInt(ClientDataType.DATE);
                packDate(((Date) arg).toLocalDate());
            } else if (cls == Time.class) {
                packInt(ClientDataType.TIME);
                packTime(((Time) arg).toLocalTime());
            } else if (cls == Timestamp.class) {
                packInt(ClientDataType.TIMESTAMP);
                packTimestamp(((java.util.Date) arg).toInstant());
            } else if (cls == BigDecimal.class) {
                packInt(ClientDataType.DECIMAL);
                packDecimal(((BigDecimal) arg));
            } else if (cls == BigInteger.class) {
                packInt(ClientDataType.BIGINTEGER);
                packBigInteger(((BigInteger) arg));
            } else {
                throw new UnsupportedOperationException("Custom objects are not supported");
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }

        closed = true;

        if (buf.refCnt() > 0) {
            buf.release();
        }
    }

    /**
     * Gets the varint string header size in bytes.
     *
     * @param len String length.
     * @return String header size, in bytes.
     */
    private int getStringHeaderSize(int len) {
        assert !closed : "Packer is closed";

        if (len < (1 << 5)) {
            return 1;
        }

        if (len < (1 << 8)) {
            return 2;
        }

        if (len < (1 << 16)) {
            return 3;
        }

        return 5;
    }
}
