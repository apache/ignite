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

import static org.apache.ignite.internal.client.proto.ClientDataType.BIGINTEGER;
import static org.apache.ignite.internal.client.proto.ClientDataType.BITMASK;
import static org.apache.ignite.internal.client.proto.ClientDataType.BOOLEAN;
import static org.apache.ignite.internal.client.proto.ClientDataType.BYTES;
import static org.apache.ignite.internal.client.proto.ClientDataType.DATE;
import static org.apache.ignite.internal.client.proto.ClientDataType.DATETIME;
import static org.apache.ignite.internal.client.proto.ClientDataType.DECIMAL;
import static org.apache.ignite.internal.client.proto.ClientDataType.DOUBLE;
import static org.apache.ignite.internal.client.proto.ClientDataType.FLOAT;
import static org.apache.ignite.internal.client.proto.ClientDataType.INT16;
import static org.apache.ignite.internal.client.proto.ClientDataType.INT32;
import static org.apache.ignite.internal.client.proto.ClientDataType.INT64;
import static org.apache.ignite.internal.client.proto.ClientDataType.INT8;
import static org.apache.ignite.internal.client.proto.ClientDataType.NUMBER;
import static org.apache.ignite.internal.client.proto.ClientDataType.STRING;
import static org.apache.ignite.internal.client.proto.ClientDataType.TIME;
import static org.apache.ignite.internal.client.proto.ClientDataType.TIMESTAMP;
import static org.msgpack.core.MessagePack.Code;

import io.netty.buffer.ByteBuf;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteUuid;
import org.msgpack.core.ExtensionTypeHeader;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessageFormatException;
import org.msgpack.core.MessageNeverUsedFormatException;
import org.msgpack.core.MessagePackException;
import org.msgpack.core.MessageSizeException;
import org.msgpack.core.MessageTypeException;

/**
 * ByteBuf-based MsgPack implementation. Replaces {@link org.msgpack.core.MessageUnpacker} to avoid extra buffers and indirection.
 * Releases wrapped buffer on {@link #close()} .
 */
public class ClientMessageUnpacker implements AutoCloseable {
    /** Underlying buffer. */
    private final ByteBuf buf;

    /** Ref count. */
    private int refCnt = 1;

    /**
     * Constructor.
     *
     * @param buf Input.
     */
    public ClientMessageUnpacker(ByteBuf buf) {
        assert buf != null;

        this.buf = buf;
    }

    /**
     * Creates an overflow exception.
     *
     * @param u32 int value.
     * @return Excetion.
     */
    private static MessageSizeException overflowU32Size(int u32) {
        long lv = (long) (u32 & 0x7fffffff) + 0x80000000L;
        return new MessageSizeException(lv);
    }

    /**
     * Create an exception for the case when an unexpected byte value is read.
     *
     * @param expected Expected format.
     * @param b        Actual format.
     * @return Exception to throw.
     */
    private static MessagePackException unexpected(String expected, byte b) {
        MessageFormat format = MessageFormat.valueOf(b);

        if (format == MessageFormat.NEVER_USED) {
            return new MessageNeverUsedFormatException(String.format("Expected %s, but encountered 0xC1 \"NEVER_USED\" byte", expected));
        } else {
            String name = format.getValueType().name();
            String typeName = name.charAt(0) + name.substring(1).toLowerCase();
            return new MessageTypeException(String.format("Expected %s, but got %s (%02x)", expected, typeName, b));
        }
    }

    /**
     * Reads an int.
     *
     * @return the int value.
     * @throws MessageTypeException when value is not MessagePack Integer type.
     */
    public int unpackInt() {
        assert refCnt > 0 : "Unpacker is closed";

        byte code = buf.readByte();

        if (Code.isFixInt(code)) {
            return code;
        }

        switch (code) {
            case Code.UINT8:
            case Code.INT8:
                return buf.readByte();

            case Code.UINT16:
            case Code.INT16:
                return buf.readShort();

            case Code.UINT32:
            case Code.INT32:
                return buf.readInt();

            default:
                throw unexpected("Integer", code);
        }
    }

    /**
     * Reads a string.
     *
     * @return String value.
     */
    public String unpackString() {
        assert refCnt > 0 : "Unpacker is closed";

        int len = unpackRawStringHeader();
        int pos = buf.readerIndex();

        String res = buf.toString(pos, len, StandardCharsets.UTF_8);

        buf.readerIndex(pos + len);

        return res;
    }

    /**
     * Reads a Nil byte.
     *
     * @throws MessageTypeException when value is not MessagePack Nil type
     */
    public void unpackNil() {
        assert refCnt > 0 : "Unpacker is closed";

        byte code = buf.readByte();

        if (code == Code.NIL) {
            return;
        }

        throw unexpected("Nil", code);
    }

    /**
     * Reads a boolean value.
     *
     * @return Boolean.
     */
    public boolean unpackBoolean() {
        assert refCnt > 0 : "Unpacker is closed";

        byte code = buf.readByte();

        switch (code) {
            case Code.FALSE:
                return false;

            case Code.TRUE:
                return true;

            default:
                throw unexpected("boolean", code);
        }
    }

    /**
     * Reads a byte.
     *
     * @return Byte.
     */
    public byte unpackByte() {
        assert refCnt > 0 : "Unpacker is closed";

        byte code = buf.readByte();

        if (Code.isFixInt(code)) {
            return code;
        }

        switch (code) {
            case Code.UINT8:
            case Code.INT8:
                return buf.readByte();

            default:
                throw unexpected("Integer", code);
        }
    }

    /**
     * Reads a short value.
     *
     * @return Short.
     */
    public short unpackShort() {
        assert refCnt > 0 : "Unpacker is closed";

        byte code = buf.readByte();

        if (Code.isFixInt(code)) {
            return code;
        }

        switch (code) {
            case Code.UINT8:
                return buf.readUnsignedByte();

            case Code.INT8:
                return buf.readByte();

            case Code.UINT16:
                return (short) buf.readUnsignedShort();

            case Code.INT16:
                return buf.readShort();

            default:
                throw unexpected("Integer", code);
        }
    }

    /**
     * Reads a long value.
     *
     * @return Long.
     */
    public long unpackLong() {
        assert refCnt > 0 : "Unpacker is closed";

        byte code = buf.readByte();

        if (Code.isFixInt(code)) {
            return code;
        }

        switch (code) {
            case Code.UINT8:
                return buf.readUnsignedByte();

            case Code.INT8:
                return buf.readByte();

            case Code.UINT16:
                return buf.readUnsignedShort();

            case Code.INT16:
                return buf.readShort();

            case Code.UINT32:
                return buf.readUnsignedInt();

            case Code.INT32:
                return buf.readInt();

            case Code.UINT64:
            case Code.INT64:
                return buf.readLong();

            default:
                throw unexpected("Integer", code);
        }
    }

    /**
     * Reads a BigInteger value.
     *
     * @return BigInteger.
     */
    public BigInteger unpackBigInteger() {
        assert refCnt > 0 : "Unpacker is closed";

        byte code = buf.readByte();

        if (Code.isFixInt(code)) {
            return BigInteger.valueOf(code);
        }

        switch (code) {
            case Code.UINT8:
                return BigInteger.valueOf(buf.readUnsignedByte());

            case Code.UINT16:
                return BigInteger.valueOf(buf.readUnsignedShort());

            case Code.UINT32:
                return BigInteger.valueOf(buf.readUnsignedInt());

            case Code.UINT64:
                long u64 = buf.readLong();
                if (u64 < 0L) {
                    return BigInteger.valueOf(u64 + Long.MAX_VALUE + 1L).setBit(63);
                } else {
                    return BigInteger.valueOf(u64);
                }

            case Code.INT8:
                return BigInteger.valueOf(buf.readByte());

            case Code.INT16:
                return BigInteger.valueOf(buf.readShort());

            case Code.INT32:
                return BigInteger.valueOf(buf.readInt());

            case Code.INT64:
                return BigInteger.valueOf(buf.readLong());

            default:
                throw unexpected("Integer", code);
        }
    }

    /**
     * Reads a float value.
     *
     * @return Float.
     */
    public float unpackFloat() {
        assert refCnt > 0 : "Unpacker is closed";

        byte code = buf.readByte();

        switch (code) {
            case Code.FLOAT32:
                return buf.readFloat();

            case Code.FLOAT64:
                return (float) buf.readDouble();

            default:
                throw unexpected("Float", code);
        }
    }

    /**
     * Reads a double value.
     *
     * @return Double.
     */
    public double unpackDouble() {
        assert refCnt > 0 : "Unpacker is closed";

        byte code = buf.readByte();

        switch (code) {
            case Code.FLOAT32:
                return buf.readFloat();

            case Code.FLOAT64:
                return buf.readDouble();

            default:
                throw unexpected("Float", code);
        }
    }

    /**
     * Reads an array header.
     *
     * @return Array size.
     */
    public int unpackArrayHeader() {
        assert refCnt > 0 : "Unpacker is closed";

        byte code = buf.readByte();

        if (Code.isFixedArray(code)) { // fixarray
            return code & 0x0f;
        }

        switch (code) {
            case Code.ARRAY16:
                return readLength16();

            case Code.ARRAY32:
                return readLength32();

            default:
                throw unexpected("Array", code);
        }
    }

    /**
     * Reads a map header.
     *
     * @return Map size.
     */
    public int unpackMapHeader() {
        assert refCnt > 0 : "Unpacker is closed";

        byte code = buf.readByte();

        if (Code.isFixedMap(code)) { // fixmap
            return code & 0x0f;
        }

        switch (code) {
            case Code.MAP16:
                return readLength16();

            case Code.MAP32:
                return readLength32();

            default:
                throw unexpected("Map", code);
        }
    }

    /**
     * Reads an extension type header.
     *
     * @return Extension type header.
     */
    public ExtensionTypeHeader unpackExtensionTypeHeader() {
        assert refCnt > 0 : "Unpacker is closed";

        byte code = buf.readByte();

        switch (code) {
            case Code.FIXEXT1: {
                return new ExtensionTypeHeader(buf.readByte(), 1);
            }

            case Code.FIXEXT2: {
                return new ExtensionTypeHeader(buf.readByte(), 2);
            }

            case Code.FIXEXT4: {
                return new ExtensionTypeHeader(buf.readByte(), 4);
            }

            case Code.FIXEXT8: {
                return new ExtensionTypeHeader(buf.readByte(), 8);
            }

            case Code.FIXEXT16: {
                return new ExtensionTypeHeader(buf.readByte(), 16);
            }

            case Code.EXT8: {
                int length = readLength8();
                byte type = buf.readByte();

                return new ExtensionTypeHeader(type, length);
            }

            case Code.EXT16: {
                int length = readLength16();
                byte type = buf.readByte();

                return new ExtensionTypeHeader(type, length);
            }

            case Code.EXT32: {
                int length = readLength32();
                byte type = buf.readByte();

                return new ExtensionTypeHeader(type, length);
            }

            default:
                throw unexpected("Ext", code);
        }
    }

    /**
     * Reads a binary header.
     *
     * @return Binary payload size.
     */
    public int unpackBinaryHeader() {
        assert refCnt > 0 : "Unpacker is closed";

        byte code = buf.readByte();

        if (Code.isFixedRaw(code)) { // FixRaw
            return code & 0x1f;
        }

        switch (code) {
            case Code.BIN8:
                return readLength8();

            case Code.BIN16:
                return readLength16();

            case Code.BIN32:
                return readLength32();

            default:
                throw unexpected("Binary", code);
        }
    }

    /**
     * Tries to read a nil value.
     *
     * @return True when there was a nil value, false otherwise.
     */
    public boolean tryUnpackNil() {
        assert refCnt > 0 : "Unpacker is closed";

        int idx = buf.readerIndex();
        byte code = buf.getByte(idx);

        if (code == Code.NIL) {
            buf.readerIndex(idx + 1);
            return true;
        }

        return false;
    }

    /**
     * Reads a payload.
     *
     * @param length Payload size.
     * @return Payload bytes.
     */
    public byte[] readPayload(int length) {
        assert refCnt > 0 : "Unpacker is closed";

        byte[] res = new byte[length];
        buf.readBytes(res);

        return res;
    }

    /**
     * Skips values.
     *
     * @param count Number of values to skip.
     */
    public void skipValues(int count) {
        assert refCnt > 0 : "Unpacker is closed";

        while (count > 0) {
            byte code = buf.readByte();
            MessageFormat f = MessageFormat.valueOf(code);

            switch (f) {
                case POSFIXINT:
                case NEGFIXINT:
                case BOOLEAN:
                case NIL:
                    break;

                case FIXMAP: {
                    int mapLen = code & 0x0f;
                    count += mapLen * 2;
                    break;
                }

                case FIXARRAY: {
                    int arrayLen = code & 0x0f;
                    count += arrayLen;
                    break;
                }

                case FIXSTR: {
                    int strLen = code & 0x1f;
                    skipBytes(strLen);
                    break;
                }

                case INT8:
                case UINT8:
                    skipBytes(1);
                    break;

                case INT16:
                case UINT16:
                    skipBytes(2);
                    break;

                case INT32:
                case UINT32:
                case FLOAT32:
                    skipBytes(4);
                    break;

                case INT64:
                case UINT64:
                case FLOAT64:
                    skipBytes(8);
                    break;

                case BIN8:
                case STR8:
                    skipBytes(readLength8());
                    break;

                case BIN16:
                case STR16:
                    skipBytes(readLength16());
                    break;

                case BIN32:
                case STR32:
                    skipBytes(readLength32());
                    break;

                case FIXEXT1:
                    skipBytes(2);
                    break;

                case FIXEXT2:
                    skipBytes(3);
                    break;

                case FIXEXT4:
                    skipBytes(5);
                    break;

                case FIXEXT8:
                    skipBytes(9);
                    break;

                case FIXEXT16:
                    skipBytes(17);
                    break;

                case EXT8:
                    skipBytes(readLength8() + 1);
                    break;

                case EXT16:
                    skipBytes(readLength16() + 1);
                    break;

                case EXT32:
                    skipBytes(readLength32() + 1);
                    break;

                case ARRAY16:
                    count += readLength16();
                    break;

                case ARRAY32:
                    count += readLength32();
                    break;

                case MAP16:
                    count += readLength16() * 2;
                    break;

                case MAP32:
                    count += readLength32() * 2;
                    break;

                default:
                    throw new MessageFormatException("Unexpected format code: " + code);
            }

            count--;
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

        if (type != ClientMsgPackType.UUID) {
            throw new MessageTypeException("Expected UUID extension (3), but got " + type);
        }

        if (len != 16) {
            throw new MessageSizeException("Expected 16 bytes for UUID extension, but got " + len, len);
        }

        return new UUID(buf.readLong(), buf.readLong());
    }

    /**
     * Reads an {@link IgniteUuid}.
     *
     * @return {@link IgniteUuid} value.
     * @throws MessageTypeException when type is not {@link IgniteUuid}.
     * @throws MessageSizeException when size is not correct.
     */
    public IgniteUuid unpackIgniteUuid() {
        assert refCnt > 0 : "Unpacker is closed";

        var hdr = unpackExtensionTypeHeader();
        var type = hdr.getType();
        var len = hdr.getLength();

        if (type != ClientMsgPackType.IGNITE_UUID) {
            throw new MessageTypeException("Expected Ignite UUID extension (1), but got " + type);
        }

        if (len != 24) {
            throw new MessageSizeException("Expected 24 bytes for UUID extension, but got " + len, len);
        }

        return new IgniteUuid(new UUID(buf.readLong(), buf.readLong()), buf.readLong());
    }

    /**
     * Reads a decimal.
     *
     * @return Decimal value.
     * @throws MessageTypeException when type is not Decimal.
     */
    public BigDecimal unpackDecimal() {
        assert refCnt > 0 : "Unpacker is closed";

        var hdr = unpackExtensionTypeHeader();
        var type = hdr.getType();
        var len = hdr.getLength();

        if (type != ClientMsgPackType.DECIMAL) {
            throw new MessageTypeException("Expected DECIMAL extension (2), but got " + type);
        }

        int scale = buf.readInt();
        var bytes = readPayload(len - 4);

        return new BigDecimal(new BigInteger(bytes), scale);
    }

    /**
     * Reads a bit set.
     *
     * @return Bit set.
     * @throws MessageTypeException when type is not BitSet.
     */
    public BitSet unpackBitSet() {
        assert refCnt > 0 : "Unpacker is closed";

        var hdr = unpackExtensionTypeHeader();
        var type = hdr.getType();
        var len = hdr.getLength();

        if (type != ClientMsgPackType.BITMASK) {
            throw new MessageTypeException("Expected BITSET extension (7), but got " + type);
        }

        var bytes = readPayload(len);

        return BitSet.valueOf(bytes);
    }

    /**
     * Reads a number.
     *
     * @return BigInteger value.
     * @throws MessageTypeException when type is not BigInteger.
     */
    public BigInteger unpackNumber() {
        assert refCnt > 0 : "Unpacker is closed";

        var hdr = unpackExtensionTypeHeader();
        var type = hdr.getType();
        var len = hdr.getLength();

        if (type != ClientMsgPackType.NUMBER) {
            throw new MessageTypeException("Expected NUMBER extension (1), but got " + type);
        }

        var bytes = readPayload(len);

        return new BigInteger(bytes);
    }

    /**
     * Reads an integer array.
     *
     * @return Integer array.
     */
    public int[] unpackIntArray() {
        assert refCnt > 0 : "Unpacker is closed";

        int size = unpackArrayHeader();

        if (size == 0) {
            return ArrayUtils.INT_EMPTY_ARRAY;
        }

        int[] res = new int[size];

        for (int i = 0; i < size; i++) {
            res[i] = unpackInt();
        }

        return res;
    }

    /**
     * Reads a date.
     *
     * @return Date value.
     * @throws MessageTypeException when type is not DATE.
     * @throws MessageSizeException when size is not correct.
     */
    public LocalDate unpackDate() {
        assert refCnt > 0 : "Unpacker is closed";

        var hdr = unpackExtensionTypeHeader();
        var type = hdr.getType();
        var len = hdr.getLength();

        if (type != ClientMsgPackType.DATE) {
            throw new MessageTypeException("Expected DATE extension (4), but got " + type);
        }

        if (len != 6) {
            throw new MessageSizeException("Expected 6 bytes for DATE extension, but got " + len, len);
        }

        return LocalDate.of(buf.readInt(), buf.readByte(), buf.readByte());
    }

    /**
     * Reads a time.
     *
     * @return Time value.
     * @throws MessageTypeException when type is not TIME.
     * @throws MessageSizeException when size is not correct.
     */
    public LocalTime unpackTime() {
        assert refCnt > 0 : "Unpacker is closed";

        var hdr = unpackExtensionTypeHeader();
        var type = hdr.getType();
        var len = hdr.getLength();

        if (type != ClientMsgPackType.TIME) {
            throw new MessageTypeException("Expected TIME extension (5), but got " + type);
        }

        if (len != 7) {
            throw new MessageSizeException("Expected 7 bytes for TIME extension, but got " + len, len);
        }

        return LocalTime.of(buf.readByte(), buf.readByte(), buf.readByte(), buf.readInt());
    }

    /**
     * Reads a datetime.
     *
     * @return Datetime value.
     * @throws MessageTypeException when type is not DATETIME.
     * @throws MessageSizeException when size is not correct.
     */
    public LocalDateTime unpackDateTime() {
        assert refCnt > 0 : "Unpacker is closed";

        var hdr = unpackExtensionTypeHeader();
        var type = hdr.getType();
        var len = hdr.getLength();

        if (type != ClientMsgPackType.DATETIME) {
            throw new MessageTypeException("Expected DATETIME extension (6), but got " + type);
        }

        if (len != 13) {
            throw new MessageSizeException("Expected 13 bytes for DATETIME extension, but got " + len, len);
        }

        return LocalDateTime.of(
                LocalDate.of(buf.readInt(), buf.readByte(), buf.readByte()),
                LocalTime.of(buf.readByte(), buf.readByte(), buf.readByte(), buf.readInt())
        );
    }

    /**
     * Reads a timestamp.
     *
     * @return Timestamp value.
     * @throws MessageTypeException when type is not TIMESTAMP.
     * @throws MessageSizeException when size is not correct.
     */
    public Instant unpackTimestamp() {
        assert refCnt > 0 : "Unpacker is closed";

        var hdr = unpackExtensionTypeHeader();
        var type = hdr.getType();
        var len = hdr.getLength();

        if (type != ClientMsgPackType.TIMESTAMP) {
            throw new MessageTypeException("Expected TIMESTAMP extension (6), but got " + type);
        }

        if (len != 12) {
            throw new MessageSizeException("Expected 12 bytes for TIMESTAMP extension, but got " + len, len);
        }

        return Instant.ofEpochSecond(buf.readLong(), buf.readInt());
    }

    /**
     * Unpacks an object based on the specified type.
     *
     * @param dataType Data type code.
     * @return Unpacked object.
     * @throws IgniteException when data type is not valid.
     */
    public Object unpackObject(int dataType) {
        if (tryUnpackNil()) {
            return null;
        }

        switch (dataType) {
            case BOOLEAN:
                return unpackBoolean();

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

            case BYTES: {
                var cnt = unpackBinaryHeader();

                return readPayload(cnt);
            }

            case DECIMAL:
                return unpackDecimal();

            case BIGINTEGER:
                return unpackBigInteger();

            case BITMASK:
                return unpackBitSet();

            case NUMBER:
                return unpackNumber();

            case DATE:
                return unpackDate();

            case TIME:
                return unpackTime();

            case DATETIME:
                return unpackDateTime();

            case TIMESTAMP:
                return unpackTimestamp();

            default:
                throw new IgniteException("Unknown client data type: " + dataType);
        }
    }

    /**
     * Packs an object.
     *
     * @return Object array.
     * @throws IllegalStateException in case of unexpected value type.
     */
    public Object[] unpackObjectArray() {
        assert refCnt > 0 : "Unpacker is closed";

        if (tryUnpackNil()) {
            return null;
        }

        int size = unpackArrayHeader();

        if (size == 0) {
            return ArrayUtils.OBJECT_EMPTY_ARRAY;
        }

        Object[] args = new Object[size];

        for (int i = 0; i < size; i++) {
            if (tryUnpackNil()) {
                continue;
            }

            args[i] = unpackObject(unpackInt());
        }
        return args;
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
    @Override
    public void close() {
        if (refCnt == 0) {
            return;
        }

        refCnt--;

        if (buf.refCnt() > 0) {
            buf.release();
        }
    }
    
    /**
     * Unpacks string header.
     *
     * @return String length.
     */
    public int unpackRawStringHeader() {
        byte code = buf.readByte();

        if (Code.isFixedRaw(code)) {
            return code & 0x1f;
        }

        switch (code) {
            case Code.STR8:
                return readLength8();

            case Code.STR16:
                return readLength16();

            case Code.STR32:
                return readLength32();

            default:
                throw unexpected("String", code);
        }
    }

    private int readLength8() {
        return buf.readUnsignedByte();
    }

    private int readLength16() {
        return buf.readUnsignedShort();
    }

    private int readLength32() {
        int u32 = buf.readInt();

        if (u32 < 0) {
            throw overflowU32Size(u32);
        }

        return u32;
    }

    private void skipBytes(int bytes) {
        buf.readerIndex(buf.readerIndex() + bytes);
    }
}
