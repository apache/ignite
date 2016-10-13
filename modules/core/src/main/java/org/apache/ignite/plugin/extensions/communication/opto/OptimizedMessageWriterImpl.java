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

package org.apache.ignite.plugin.extensions.communication.opto;

import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

import static org.apache.ignite.internal.util.GridUnsafe.BIG_ENDIAN;
import static org.apache.ignite.internal.util.GridUnsafe.BYTE_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.CHAR_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.DOUBLE_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.FLOAT_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.INT_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.LONG_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.SHORT_ARR_OFF;

/**
 * Optimized message writer implementation.
 */
public class OptimizedMessageWriterImpl implements OptimizedMessageWriter {
    /** State. */
    private final OptimizedMessageState state;

    /** Current buffer. */
    private ByteBuffer buf;

    /** */
    private byte[] heapArr;

    /** */
    private long baseOff;

    /**
     * Constructor.
     *
     * @param state State.
     */
    public OptimizedMessageWriterImpl(OptimizedMessageState state) {
        this.state = state;

        nextBuffer();
    }

    /** {@inheritDoc} */
    @Override public void writeHeader(byte type) {
        writeByte(type);
    }

    /** {@inheritDoc} */
    @Override public void writeByte(byte val) {
        if (remaining() == 0)
            pushBuffer();

        buf.put(val);
    }

    /** {@inheritDoc} */
    @Override public void writeShort(short val) {
        if (remaining() < 2)
            pushBuffer();

        int pos = buf.position();

        long off = baseOff + pos;

        if (BIG_ENDIAN)
            GridUnsafe.putShortLE(heapArr, off, val);
        else
            GridUnsafe.putShort(heapArr, off, val);

        buf.position(pos + 2);
    }

    /** {@inheritDoc} */
    @Override public void writeInt(int val) {
        if (remaining() < 5)
            pushBuffer();

        if (val == Integer.MAX_VALUE)
            val = Integer.MIN_VALUE;
        else
            val++;

        int pos = buf.position();

        while ((val & 0xFFFF_FF80) != 0) {
            byte b = (byte)(val | 0x80);

            GridUnsafe.putByte(heapArr, baseOff + pos++, b);

            val >>>= 7;
        }

        GridUnsafe.putByte(heapArr, baseOff + pos++, (byte)val);

        buf.position(pos);
    }

    /** {@inheritDoc} */
    @Override public void writeLong(long val) {
        if (remaining() < 10)
            pushBuffer();

        if (val == Long.MAX_VALUE)
            val = Long.MIN_VALUE;
        else
            val++;

        int pos = buf.position();

        while ((val & 0xFFFF_FFFF_FFFF_FF80L) != 0) {
            byte b = (byte)(val | 0x80);

            GridUnsafe.putByte(heapArr, baseOff + pos++, b);

            val >>>= 7;
        }

        GridUnsafe.putByte(heapArr, baseOff + pos++, (byte)val);

        buf.position(pos);
    }

    /** {@inheritDoc} */
    @Override public void writeFloat(float val) {
        if (remaining() < 4)
            pushBuffer();

        int pos = buf.position();

        long off = baseOff + pos;

        if (BIG_ENDIAN)
            GridUnsafe.putFloatLE(heapArr, off, val);
        else
            GridUnsafe.putFloat(heapArr, off, val);

        buf.position(pos + 4);
    }

    /** {@inheritDoc} */
    @Override public void writeDouble(double val) {
        if (remaining() < 8)
            pushBuffer();

        int pos = buf.position();

        long off = baseOff + pos;

        if (BIG_ENDIAN)
            GridUnsafe.putDoubleLE(heapArr, off, val);
        else
            GridUnsafe.putDouble(heapArr, off, val);

        buf.position(pos + 8);
    }

    /** {@inheritDoc} */
    @Override public void writeChar(char val) {
        if (remaining() < 8)
            pushBuffer();

        int pos = buf.position();

        long off = baseOff + pos;

        if (BIG_ENDIAN)
            GridUnsafe.putCharLE(heapArr, off, val);
        else
            GridUnsafe.putChar(heapArr, off, val);

        buf.position(pos + 2);
    }

    /** {@inheritDoc} */
    @Override public void writeBoolean(boolean val) {
        writeByte(val ? (byte)1 : (byte)0);
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(byte[] val) {
        if (val != null)
            writeByteArray(val, 0, val.length);
        else
            writeInt(-1);
    }

    /** {@inheritDoc} */
    @Override public void writeByteArray(byte[] val, long off, int len) {
        if (val != null)
            writeArray(val, BYTE_ARR_OFF + off, len, len);
        else
            writeInt(-1);
    }

    /** {@inheritDoc} */
    @Override public void writeShortArray(short[] val) {
        if (val != null)
            if (BIG_ENDIAN)
                throw new UnsupportedOperationException(); // TODO
            else
                writeArray(val, SHORT_ARR_OFF, val.length, val.length << 1);
        else
            writeInt(-1);
    }

    /** {@inheritDoc} */
    @Override public void writeIntArray(int[] val) {
        if (val != null)
            if (BIG_ENDIAN)
                throw new UnsupportedOperationException(); // TODO
            else
                writeArray(val, INT_ARR_OFF, val.length, val.length << 2);
        else
            writeInt(-1);
    }

    /** {@inheritDoc} */
    @Override public void writeLongArray(long[] val) {
        if (val != null)
            if (BIG_ENDIAN)
                throw new UnsupportedOperationException(); // TODO
            else
                writeArray(val, LONG_ARR_OFF, val.length, val.length << 3);
        else
            writeInt(-1);
    }

    /** {@inheritDoc} */
    @Override public void writeFloatArray(float[] val) {
        if (val != null)
            if (BIG_ENDIAN)
                throw new UnsupportedOperationException(); // TODO
            else
                writeArray(val, FLOAT_ARR_OFF, val.length, val.length << 2);
        else
            writeInt(-1);
    }

    /** {@inheritDoc} */
    @Override public void writeDoubleArray(double[] val) {
        if (val != null)
            if (BIG_ENDIAN)
                throw new UnsupportedOperationException(); // TODO
            else
                writeArray(val, DOUBLE_ARR_OFF, val.length, val.length << 3);
        else
            writeInt(-1);
    }

    /** {@inheritDoc} */
    @Override public void writeCharArray(char[] val) {
        if (val != null) {
            if (BIG_ENDIAN)
                throw new UnsupportedOperationException(); // TODO
            else
                writeArray(val, CHAR_ARR_OFF, val.length, val.length << 1);
        }
        else
            writeInt(-1);
    }

    /** {@inheritDoc} */
    @Override public void writeBooleanArray(boolean[] val) {
        if (val != null)
            writeArray(val, GridUnsafe.BOOLEAN_ARR_OFF, val.length, val.length);
        else
            writeInt(-1);
    }

    /** {@inheritDoc} */
    @Override public void writeString(String val) {
        writeByteArray(val != null ? val.getBytes() : null);
    }

    /** {@inheritDoc} */
    @Override public void writeBitSet(BitSet val) {
        writeLongArray(val != null ? val.toLongArray() : null);
    }

    /** {@inheritDoc} */
    @Override public void writeUuid(UUID val) {
        if (remaining() < 1 + 10 + 10)
            pushBuffer();

        if (val == null)
            writeBoolean(true);
        else {
            writeBoolean(false);
            writeLong(val.getMostSignificantBits());
            writeLong(val.getLeastSignificantBits());
        }
    }

    /** {@inheritDoc} */
    @Override public void writeIgniteUuid(IgniteUuid val) {
        if (remaining() < 1 + 10 + 10 + 10)
            pushBuffer();

        if (val == null)
            writeBoolean(true);
        else {
            writeBoolean(false);
            writeLong(val.globalId().getMostSignificantBits());
            writeLong(val.globalId().getLeastSignificantBits());
            writeLong(val.localId());
        }
    }

    /** {@inheritDoc} */
    @Override public void writeMessage(Message val) {
        // TODO
    }

    /** {@inheritDoc} */
    @Override public <T> void writeObjectArray(T[] arr, MessageCollectionItemType itemType) {
        if (arr != null) {
            writeInt(arr.length);

            for (T obj : arr)
                write(itemType, obj);
        }
        else
            writeInt(-1);
    }

    /** {@inheritDoc} */
    @Override public <T> void writeCollection(Collection<T> col, MessageCollectionItemType itemType) {
        if (col != null) {
            writeInt(col.size());

            for (T obj : col)
                write(itemType, obj);
        }
        else
            writeInt(-1);
    }

    /** {@inheritDoc} */
    @Override public <K, V> void writeMap(Map<K, V> map, MessageCollectionItemType keyType,
        MessageCollectionItemType valType) {
        if (map != null) {
            writeInt(map.size());

            for (Map.Entry<K, V> entry : map.entrySet()) {
                write(keyType, entry.getKey());
                write(valType, entry.getValue());
            }
        }
        else
            writeInt(-1);
    }

    /**
     * @param type Type.
     * @param val Value.
     */
    private void write(MessageCollectionItemType type, Object val) {
        switch (type) {
            case BYTE:
                writeByte((Byte)val);

                break;

            case SHORT:
                writeShort((Short)val);

                break;

            case INT:
                writeInt((Integer)val);

                break;

            case LONG:
                writeLong((Long)val);

                break;

            case FLOAT:
                writeFloat((Float)val);

                break;

            case DOUBLE:
                writeDouble((Double)val);

                break;

            case CHAR:
                writeChar((Character)val);

                break;

            case BOOLEAN:
                writeBoolean((Boolean)val);

                break;

            case BYTE_ARR:
                writeByteArray((byte[])val);

                break;

            case SHORT_ARR:
                writeShortArray((short[])val);

                break;

            case INT_ARR:
                writeIntArray((int[])val);

                break;

            case LONG_ARR:
                writeLongArray((long[])val);

                break;

            case FLOAT_ARR:
                writeFloatArray((float[])val);

                break;

            case DOUBLE_ARR:
                writeDoubleArray((double[])val);

                break;

            case CHAR_ARR:
                writeCharArray((char[])val);

                break;

            case BOOLEAN_ARR:
                writeBooleanArray((boolean[])val);

                break;

            case STRING:
                writeString((String)val);

                break;

            case BIT_SET:
                writeBitSet((BitSet)val);

                break;

            case UUID:
                writeUuid((UUID)val);

                break;

            case IGNITE_UUID:
                writeIgniteUuid((IgniteUuid)val);

                break;

            case MSG:
                writeMessage((Message)val);

                break;

            default:
                throw new IllegalArgumentException("Unknown type: " + type);
        }
    }

    /**
     * Push buffer.
     */
    private void pushBuffer() {
        assert buf.remaining() == 0;

        state.pushBuffer();

        nextBuffer();
    }

    /**
     * Set next buffer.
     */
    private void nextBuffer() {
        if (buf == null)
            buf = state.buffer();
        else
            buf = state.pushBuffer();

        heapArr = buf.isDirect() ? null : buf.array();
        baseOff = buf.isDirect() ? ((DirectBuffer)buf).address() : GridUnsafe.BYTE_ARR_OFF;
    }

    /**
     * @return Number of remaining bytes.
     */
    private int remaining() {
        return buf.remaining();
    }

    /**
     * Write array.
     *
     * @param arr Array.
     * @param off Offset.
     * @param len Length.
     * @param lenBytes Length in bytes.
     */
    private void writeArray(Object arr, long off, int len, int lenBytes) {
        writeInt(len);

        writeArray0(arr, off, lenBytes);
    }

    /**
     * Write array.
     *
     * @param arr Array.
     * @param off Offset.
     * @param lenBytes Length in bytes.
     */
    private void writeArray0(Object arr, long off, int lenBytes) {
        if (lenBytes == 0)
            return;

        int remaining = remaining();

        if (remaining == 0) {
            nextBuffer();

            remaining = remaining();
        }

        if (lenBytes <= remaining) {
            // Full write in one hop.
            int pos = buf.position();

            GridUnsafe.copyMemory(arr, off, heapArr, baseOff + pos, lenBytes);

            buf.position(pos + lenBytes);
        }
        else {
            // Need several writes.
            int written = 0;

            while (written < lenBytes) {
                int cnt = Math.min(lenBytes - written, remaining); // How many bytes to write.

                int pos = buf.position();

                GridUnsafe.copyMemory(arr, off + written, heapArr, baseOff + pos, cnt);

                buf.position(pos + cnt);

                written += cnt;

                remaining = remaining();

                if (remaining == 0) {
                    nextBuffer();

                    remaining = remaining();
                }
            }
        }
    }
}
