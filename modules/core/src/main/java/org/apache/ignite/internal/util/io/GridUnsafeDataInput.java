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

package org.apache.ignite.internal.util.io;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UTFDataFormatException;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_MARSHAL_BUFFERS_RECHECK;
import static org.apache.ignite.internal.binary.streams.BinaryMemoryAllocator.DFLT_MARSHAL_BUFFERS_RECHECK;
import static org.apache.ignite.internal.util.GridUnsafe.BIG_ENDIAN;
import static org.apache.ignite.internal.util.GridUnsafe.BYTE_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.CHAR_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.DOUBLE_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.FLOAT_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.INT_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.LONG_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.SHORT_ARR_OFF;

/**
 * Data input based on {@code Unsafe} operations.
 */
public class GridUnsafeDataInput extends InputStream implements GridDataInput {
    /** */
    private static final Long CHECK_FREQ = Long.getLong(IGNITE_MARSHAL_BUFFERS_RECHECK, DFLT_MARSHAL_BUFFERS_RECHECK);

    /** Maximum data block length. */
    private static final int MAX_BLOCK_SIZE = 1024;

    /** Length of char buffer (for reading strings). */
    private static final int CHAR_BUF_SIZE = 256;

    /** Buffer for reading general/block data. */
    @GridToStringExclude
    private final byte[] utfBuf = new byte[MAX_BLOCK_SIZE];

    /** Char buffer for fast string reads. */
    @GridToStringExclude
    private final char[] urfCBuf = new char[CHAR_BUF_SIZE];

    /** Current offset into buf. */
    private int pos;

    /** End offset of valid data in buf, or -1 if no more block data. */
    private int end = -1;

    /** Bytes. */
    @GridToStringExclude
    private byte[] buf;

    /** Offset. */
    private int off;

    /** Max. */
    private int max;

    /** Underlying input stream. */
    @GridToStringExclude
    private InputStream in;

    /** Buffer for reading from stream. */
    @GridToStringExclude
    private byte[] inBuf = new byte[1024];

    /** Maximum message size. */
    private int maxOff;

    /** Last length check timestamp. */
    private long lastCheck;

    /** {@inheritDoc} */
    @Override public void bytes(byte[] bytes, int len) {
        bytes(bytes, 0, len);
    }

    /**
     * @param bytes Bytes.
     * @param off Offset.
     * @param len Length.
     */
    public void bytes(byte[] bytes, int off, int len) {
        buf = bytes;

        max = len;
        this.off = off;
    }

    /** {@inheritDoc} */
    @Override public void inputStream(InputStream in) throws IOException {
        this.in = in;

        buf = inBuf;
    }

    /**
     * Reads from stream to buffer. If stream is {@code null}, this method is no-op.
     *
     * @param size Number of bytes to read.
     * @throws IOException In case of error.
     */
    private void fromStream(int size) throws IOException {
        if (in == null)
            return;

        maxOff = Math.max(maxOff, size);

        long now = U.currentTimeMillis();

        // Increase size of buffer if needed.
        if (size > inBuf.length)
            buf = inBuf = new byte[Math.max(inBuf.length << 1, size)]; // Grow.
        else if (now - lastCheck > CHECK_FREQ) {
            int halfSize = inBuf.length >> 1;

            if (maxOff < halfSize) {
                byte[] newInBuf = new byte[halfSize]; // Shrink.

                System.arraycopy(inBuf, 0, newInBuf, 0, off);

                buf = inBuf = newInBuf;
            }

            maxOff = 0;
            lastCheck = now;
        }

        off = 0;
        max = 0;

        while (max != size) {
            int read = in.read(inBuf, max, size - max);

            if (read == -1)
                throw new EOFException("End of stream reached: " + in);

            max += read;
        }
    }

    /**
     * @param more Bytes to move forward.
     * @return Old offset value.
     * @throws IOException In case of error.
     */
    private int offset(int more) throws IOException {
        int old = off;

        off += more;

        if (off > max)
            throw new EOFException("Attempt to read beyond the end of the stream " +
                "[pos=" + off + ", more=" + more + ", max=" + max + ']');

        return old;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NonSynchronizedMethodOverridesSynchronizedMethod")
    @Override public void reset() throws IOException {
        in = null;

        off = 0;
        max = 0;
    }

    /** {@inheritDoc} */
    @Override public byte[] readByteArray() throws IOException {
        int arrSize = readInt();

        fromStream(arrSize);

        byte[] arr = new byte[arrSize];

        System.arraycopy(buf, offset(arrSize), arr, 0, arrSize);

        return arr;
    }

    /** {@inheritDoc} */
    @Override public short[] readShortArray() throws IOException {
        int arrSize = readInt();

        int bytesToCp = arrSize << 1;

        fromStream(bytesToCp);

        short[] arr = new short[arrSize];

        long off = BYTE_ARR_OFF + offset(bytesToCp);

        if (BIG_ENDIAN) {
            for (int i = 0; i < arr.length; i++) {
                arr[i] = GridUnsafe.getShortLE(buf, off);

                off += 2;
            }
        }
        else
            GridUnsafe.copyMemory(buf, off, arr, SHORT_ARR_OFF, bytesToCp);

        return arr;
    }

    /** {@inheritDoc} */
    @Override public int[] readIntArray() throws IOException {
        int arrSize = readInt();

        int bytesToCp = arrSize << 2;

        fromStream(bytesToCp);

        int[] arr = new int[arrSize];

        long off = BYTE_ARR_OFF + offset(bytesToCp);

        if (BIG_ENDIAN) {
            for (int i = 0; i < arr.length; i++) {
                arr[i] = GridUnsafe.getIntLE(buf, off);

                off += 4;
            }
        }
        else
            GridUnsafe.copyMemory(buf, off, arr, INT_ARR_OFF, bytesToCp);

        return arr;
    }

    /** {@inheritDoc} */
    @Override public double[] readDoubleArray() throws IOException {
        int arrSize = readInt();

        int bytesToCp = arrSize << 3;

        fromStream(bytesToCp);

        double[] arr = new double[arrSize];

        long off = BYTE_ARR_OFF + offset(bytesToCp);

        if (BIG_ENDIAN) {
            for (int i = 0; i < arr.length; i++) {
                arr[i] = GridUnsafe.getDoubleLE(buf, off);

                off += 8;
            }
        }
        else
            GridUnsafe.copyMemory(buf, off, arr, DOUBLE_ARR_OFF, bytesToCp);

        return arr;
    }

    /** {@inheritDoc} */
    @Override public boolean[] readBooleanArray() throws IOException {
        int arrSize = readInt();

        boolean[] vals = new boolean[arrSize];

        for (int i = 0; i < arrSize; i++)
            vals[i] = readBoolean();

        return vals;
    }

    /** {@inheritDoc} */
    @Override public char[] readCharArray() throws IOException {
        int arrSize = readInt();

        int bytesToCp = arrSize << 1;

        fromStream(bytesToCp);

        char[] arr = new char[arrSize];

        long off = BYTE_ARR_OFF + offset(bytesToCp);

        if (BIG_ENDIAN) {
            for (int i = 0; i < arr.length; i++) {
                arr[i] = GridUnsafe.getCharLE(buf, off);

                off += 2;
            }
        }
        else
            GridUnsafe.copyMemory(buf, off, arr, CHAR_ARR_OFF, bytesToCp);

        return arr;
    }

    /** {@inheritDoc} */
    @Override public long[] readLongArray() throws IOException {
        int arrSize = readInt();

        int bytesToCp = arrSize << 3;

        fromStream(bytesToCp);

        long[] arr = new long[arrSize];

        long off = BYTE_ARR_OFF + offset(bytesToCp);

        if (BIG_ENDIAN) {
            for (int i = 0; i < arr.length; i++) {
                arr[i] = GridUnsafe.getLongLE(buf, off);

                off += 8;
            }
        }
        else
            GridUnsafe.copyMemory(buf, off, arr, LONG_ARR_OFF, bytesToCp);

        return arr;
    }

    /** {@inheritDoc} */
    @Override public float[] readFloatArray() throws IOException {
        int arrSize = readInt();

        int bytesToCp = arrSize << 2;

        fromStream(bytesToCp);

        float[] arr = new float[arrSize];

        long off = BYTE_ARR_OFF + offset(bytesToCp);

        if (BIG_ENDIAN) {
            for (int i = 0; i < arr.length; i++) {
                arr[i] = GridUnsafe.getFloatLE(buf, off);

                off += 4;
            }
        }
        else
            GridUnsafe.copyMemory(buf, off, arr, FLOAT_ARR_OFF, bytesToCp);

        return arr;
    }

    /** {@inheritDoc} */
    @Override public void readFully(byte[] b) throws IOException {
        int len = b.length;

        fromStream(len);

        System.arraycopy(buf, offset(len), b, 0, len);
    }

    /** {@inheritDoc} */
    @Override public void readFully(byte[] b, int off, int len) throws IOException {
        fromStream(len);

        System.arraycopy(buf, offset(len), b, off, len);
    }

    /** {@inheritDoc} */
    @Override public int skipBytes(int n) throws IOException {
        if (off + n > max)
            n = max - off;

        off += n;

        return n;
    }

    /** {@inheritDoc} */
    @Override public boolean readBoolean() throws IOException {
        fromStream(1);

        return GridUnsafe.getBoolean(buf, BYTE_ARR_OFF + offset(1));
    }

    /** {@inheritDoc} */
    @Override public byte readByte() throws IOException {
        fromStream(1);

        return GridUnsafe.getByte(buf, BYTE_ARR_OFF + offset(1));
    }

    /** {@inheritDoc} */
    @Override public int readUnsignedByte() throws IOException {
        return readByte() & 0xff;
    }

    /** {@inheritDoc} */
    @Override public short readShort() throws IOException {
        fromStream(2);

        long off = BYTE_ARR_OFF + offset(2);

        return BIG_ENDIAN ? GridUnsafe.getShortLE(buf, off) : GridUnsafe.getShort(buf, off);
    }

    /** {@inheritDoc} */
    @Override public int readUnsignedShort() throws IOException {
        return readShort() & 0xffff;
    }

    /** {@inheritDoc} */
    @Override public char readChar() throws IOException {
        fromStream(2);

        long off = BYTE_ARR_OFF + this.off;

        char v = BIG_ENDIAN ? GridUnsafe.getCharLE(buf, off) : GridUnsafe.getChar(buf, off);

        offset(2);

        return v;
    }

    /** {@inheritDoc} */
    @Override public int readInt() throws IOException {
        fromStream(4);

        long off = BYTE_ARR_OFF + offset(4);

        return BIG_ENDIAN ? GridUnsafe.getIntLE(buf, off) : GridUnsafe.getInt(buf, off);
    }

    /** {@inheritDoc} */
    @Override public long readLong() throws IOException {
        fromStream(8);

        long off = BYTE_ARR_OFF + offset(8);

        return BIG_ENDIAN ? GridUnsafe.getLongLE(buf, off) : GridUnsafe.getLong(buf, off);
    }

    /** {@inheritDoc} */
    @Override public float readFloat() throws IOException {
        int v = readInt();

        return Float.intBitsToFloat(v);
    }

    /** {@inheritDoc} */
    @Override public double readDouble() throws IOException {
        long v = readLong();

        return Double.longBitsToDouble(v);
    }

    /** {@inheritDoc} */
    @Override public int read() throws IOException {
        try {
            return readUnsignedByte();
        }
        catch (EOFException ignored) {
            return -1;
        }
    }

    /** {@inheritDoc} */
    @Override public int read(byte b[], int off, int len) throws IOException {
        if (b == null)
            throw new NullPointerException();

        if (off < 0 || len < 0 || len > b.length - off)
            throw new IndexOutOfBoundsException();

        if (len == 0)
            return 0;

        if (in != null)
            return in.read(b, off, len);
        else {
            int toRead = Math.min(len, max - this.off);

            System.arraycopy(buf, offset(toRead), b, off, toRead);

            return toRead;
        }
    }

    /** {@inheritDoc} */
    @Override public String readLine() throws IOException {
        SB sb = new SB();

        int b;

        while ((b = read()) >= 0) {
            char c = (char)b;

            switch (c) {
                case '\n':
                    return sb.toString();

                case '\r':
                    b = read();

                    if (b < 0 || b == '\n')
                        return sb.toString();
                    else
                        sb.a((char)b);

                    break;

                default:
                    sb.a(c);
            }
        }

        return sb.toString();
    }

    /** {@inheritDoc} */
    @Override public String readUTF() throws IOException {
        return readUTFBody(readInt());
    }

    /**
     * Reads in the "body" (i.e., the UTF representation minus the 2-byte
     * or 8-byte length header) of a UTF encoding, which occupies the next
     * utfLen bytes.
     *
     * @param utfLen UTF encoding length.
     * @return String.
     * @throws IOException In case of error.
     */
    private String readUTFBody(long utfLen) throws IOException {
        StringBuilder sbuf = new StringBuilder();

        end = pos = 0;

        while (utfLen > 0) {
            int avail = end - pos;

            if (avail >= 3 || (long)avail == utfLen)
                utfLen -= readUTFSpan(sbuf, utfLen);
            else {
                // shift and refill buffer manually
                if (avail > 0)
                    System.arraycopy(utfBuf, pos, utfBuf, 0, avail);

                pos = 0;
                end = (int)Math.min(MAX_BLOCK_SIZE, utfLen);

                readFully(utfBuf, avail, end - avail);
            }
        }

        return sbuf.toString();
    }

    /**
     * Reads span of UTF-encoded characters out of internal buffer
     * (starting at offset pos and ending at or before offset end),
     * consuming no more than utfLen bytes. Appends read characters to
     * sbuf. Returns the number of bytes consumed.
     *
     * @param sbuf String builder.
     * @param utfLen UTF encoding length.
     * @return Number of bytes consumed.
     * @throws IOException In case of error.
     */
    @SuppressWarnings("ThrowFromFinallyBlock")
    private long readUTFSpan(StringBuilder sbuf, long utfLen) throws IOException {
        int cpos = 0;
        int start = pos;
        int avail = Math.min(end - pos, CHAR_BUF_SIZE);
        int stop = pos + ((utfLen > avail) ? avail - 2 : (int)utfLen);
        boolean outOfBounds = false;

        try {
            while (pos < stop) {
                int b1 = utfBuf[pos++] & 0xFF;

                int b2, b3;

                switch (b1 >> 4) {
                    case 0:
                    case 1:
                    case 2:
                    case 3:
                    case 4:
                    case 5:
                    case 6:
                    case 7:
                        // 1 byte format: 0xxxxxxx
                        urfCBuf[cpos++] = (char)b1;

                        break;

                    case 12:
                    case 13:
                        // 2 byte format: 110xxxxx 10xxxxxx
                        b2 = utfBuf[pos++];

                        if ((b2 & 0xC0) != 0x80)
                            throw new UTFDataFormatException();

                        urfCBuf[cpos++] = (char)(((b1 & 0x1F) << 6) | (b2 & 0x3F));

                        break;

                    case 14:
                        // 3 byte format: 1110xxxx 10xxxxxx 10xxxxxx
                        b3 = utfBuf[pos + 1];
                        b2 = utfBuf[pos];

                        pos += 2;

                        if ((b2 & 0xC0) != 0x80 || (b3 & 0xC0) != 0x80)
                            throw new UTFDataFormatException();

                        urfCBuf[cpos++] = (char)(((b1 & 0x0F) << 12) | ((b2 & 0x3F) << 6) | (b3 & 0x3F));

                        break;

                    default:
                        // 10xx xxxx, 1111 xxxx
                        throw new UTFDataFormatException();
                }
            }
        }
        catch (ArrayIndexOutOfBoundsException ignored) {
            outOfBounds = true;
        }
        finally {
            if (outOfBounds || (pos - start) > utfLen) {
                pos = start + (int)utfLen;

                throw new UTFDataFormatException();
            }
        }

        sbuf.append(urfCBuf, 0, cpos);

        return pos - start;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridUnsafeDataInput.class, this);
    }
}
