/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import org.h2.util.IOUtils;

/**
 * This class is backed by an input stream and supports reading values and
 * variable size data.
 */
public class DataReader extends Reader {

    private final InputStream in;

    /**
     * Create a new data reader.
     *
     * @param in the input stream
     */
    public DataReader(InputStream in) {
        this.in = in;
    }

    /**
     * Read a byte.
     *
     * @return the byte
     */
    public byte readByte() throws IOException {
        int x = in.read();
        if (x < 0) {
            throw new FastEOFException();
        }
        return (byte) x;
    }

    /**
     * Read a variable size integer.
     *
     * @return the value
     */
    public int readVarInt() throws IOException {
        int b = readByte();
        if (b >= 0) {
            return b;
        }
        int x = b & 0x7f;
        b = readByte();
        if (b >= 0) {
            return x | (b << 7);
        }
        x |= (b & 0x7f) << 7;
        b = readByte();
        if (b >= 0) {
            return x | (b << 14);
        }
        x |= (b & 0x7f) << 14;
        b = readByte();
        if (b >= 0) {
            return x | b << 21;
        }
        return x | ((b & 0x7f) << 21) | (readByte() << 28);
    }

    /**
     * Read a variable size long.
     *
     * @return the value
     */
    public long readVarLong() throws IOException {
        long x = readByte();
        if (x >= 0) {
            return x;
        }
        x &= 0x7f;
        for (int s = 7;; s += 7) {
            long b = readByte();
            x |= (b & 0x7f) << s;
            if (b >= 0) {
                return x;
            }
        }
    }

    /**
     * Read an integer.
     *
     * @return the value
     */
    // public int readInt() throws IOException {
    //     return (read() << 24) + ((read() & 0xff) << 16) +
    //             ((read() & 0xff) << 8) + (read() & 0xff);
    //}

    /**
     * Read a long.
     *
     * @return the value
     */
    // public long readLong() throws IOException {
    //    return ((long) (readInt()) << 32) + (readInt() & 0xffffffffL);
    // }

    /**
     * Read a number of bytes.
     *
     * @param buff the target buffer
     * @param len the number of bytes to read
     */
    public void readFully(byte[] buff, int len) throws IOException {
        int got = IOUtils.readFully(in, buff, len);
        if (got < len) {
            throw new FastEOFException();
        }
    }

    /**
     * Read a string from the stream.
     *
     * @return the string
     */
    public String readString() throws IOException {
        int len = readVarInt();
        return readString(len);
    }

    private String readString(int len) throws IOException {
        char[] chars = new char[len];
        for (int i = 0; i < len; i++) {
            chars[i] = readChar();
        }
        return new String(chars);
    }

    /**
     * Read one character from the input stream.
     *
     * @return the character
     */
    private char readChar() throws IOException {
        int x = readByte() & 0xff;
        if (x < 0x80) {
            return (char) x;
        } else if (x >= 0xe0) {
            return (char) (((x & 0xf) << 12) +
                    ((readByte() & 0x3f) << 6) +
                    (readByte() & 0x3f));
        } else {
            return (char) (((x & 0x1f) << 6) +
                    (readByte() & 0x3f));
        }
    }

    @Override
    public void close() throws IOException {
        // ignore
    }

    @Override
    public int read(char[] buff, int off, int len) throws IOException {
        if (len == 0) {
            return 0;
        }
        int i = 0;
        try {
            for (; i < len; i++) {
                buff[off + i] = readChar();
            }
            return len;
        } catch (EOFException e) {
            if (i == 0) {
                return -1;
            }
            return i;
        }
    }

    /**
     * Constructing such an EOF exception is fast, because the stack trace is
     * not filled in. If used in a static context, this will also avoid
     * classloader memory leaks.
     */
    static class FastEOFException extends EOFException {

        private static final long serialVersionUID = 1L;

        @Override
        public synchronized Throwable fillInStackTrace() {
            return null;
        }

    }

}
