/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.compress;

import java.io.IOException;
import java.io.InputStream;
import org.h2.message.DbException;
import org.h2.util.Utils;

/**
 * An input stream to read from an LZF stream.
 * The data is automatically expanded.
 */
public class LZFInputStream extends InputStream {

    private final InputStream in;
    private CompressLZF decompress = new CompressLZF();
    private int pos;
    private int bufferLength;
    private byte[] inBuffer;
    private byte[] buffer;

    public LZFInputStream(InputStream in) throws IOException {
        this.in = in;
        if (readInt() != LZFOutputStream.MAGIC) {
            throw new IOException("Not an LZFInputStream");
        }
    }

    private static byte[] ensureSize(byte[] buff, int len) {
        return buff == null || buff.length < len ? Utils.newBytes(len) : buff;
    }

    private void fillBuffer() throws IOException {
        if (buffer != null && pos < bufferLength) {
            return;
        }
        int len = readInt();
        if (decompress == null) {
            // EOF
            this.bufferLength = 0;
        } else if (len < 0) {
            len = -len;
            buffer = ensureSize(buffer, len);
            readFully(buffer, len);
            this.bufferLength = len;
        } else {
            inBuffer = ensureSize(inBuffer, len);
            int size = readInt();
            readFully(inBuffer, len);
            buffer = ensureSize(buffer, size);
            try {
                decompress.expand(inBuffer, 0, len, buffer, 0, size);
            } catch (ArrayIndexOutOfBoundsException e) {
                DbException.convertToIOException(e);
            }
            this.bufferLength = size;
        }
        pos = 0;
    }

    private void readFully(byte[] buff, int len) throws IOException {
        int off = 0;
        while (len > 0) {
            int l = in.read(buff, off, len);
            len -= l;
            off += l;
        }
    }

    private int readInt() throws IOException {
        int x = in.read();
        if (x < 0) {
            decompress = null;
            return 0;
        }
        x = (x << 24) + (in.read() << 16) + (in.read() << 8) + in.read();
        return x;
    }

    @Override
    public int read() throws IOException {
        fillBuffer();
        if (pos >= bufferLength) {
            return -1;
        }
        return buffer[pos++] & 255;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (len == 0) {
            return 0;
        }
        int read = 0;
        while (len > 0) {
            int r = readBlock(b, off, len);
            if (r < 0) {
                break;
            }
            read += r;
            off += r;
            len -= r;
        }
        return read == 0 ? -1 : read;
    }

    private int readBlock(byte[] b, int off, int len) throws IOException {
        fillBuffer();
        if (pos >= bufferLength) {
            return -1;
        }
        int max = Math.min(len, bufferLength - pos);
        max = Math.min(max, b.length - off);
        System.arraycopy(buffer, pos, b, off, max);
        pos += max;
        return max;
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

}
