/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

import org.h2.engine.Constants;

/**
 * An input stream that reads the data from a reader and limits the number of
 * bytes that can be read.
 */
public class CountingReaderInputStream extends InputStream {

    private final Reader reader;

    private final CharBuffer charBuffer =
            CharBuffer.allocate(Constants.IO_BUFFER_SIZE);

    private final CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder().
            onMalformedInput(CodingErrorAction.REPLACE).
            onUnmappableCharacter(CodingErrorAction.REPLACE);

    private ByteBuffer byteBuffer = ByteBuffer.allocate(0);
    private long length;
    private long remaining;

    CountingReaderInputStream(Reader reader, long maxLength) {
        this.reader = reader;
        this.remaining = maxLength;
    }

    @Override
    public int read(byte[] buff, int offset, int len) throws IOException {
        if (!fetch()) {
            return -1;
        }
        len = Math.min(len, byteBuffer.remaining());
        byteBuffer.get(buff, offset, len);
        return len;
    }

    @Override
    public int read() throws IOException {
        if (!fetch()) {
            return -1;
        }
        return byteBuffer.get() & 255;
    }

    private boolean fetch() throws IOException {
        if (byteBuffer != null && byteBuffer.remaining() == 0) {
            fillBuffer();
        }
        return byteBuffer != null;
    }

    private void fillBuffer() throws IOException {
        int len = (int) Math.min(charBuffer.capacity() - charBuffer.position(),
                remaining);
        if (len > 0) {
            len = reader.read(charBuffer.array(), charBuffer.position(), len);
        }
        if (len > 0) {
            remaining -= len;
        } else {
            len = 0;
            remaining = 0;
        }
        length += len;
        charBuffer.limit(charBuffer.position() + len);
        charBuffer.rewind();
        byteBuffer = ByteBuffer.allocate(Constants.IO_BUFFER_SIZE);
        boolean end = remaining == 0;
        encoder.encode(charBuffer, byteBuffer, end);
        if (end && byteBuffer.position() == 0) {
            // EOF
            byteBuffer = null;
            return;
        }
        byteBuffer.flip();
        charBuffer.compact();
        charBuffer.flip();
        charBuffer.position(charBuffer.limit());
    }

    /**
     * The number of characters read so far (but there might still be some bytes
     * in the buffer).
     *
     * @return the number of characters
     */
    public long getLength() {
        return length;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

}