/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Allows to read from a file channel like an input stream.
 */
public class FileChannelInputStream extends InputStream {

    private final FileChannel channel;
    private final boolean closeChannel;

    private ByteBuffer buffer;
    private long pos;

    /**
     * Create a new file object input stream from the file channel.
     *
     * @param channel the file channel
     * @param closeChannel whether closing the stream should close the channel
     */
    public FileChannelInputStream(FileChannel channel, boolean closeChannel) {
        this.channel = channel;
        this.closeChannel = closeChannel;
    }

    @Override
    public int read() throws IOException {
        if (buffer == null) {
            buffer = ByteBuffer.allocate(1);
        }
        buffer.rewind();
        int len = channel.read(buffer, pos++);
        if (len < 0) {
            return -1;
        }
        return buffer.get(0) & 0xff;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        ByteBuffer buff = ByteBuffer.wrap(b, off, len);
        int read = channel.read(buff, pos);
        if (read == -1) {
            return -1;
        }
        pos += read;
        return read;
    }

    @Override
    public void close() throws IOException {
        if (closeChannel) {
            channel.close();
        }
    }

}
