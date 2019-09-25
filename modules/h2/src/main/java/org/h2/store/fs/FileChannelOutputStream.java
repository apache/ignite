/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Allows to write to a file channel like an output stream.
 */
public class FileChannelOutputStream extends OutputStream {

    private final FileChannel channel;
    private final byte[] buffer = { 0 };

    /**
     * Create a new file object output stream from the file channel.
     *
     * @param channel the file channel
     * @param append true for append mode, false for truncate and overwrite
     */
    public FileChannelOutputStream(FileChannel channel, boolean append)
            throws IOException {
        this.channel = channel;
        if (append) {
            channel.position(channel.size());
        } else {
            channel.position(0);
            channel.truncate(0);
        }
    }

    @Override
    public void write(int b) throws IOException {
        buffer[0] = (byte) b;
        FileUtils.writeFully(channel, ByteBuffer.wrap(buffer));
    }

    @Override
    public void write(byte[] b) throws IOException {
        FileUtils.writeFully(channel, ByteBuffer.wrap(b));
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        FileUtils.writeFully(channel, ByteBuffer.wrap(b, off, len));
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

}
