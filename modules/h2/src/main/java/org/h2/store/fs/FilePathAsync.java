/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.NonWritableChannelException;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * This file system stores files on disk and uses
 * java.nio.channels.AsynchronousFileChannel to access the files.
 */
public class FilePathAsync extends FilePathWrapper {

    private static final boolean AVAILABLE;

    /*
     * Android has NIO2 only since API 26.
     */
    static {
        boolean a = false;
        try {
            AsynchronousFileChannel.class.getName();
            a = true;
        } catch (Throwable e) {
            // Nothing to do
        }
        AVAILABLE = a;
    }

    /**
     * Creates new instance of FilePathAsync.
     */
    public FilePathAsync() {
        if (!AVAILABLE) {
            throw new UnsupportedOperationException("NIO2 is not available");
        }
    }

    @Override
    public FileChannel open(String mode) throws IOException {
        return new FileAsync(name.substring(getScheme().length() + 1), mode);
    }

    @Override
    public String getScheme() {
        return "async";
    }

}

/**
 * File which uses NIO2 AsynchronousFileChannel.
 */
class FileAsync extends FileBase {

    private static final OpenOption[] R = { StandardOpenOption.READ };

    private static final OpenOption[] W = { StandardOpenOption.READ, StandardOpenOption.WRITE,
            StandardOpenOption.CREATE };

    private static final OpenOption[] RWS = { StandardOpenOption.READ, StandardOpenOption.WRITE,
            StandardOpenOption.CREATE, StandardOpenOption.SYNC };

    private static final OpenOption[] RWD = { StandardOpenOption.READ, StandardOpenOption.WRITE,
            StandardOpenOption.CREATE, StandardOpenOption.DSYNC };

    private final String name;

    private final AsynchronousFileChannel channel;

    private long position;

    private static <T> T complete(Future<T> future) throws IOException {
        boolean interrupted = false;
        for (;;) {
            try {
                T result = future.get();
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
                return result;
            } catch (InterruptedException e) {
                interrupted = true;
            } catch (ExecutionException e) {
                throw new IOException(e.getCause());
            }
        }
    }

    FileAsync(String fileName, String mode) throws IOException {
        this.name = fileName;
        OpenOption[] options;
        switch (mode) {
        case "r":
            options = R;
            break;
        case "rw":
            options = W;
            break;
        case "rws":
            options = RWS;
            break;
        case "rwd":
            options = RWD;
            break;
        default:
            throw new IllegalArgumentException(mode);
        }
        channel = AsynchronousFileChannel.open(Paths.get(fileName), options);
    }

    @Override
    public void implCloseChannel() throws IOException {
        channel.close();
    }

    @Override
    public long position() throws IOException {
        return position;
    }

    @Override
    public long size() throws IOException {
        return channel.size();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        int read = complete(channel.read(dst, position));
        if (read > 0) {
            position += read;
        }
        return read;
    }

    @Override
    public FileChannel position(long pos) throws IOException {
        if (pos < 0) {
            throw new IllegalArgumentException();
        }
        position = pos;
        return this;
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        return complete(channel.read(dst, position));
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        try {
            return complete(channel.write(src, position));
        } catch (NonWritableChannelException e) {
            throw new IOException("read only");
        }
    }

    @Override
    public FileChannel truncate(long newLength) throws IOException {
        channel.truncate(newLength);
        if (newLength < position) {
            position = newLength;
        }
        return this;
    }

    @Override
    public void force(boolean metaData) throws IOException {
        channel.force(metaData);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        int written;
        try {
            written = complete(channel.write(src, position));
            position += written;
        } catch (NonWritableChannelException e) {
            throw new IOException("read only");
        }
        return written;
    }

    @Override
    public synchronized FileLock tryLock(long position, long size, boolean shared) throws IOException {
        return channel.tryLock(position, size, shared);
    }

    @Override
    public String toString() {
        return "async:" + name;
    }

}
