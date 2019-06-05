/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

/**
 * A file system that re-opens and re-tries the operation if the file was
 * closed, because a thread was interrupted. This will clear the interrupt flag.
 * It is mainly useful for applications that call Thread.interrupt by mistake.
 */
public class FilePathRetryOnInterrupt extends FilePathWrapper {

    /**
     * The prefix.
     */
    static final String SCHEME = "retry";

    @Override
    public FileChannel open(String mode) throws IOException {
        return new FileRetryOnInterrupt(name.substring(getScheme().length() + 1), mode);
    }

    @Override
    public String getScheme() {
        return SCHEME;
    }

}

/**
 * A file object that re-opens and re-tries the operation if the file was
 * closed.
 */
class FileRetryOnInterrupt extends FileBase {

    private final String fileName;
    private final String mode;
    private FileChannel channel;
    private FileLockRetry lock;

    FileRetryOnInterrupt(String fileName, String mode) throws IOException {
        this.fileName = fileName;
        this.mode = mode;
        open();
    }

    private void open() throws IOException {
        channel = FileUtils.open(fileName, mode);
    }

    private void reopen(int i, IOException e) throws IOException {
        if (i > 20) {
            throw e;
        }
        if (!(e instanceof ClosedByInterruptException) &&
                !(e instanceof ClosedChannelException)) {
            throw e;
        }
        // clear the interrupt flag, to avoid re-opening many times
        Thread.interrupted();
        FileChannel before = channel;
        // ensure we don't re-open concurrently;
        // sometimes we don't re-open, which is fine,
        // as this method is called in a loop
        synchronized (this) {
            if (before == channel) {
                open();
                reLock();
            }
        }
    }

    private void reLock() throws IOException {
        if (lock == null) {
            return;
        }
        try {
            lock.base.release();
        } catch (IOException e) {
            // ignore
        }
        FileLock l2 = channel.tryLock(lock.position(), lock.size(), lock.isShared());
        if (l2 == null) {
            throw new IOException("Re-locking failed");
        }
        lock.base = l2;
    }

    @Override
    public void implCloseChannel() throws IOException {
        try {
            channel.close();
        } catch (IOException e) {
            // ignore
        }
    }

    @Override
    public long position() throws IOException {
        for (int i = 0;; i++) {
            try {
                return channel.position();
            } catch (IOException e) {
                reopen(i, e);
            }
        }
    }

    @Override
    public long size() throws IOException {
        for (int i = 0;; i++) {
            try {
                return channel.size();
            } catch (IOException e) {
                reopen(i, e);
            }
        }
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        long pos = position();
        for (int i = 0;; i++) {
            try {
                return channel.read(dst);
            } catch (IOException e) {
                reopen(i, e);
                position(pos);
            }
        }
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        for (int i = 0;; i++) {
            try {
                return channel.read(dst, position);
            } catch (IOException e) {
                reopen(i, e);
            }
        }
    }

    @Override
    public FileChannel position(long pos) throws IOException {
        for (int i = 0;; i++) {
            try {
                channel.position(pos);
                return this;
            } catch (IOException e) {
                reopen(i, e);
            }
        }
    }

    @Override
    public FileChannel truncate(long newLength) throws IOException {
        for (int i = 0;; i++) {
            try {
                channel.truncate(newLength);
                return this;
            } catch (IOException e) {
                reopen(i, e);
            }
        }
    }

    @Override
    public void force(boolean metaData) throws IOException {
        for (int i = 0;; i++) {
            try {
                channel.force(metaData);
                return;
            } catch (IOException e) {
                reopen(i, e);
            }
        }
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        long pos = position();
        for (int i = 0;; i++) {
            try {
                return channel.write(src);
            } catch (IOException e) {
                reopen(i, e);
                position(pos);
            }
        }
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        for (int i = 0;; i++) {
            try {
                return channel.write(src, position);
            } catch (IOException e) {
                reopen(i, e);
            }
        }
    }

    @Override
    public synchronized FileLock tryLock(long position, long size,
            boolean shared) throws IOException {
        FileLock l = channel.tryLock(position, size, shared);
        if (l == null) {
            return null;
        }
        lock = new FileLockRetry(l, this);
        return lock;
    }

    /**
     * A wrapped file lock.
     */
    static class FileLockRetry extends FileLock {

        /**
         * The base lock.
         */
        FileLock base;

        protected FileLockRetry(FileLock base, FileChannel channel) {
            super(channel, base.position(), base.size(), base.isShared());
            this.base = base;
        }

        @Override
        public boolean isValid() {
            return base.isValid();
        }

        @Override
        public void release() throws IOException {
            base.release();
        }

    }

    @Override
    public String toString() {
        return FilePathRetryOnInterrupt.SCHEME + ":" + fileName;
    }

}

