/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.WeakReference;
import java.lang.reflect.Method;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.NonWritableChannelException;
import java.util.concurrent.TimeUnit;

import org.h2.engine.SysProperties;

/**
 * This file system stores files on disk and uses java.nio to access the files.
 * This class used memory mapped files.
 */
public class FilePathNioMapped extends FilePathNio {

    @Override
    public FileChannel open(String mode) throws IOException {
        return new FileNioMapped(name.substring(getScheme().length() + 1), mode);
    }

    @Override
    public String getScheme() {
        return "nioMapped";
    }

}

/**
 * Uses memory mapped files.
 * The file size is limited to 2 GB.
 */
class FileNioMapped extends FileBase {

    private static final long GC_TIMEOUT_MS = 10_000;
    private final String name;
    private final MapMode mode;
    private RandomAccessFile file;
    private MappedByteBuffer mapped;
    private long fileLength;

    /**
     * The position within the file. Can't use the position of the mapped buffer
     * because it doesn't support seeking past the end of the file.
     */
    private int pos;

    FileNioMapped(String fileName, String mode) throws IOException {
        if ("r".equals(mode)) {
            this.mode = MapMode.READ_ONLY;
        } else {
            this.mode = MapMode.READ_WRITE;
        }
        this.name = fileName;
        file = new RandomAccessFile(fileName, mode);
        reMap();
    }

    private void unMap() throws IOException {
        if (mapped == null) {
            return;
        }
        // first write all data
        mapped.force();

        // need to dispose old direct buffer, see bug
        // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4724038

        boolean useSystemGc = true;
        if (SysProperties.NIO_CLEANER_HACK) {
            try {
                Method cleanerMethod = mapped.getClass().getMethod("cleaner");
                cleanerMethod.setAccessible(true);
                Object cleaner = cleanerMethod.invoke(mapped);
                if (cleaner != null) {
                    Method clearMethod = cleaner.getClass().getMethod("clean");
                    clearMethod.invoke(cleaner);
                }
                useSystemGc = false;
            } catch (Throwable e) {
                // useSystemGc is already true
            } finally {
                mapped = null;
            }
        }
        if (useSystemGc) {
            WeakReference<MappedByteBuffer> bufferWeakRef =
                    new WeakReference<>(mapped);
            mapped = null;
            long start = System.nanoTime();
            while (bufferWeakRef.get() != null) {
                if (System.nanoTime() - start > TimeUnit.MILLISECONDS.toNanos(GC_TIMEOUT_MS)) {
                    throw new IOException("Timeout (" + GC_TIMEOUT_MS
                            + " ms) reached while trying to GC mapped buffer");
                }
                System.gc();
                Thread.yield();
            }
        }
    }

    /**
     * Re-map byte buffer into memory, called when file size has changed or file
     * was created.
     */
    private void reMap() throws IOException {
        int oldPos = 0;
        if (mapped != null) {
            oldPos = pos;
            unMap();
        }
        fileLength = file.length();
        checkFileSizeLimit(fileLength);
        // maps new MappedByteBuffer; the old one is disposed during GC
        mapped = file.getChannel().map(mode, 0, fileLength);
        int limit = mapped.limit();
        int capacity = mapped.capacity();
        if (limit < fileLength || capacity < fileLength) {
            throw new IOException("Unable to map: length=" + limit +
                    " capacity=" + capacity + " length=" + fileLength);
        }
        if (SysProperties.NIO_LOAD_MAPPED) {
            mapped.load();
        }
        this.pos = Math.min(oldPos, (int) fileLength);
    }

    private static void checkFileSizeLimit(long length) throws IOException {
        if (length > Integer.MAX_VALUE) {
            throw new IOException(
                    "File over 2GB is not supported yet when using this file system");
        }
    }

    @Override
    public void implCloseChannel() throws IOException {
        if (file != null) {
            unMap();
            file.close();
            file = null;
        }
    }

    @Override
    public long position() {
        return pos;
    }

    @Override
    public String toString() {
        return "nioMapped:" + name;
    }

    @Override
    public synchronized long size() throws IOException {
        return fileLength;
    }

    @Override
    public synchronized int read(ByteBuffer dst) throws IOException {
        try {
            int len = dst.remaining();
            if (len == 0) {
                return 0;
            }
            len = (int) Math.min(len, fileLength - pos);
            if (len <= 0) {
                return -1;
            }
            mapped.position(pos);
            mapped.get(dst.array(), dst.arrayOffset() + dst.position(), len);
            dst.position(dst.position() + len);
            pos += len;
            return len;
        } catch (IllegalArgumentException e) {
            EOFException e2 = new EOFException("EOF");
            e2.initCause(e);
            throw e2;
        } catch (BufferUnderflowException e) {
            EOFException e2 = new EOFException("EOF");
            e2.initCause(e);
            throw e2;
        }
    }

    @Override
    public FileChannel position(long pos) throws IOException {
        checkFileSizeLimit(pos);
        this.pos = (int) pos;
        return this;
    }

    @Override
    public synchronized FileChannel truncate(long newLength) throws IOException {
        // compatibility with JDK FileChannel#truncate
        if (mode == MapMode.READ_ONLY) {
            throw new NonWritableChannelException();
        }
        if (newLength < size()) {
            setFileLength(newLength);
        }
        return this;
    }

    public synchronized void setFileLength(long newLength) throws IOException {
        checkFileSizeLimit(newLength);
        int oldPos = pos;
        unMap();
        for (int i = 0;; i++) {
            try {
                file.setLength(newLength);
                break;
            } catch (IOException e) {
                if (i > 16 || !e.toString().contains("user-mapped section open")) {
                    throw e;
                }
            }
            System.gc();
        }
        reMap();
        pos = (int) Math.min(newLength, oldPos);
    }

    @Override
    public void force(boolean metaData) throws IOException {
        mapped.force();
        file.getFD().sync();
    }

    @Override
    public synchronized int write(ByteBuffer src) throws IOException {
        int len = src.remaining();
        // check if need to expand file
        if (mapped.capacity() < pos + len) {
            setFileLength(pos + len);
        }
        mapped.position(pos);
        mapped.put(src);
        pos += len;
        return len;
    }

    @Override
    public synchronized FileLock tryLock(long position, long size,
            boolean shared) throws IOException {
        return file.getChannel().tryLock(position, size, shared);
    }

}
