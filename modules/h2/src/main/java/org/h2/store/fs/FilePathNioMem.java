/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.NonWritableChannelException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.h2.api.ErrorCode;
import org.h2.compress.CompressLZF;
import org.h2.message.DbException;
import org.h2.util.MathUtils;
import org.h2.util.New;

/**
 * This file system keeps files fully in memory. There is an option to compress
 * file blocks to save memory.
 */
public class FilePathNioMem extends FilePath {

    private static final TreeMap<String, FileNioMemData> MEMORY_FILES =
            new TreeMap<>();

    /**
     * The percentage of uncompressed (cached) entries.
     */
    float compressLaterCachePercent = 1;

    @Override
    public FilePathNioMem getPath(String path) {
        FilePathNioMem p = new FilePathNioMem();
        p.name = getCanonicalPath(path);
        return p;
    }

    @Override
    public long size() {
        return getMemoryFile().length();
    }

    @Override
    public void moveTo(FilePath newName, boolean atomicReplace) {
        synchronized (MEMORY_FILES) {
            if (!atomicReplace && !name.equals(newName.name) &&
                    MEMORY_FILES.containsKey(newName.name)) {
                throw DbException.get(ErrorCode.FILE_RENAME_FAILED_2, name, newName + " (exists)");
            }
            FileNioMemData f = getMemoryFile();
            f.setName(newName.name);
            MEMORY_FILES.remove(name);
            MEMORY_FILES.put(newName.name, f);
        }
    }

    @Override
    public boolean createFile() {
        synchronized (MEMORY_FILES) {
            if (exists()) {
                return false;
            }
            getMemoryFile();
        }
        return true;
    }

    @Override
    public boolean exists() {
        if (isRoot()) {
            return true;
        }
        synchronized (MEMORY_FILES) {
            return MEMORY_FILES.get(name) != null;
        }
    }

    @Override
    public void delete() {
        if (isRoot()) {
            return;
        }
        synchronized (MEMORY_FILES) {
            MEMORY_FILES.remove(name);
        }
    }

    @Override
    public List<FilePath> newDirectoryStream() {
        ArrayList<FilePath> list = New.arrayList();
        synchronized (MEMORY_FILES) {
            for (String n : MEMORY_FILES.tailMap(name).keySet()) {
                if (n.startsWith(name)) {
                    list.add(getPath(n));
                } else {
                    break;
                }
            }
            return list;
        }
    }

    @Override
    public boolean setReadOnly() {
        return getMemoryFile().setReadOnly();
    }

    @Override
    public boolean canWrite() {
        return getMemoryFile().canWrite();
    }

    @Override
    public FilePathNioMem getParent() {
        int idx = name.lastIndexOf('/');
        return idx < 0 ? null : getPath(name.substring(0, idx));
    }

    @Override
    public boolean isDirectory() {
        if (isRoot()) {
            return true;
        }
        // TODO in memory file system currently
        // does not really support directories
        synchronized (MEMORY_FILES) {
            return MEMORY_FILES.get(name) == null;
        }
    }

    @Override
    public boolean isAbsolute() {
        // TODO relative files are not supported
        return true;
    }

    @Override
    public FilePathNioMem toRealPath() {
        return this;
    }

    @Override
    public long lastModified() {
        return getMemoryFile().getLastModified();
    }

    @Override
    public void createDirectory() {
        if (exists() && isDirectory()) {
            throw DbException.get(ErrorCode.FILE_CREATION_FAILED_1,
                    name + " (a file with this name already exists)");
        }
        // TODO directories are not really supported
    }

    @Override
    public OutputStream newOutputStream(boolean append) throws IOException {
        FileNioMemData obj = getMemoryFile();
        FileNioMem m = new FileNioMem(obj, false);
        return new FileChannelOutputStream(m, append);
    }

    @Override
    public InputStream newInputStream() {
        FileNioMemData obj = getMemoryFile();
        FileNioMem m = new FileNioMem(obj, true);
        return new FileChannelInputStream(m, true);
    }

    @Override
    public FileChannel open(String mode) {
        FileNioMemData obj = getMemoryFile();
        return new FileNioMem(obj, "r".equals(mode));
    }

    private FileNioMemData getMemoryFile() {
        synchronized (MEMORY_FILES) {
            FileNioMemData m = MEMORY_FILES.get(name);
            if (m == null) {
                m = new FileNioMemData(name, compressed(), compressLaterCachePercent);
                MEMORY_FILES.put(name, m);
            }
            return m;
        }
    }

    protected boolean isRoot() {
        return name.equals(getScheme() + ":");
    }

    /**
     * Get the canonical path of a file (with backslashes replaced with forward
     * slashes).
     *
     * @param fileName the file name
     * @return the canonical path
     */
    protected static String getCanonicalPath(String fileName) {
        fileName = fileName.replace('\\', '/');
        int idx = fileName.lastIndexOf(':') + 1;
        if (fileName.length() > idx && fileName.charAt(idx) != '/') {
            fileName = fileName.substring(0, idx) + "/" + fileName.substring(idx);
        }
        return fileName;
    }

    @Override
    public String getScheme() {
        return "nioMemFS";
    }

    /**
     * Whether the file should be compressed.
     *
     * @return true if it should be compressed.
     */
    boolean compressed() {
        return false;
    }

}

/**
 * A memory file system that compresses blocks to conserve memory.
 */
class FilePathNioMemLZF extends FilePathNioMem {

    @Override
    boolean compressed() {
        return true;
    }

    @Override
    public FilePathNioMem getPath(String path) {
        if (!path.startsWith(getScheme())) {
            throw new IllegalArgumentException(path +
                    " doesn't start with " + getScheme());
        }
        int idx1 = path.indexOf(':');
        int idx2 = path.lastIndexOf(':');
        final FilePathNioMemLZF p = new FilePathNioMemLZF();
        if (idx1 != -1 && idx1 != idx2) {
            p.compressLaterCachePercent = Float.parseFloat(path.substring(idx1 + 1, idx2));
        }
        p.name = getCanonicalPath(path);
        return p;
    }

    @Override
    protected boolean isRoot() {
        return name.lastIndexOf(':') == name.length() - 1;
    }

    @Override
    public String getScheme() {
        return "nioMemLZF";
    }

}

/**
 * This class represents an in-memory file.
 */
class FileNioMem extends FileBase {

    /**
     * The file data.
     */
    final FileNioMemData data;

    private final boolean readOnly;
    private long pos;

    FileNioMem(FileNioMemData data, boolean readOnly) {
        this.data = data;
        this.readOnly = readOnly;
    }

    @Override
    public long size() {
        return data.length();
    }

    @Override
    public FileChannel truncate(long newLength) throws IOException {
        // compatibility with JDK FileChannel#truncate
        if (readOnly) {
            throw new NonWritableChannelException();
        }
        if (newLength < size()) {
            data.touch(readOnly);
            pos = Math.min(pos, newLength);
            data.truncate(newLength);
        }
        return this;
    }

    @Override
    public FileChannel position(long newPos) {
        this.pos = (int) newPos;
        return this;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        int len = src.remaining();
        if (len == 0) {
            return 0;
        }
        data.touch(readOnly);
        // offset is 0 because we start writing from src.position()
        pos = data.readWrite(pos, src, 0, len, true);
        src.position(src.position() + len);
        return len;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        int len = dst.remaining();
        if (len == 0) {
            return 0;
        }
        long newPos = data.readWrite(pos, dst, dst.position(), len, false);
        len = (int) (newPos - pos);
        if (len <= 0) {
            return -1;
        }
        dst.position(dst.position() + len);
        pos = newPos;
        return len;
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        int len = dst.remaining();
        if (len == 0) {
            return 0;
        }
        long newPos;
        newPos = data.readWrite(position, dst, dst.position(), len, false);
        len = (int) (newPos - position);
        if (len <= 0) {
            return -1;
        }
        dst.position(dst.position() + len);
        return len;
    }

    @Override
    public long position() {
        return pos;
    }

    @Override
    public void implCloseChannel() throws IOException {
        pos = 0;
    }

    @Override
    public void force(boolean metaData) throws IOException {
        // do nothing
    }

    @Override
    public synchronized FileLock tryLock(long position, long size,
            boolean shared) throws IOException {
        if (shared) {
            if (!data.lockShared()) {
                return null;
            }
        } else {
            if (!data.lockExclusive()) {
                return null;
            }
        }

        return new FileLock(new FakeFileChannel(), position, size, shared) {

            @Override
            public boolean isValid() {
                return true;
            }

            @Override
            public void release() throws IOException {
                data.unlock();
            }
        };
    }

    @Override
    public String toString() {
        return data.getName();
    }

}

/**
 * This class contains the data of an in-memory random access file.
 * Data compression using the LZF algorithm is supported as well.
 */
class FileNioMemData {

    private static final int CACHE_MIN_SIZE = 8;
    private static final int BLOCK_SIZE_SHIFT = 16;

    private static final int BLOCK_SIZE = 1 << BLOCK_SIZE_SHIFT;
    private static final int BLOCK_SIZE_MASK = BLOCK_SIZE - 1;
    private static final ByteBuffer COMPRESSED_EMPTY_BLOCK;

    private static final ThreadLocal<CompressLZF> LZF_THREAD_LOCAL =
            new ThreadLocal<CompressLZF>() {
        @Override
        protected CompressLZF initialValue() {
            return new CompressLZF();
        }
    };
    /** the output buffer when compressing */
    private static final ThreadLocal<byte[] > COMPRESS_OUT_BUF_THREAD_LOCAL =
            new ThreadLocal<byte[] >() {
        @Override
        protected byte[] initialValue() {
            return new byte[BLOCK_SIZE * 2];
        }
    };

    /**
     * The hash code of the name.
     */
    final int nameHashCode;

    private final CompressLaterCache<CompressItem, CompressItem> compressLaterCache =
        new CompressLaterCache<>(CACHE_MIN_SIZE);

    private String name;
    private final boolean compress;
    private final float compressLaterCachePercent;
    private long length;
    private AtomicReference<ByteBuffer>[] buffers;
    private long lastModified;
    private boolean isReadOnly;
    private boolean isLockedExclusive;
    private int sharedLockCount;
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    static {
        final byte[] n = new byte[BLOCK_SIZE];
        final byte[] output = new byte[BLOCK_SIZE * 2];
        int len = new CompressLZF().compress(n, BLOCK_SIZE, output, 0);
        COMPRESSED_EMPTY_BLOCK = ByteBuffer.allocateDirect(len);
        COMPRESSED_EMPTY_BLOCK.put(output, 0, len);
    }

    @SuppressWarnings("unchecked")
    FileNioMemData(String name, boolean compress, float compressLaterCachePercent) {
        this.name = name;
        this.nameHashCode = name.hashCode();
        this.compress = compress;
        this.compressLaterCachePercent = compressLaterCachePercent;
        buffers = new AtomicReference[0];
        lastModified = System.currentTimeMillis();
    }

    /**
     * Lock the file in exclusive mode if possible.
     *
     * @return if locking was successful
     */
    synchronized boolean lockExclusive() {
        if (sharedLockCount > 0 || isLockedExclusive) {
            return false;
        }
        isLockedExclusive = true;
        return true;
    }

    /**
     * Lock the file in shared mode if possible.
     *
     * @return if locking was successful
     */
    synchronized boolean lockShared() {
        if (isLockedExclusive) {
            return false;
        }
        sharedLockCount++;
        return true;
    }

    /**
     * Unlock the file.
     */
    synchronized void unlock() {
        if (isLockedExclusive) {
            isLockedExclusive = false;
        } else {
            sharedLockCount = Math.max(0, sharedLockCount - 1);
        }
    }

    /**
     * This small cache compresses the data if an element leaves the cache.
     */
    static class CompressLaterCache<K, V> extends LinkedHashMap<K, V> {

        private static final long serialVersionUID = 1L;
        private int size;

        CompressLaterCache(int size) {
            super(size, (float) 0.75, true);
            this.size = size;
        }

        @Override
        public synchronized V put(K key, V value) {
            return super.put(key, value);
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            if (size() < size) {
                return false;
            }
            CompressItem c = (CompressItem) eldest.getKey();
            c.data.compressPage(c.page);
            return true;
        }

        public void setCacheSize(int size) {
            this.size = size;
        }
    }

    /**
     * Represents a compressed item.
     */
    static class CompressItem {

        /**
         * The file data.
         */
        public final FileNioMemData data;

        /**
         * The page to compress.
         */
        public final int page;

        public CompressItem(FileNioMemData data, int page) {
            this.data = data;
            this.page = page;
        }

        @Override
        public int hashCode() {
            return page ^ data.nameHashCode;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof CompressItem) {
                CompressItem c = (CompressItem) o;
                return c.data == data && c.page == page;
            }
            return false;
        }

    }

    private void addToCompressLaterCache(int page) {
        CompressItem c = new CompressItem(this, page);
        compressLaterCache.put(c, c);
    }

    private ByteBuffer expandPage(int page) {
        final ByteBuffer d = buffers[page].get();
        if (d.capacity() == BLOCK_SIZE) {
            // already expanded, or not compressed
            return d;
        }
        synchronized (d) {
            if (d.capacity() == BLOCK_SIZE) {
                return d;
            }
            ByteBuffer out = ByteBuffer.allocateDirect(BLOCK_SIZE);
            if (d != COMPRESSED_EMPTY_BLOCK) {
                d.position(0);
                CompressLZF.expand(d, out);
            }
            buffers[page].compareAndSet(d, out);
            return out;
        }
    }

    /**
     * Compress the data in a byte array.
     *
     * @param page which page to compress
     */
    void compressPage(int page) {
        final ByteBuffer d = buffers[page].get();
        synchronized (d) {
            if (d.capacity() != BLOCK_SIZE) {
                // already compressed
                return;
            }
            final byte[] compressOutputBuffer = COMPRESS_OUT_BUF_THREAD_LOCAL.get();
            int len = LZF_THREAD_LOCAL.get().compress(d, 0, compressOutputBuffer, 0);
            ByteBuffer out = ByteBuffer.allocateDirect(len);
            out.put(compressOutputBuffer, 0, len);
            buffers[page].compareAndSet(d, out);
        }
    }

    /**
     * Update the last modified time.
     *
     * @param openReadOnly if the file was opened in read-only mode
     */
    void touch(boolean openReadOnly) throws IOException {
        if (isReadOnly || openReadOnly) {
            throw new IOException("Read only");
        }
        lastModified = System.currentTimeMillis();
    }

    /**
     * Get the file length.
     *
     * @return the length
     */
    long length() {
        return length;
    }

    /**
     * Truncate the file.
     *
     * @param newLength the new length
     */
    void truncate(long newLength) {
        rwLock.writeLock().lock();
        try {
            changeLength(newLength);
            long end = MathUtils.roundUpLong(newLength, BLOCK_SIZE);
            if (end != newLength) {
                int lastPage = (int) (newLength >>> BLOCK_SIZE_SHIFT);
                ByteBuffer d = expandPage(lastPage);
                for (int i = (int) (newLength & BLOCK_SIZE_MASK); i < BLOCK_SIZE; i++) {
                    d.put(i, (byte) 0);
                }
                if (compress) {
                    addToCompressLaterCache(lastPage);
                }
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @SuppressWarnings("unchecked")
    private void changeLength(long len) {
        length = len;
        len = MathUtils.roundUpLong(len, BLOCK_SIZE);
        int blocks = (int) (len >>> BLOCK_SIZE_SHIFT);
        if (blocks != buffers.length) {
            final AtomicReference<ByteBuffer>[] newBuffers = new AtomicReference[blocks];
            System.arraycopy(buffers, 0, newBuffers, 0,
                    Math.min(buffers.length, newBuffers.length));
            for (int i = buffers.length; i < blocks; i++) {
                newBuffers[i] = new AtomicReference<>(COMPRESSED_EMPTY_BLOCK);
            }
            buffers = newBuffers;
        }
        compressLaterCache.setCacheSize(Math.max(CACHE_MIN_SIZE, (int) (blocks *
                compressLaterCachePercent / 100)));
    }

    /**
     * Read or write.
     *
     * @param pos the position
     * @param b the byte array
     * @param off the offset within the byte array
     * @param len the number of bytes
     * @param write true for writing
     * @return the new position
     */
    long readWrite(long pos, ByteBuffer b, int off, int len, boolean write) {
        final java.util.concurrent.locks.Lock lock = write ? rwLock.writeLock()
                : rwLock.readLock();
        lock.lock();
        try {

            long end = pos + len;
            if (end > length) {
                if (write) {
                    changeLength(end);
                } else {
                    len = (int) (length - pos);
                }
            }
            while (len > 0) {
                final int l = (int) Math.min(len, BLOCK_SIZE - (pos & BLOCK_SIZE_MASK));
                final int page = (int) (pos >>> BLOCK_SIZE_SHIFT);
                final ByteBuffer block = expandPage(page);
                int blockOffset = (int) (pos & BLOCK_SIZE_MASK);
                if (write) {
                    final ByteBuffer srcTmp = b.slice();
                    final ByteBuffer dstTmp = block.duplicate();
                    srcTmp.position(off);
                    srcTmp.limit(off + l);
                    dstTmp.position(blockOffset);
                    dstTmp.put(srcTmp);
                } else {
                    // duplicate, so this can be done concurrently
                    final ByteBuffer tmp = block.duplicate();
                    tmp.position(blockOffset);
                    tmp.limit(l + blockOffset);
                    int oldPosition = b.position();
                    b.position(off);
                    b.put(tmp);
                    // restore old position
                    b.position(oldPosition);
                }
                if (compress) {
                    addToCompressLaterCache(page);
                }
                off += l;
                pos += l;
                len -= l;
            }
            return pos;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Set the file name.
     *
     * @param name the name
     */
    void setName(String name) {
        this.name = name;
    }

    /**
     * Get the file name
     *
     * @return the name
     */
    String getName() {
        return name;
    }

    /**
     * Get the last modified time.
     *
     * @return the time
     */
    long getLastModified() {
        return lastModified;
    }

    /**
     * Check whether writing is allowed.
     *
     * @return true if it is
     */
    boolean canWrite() {
        return !isReadOnly;
    }

    /**
     * Set the read-only flag.
     *
     * @return true
     */
    boolean setReadOnly() {
        isReadOnly = true;
        return true;
    }

}


