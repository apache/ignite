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
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import org.h2.api.ErrorCode;
import org.h2.compress.CompressLZF;
import org.h2.message.DbException;
import org.h2.util.MathUtils;
import org.h2.util.New;

/**
 * This file system keeps files fully in memory. There is an option to compress
 * file blocks to save memory.
 */
public class FilePathMem extends FilePath {

    private static final TreeMap<String, FileMemData> MEMORY_FILES =
            new TreeMap<>();
    private static final FileMemData DIRECTORY = new FileMemData("", false);

    @Override
    public FilePathMem getPath(String path) {
        FilePathMem p = new FilePathMem();
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
            if (!atomicReplace && !newName.name.equals(name) &&
                    MEMORY_FILES.containsKey(newName.name)) {
                throw DbException.get(ErrorCode.FILE_RENAME_FAILED_2, name, newName + " (exists)");
            }
            FileMemData f = getMemoryFile();
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
            FileMemData old = MEMORY_FILES.remove(name);
            if (old != null) {
                old.truncate(0);
            }
        }
    }

    @Override
    public List<FilePath> newDirectoryStream() {
        ArrayList<FilePath> list = New.arrayList();
        synchronized (MEMORY_FILES) {
            for (String n : MEMORY_FILES.tailMap(name).keySet()) {
                if (n.startsWith(name)) {
                    if (!n.equals(name) && n.indexOf('/', name.length() + 1) < 0) {
                        list.add(getPath(n));
                    }
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
    public FilePathMem getParent() {
        int idx = name.lastIndexOf('/');
        return idx < 0 ? null : getPath(name.substring(0, idx));
    }

    @Override
    public boolean isDirectory() {
        if (isRoot()) {
            return true;
        }
        synchronized (MEMORY_FILES) {
            FileMemData d = MEMORY_FILES.get(name);
            return d == DIRECTORY;
        }
    }

    @Override
    public boolean isAbsolute() {
        // TODO relative files are not supported
        return true;
    }

    @Override
    public FilePathMem toRealPath() {
        return this;
    }

    @Override
    public long lastModified() {
        return getMemoryFile().getLastModified();
    }

    @Override
    public void createDirectory() {
        if (exists()) {
            throw DbException.get(ErrorCode.FILE_CREATION_FAILED_1,
                    name + " (a file with this name already exists)");
        }
        synchronized (MEMORY_FILES) {
            MEMORY_FILES.put(name, DIRECTORY);
        }
    }

    @Override
    public OutputStream newOutputStream(boolean append) throws IOException {
        FileMemData obj = getMemoryFile();
        FileMem m = new FileMem(obj, false);
        return new FileChannelOutputStream(m, append);
    }

    @Override
    public InputStream newInputStream() {
        FileMemData obj = getMemoryFile();
        FileMem m = new FileMem(obj, true);
        return new FileChannelInputStream(m, true);
    }

    @Override
    public FileChannel open(String mode) {
        FileMemData obj = getMemoryFile();
        return new FileMem(obj, "r".equals(mode));
    }

    private FileMemData getMemoryFile() {
        synchronized (MEMORY_FILES) {
            FileMemData m = MEMORY_FILES.get(name);
            if (m == DIRECTORY) {
                throw DbException.get(ErrorCode.FILE_CREATION_FAILED_1,
                        name + " (a directory with this name already exists)");
            }
            if (m == null) {
                m = new FileMemData(name, compressed());
                MEMORY_FILES.put(name, m);
            }
            return m;
        }
    }

    private boolean isRoot() {
        return name.equals(getScheme() + ":");
    }

    /**
     * Get the canonical path for this file name.
     *
     * @param fileName the file name
     * @return the canonical path
     */
    protected static String getCanonicalPath(String fileName) {
        fileName = fileName.replace('\\', '/');
        int idx = fileName.indexOf(':') + 1;
        if (fileName.length() > idx && fileName.charAt(idx) != '/') {
            fileName = fileName.substring(0, idx) + "/" + fileName.substring(idx);
        }
        return fileName;
    }

    @Override
    public String getScheme() {
        return "memFS";
    }

    /**
     * Whether the file should be compressed.
     *
     * @return if it should be compressed.
     */
    boolean compressed() {
        return false;
    }

}

/**
 * A memory file system that compresses blocks to conserve memory.
 */
class FilePathMemLZF extends FilePathMem {

    @Override
    public FilePathMem getPath(String path) {
        FilePathMemLZF p = new FilePathMemLZF();
        p.name = getCanonicalPath(path);
        return p;
    }

    @Override
    boolean compressed() {
        return true;
    }

    @Override
    public String getScheme() {
        return "memLZF";
    }

}

/**
 * This class represents an in-memory file.
 */
class FileMem extends FileBase {

    /**
     * The file data.
     */
    final FileMemData data;

    private final boolean readOnly;
    private long pos;

    FileMem(FileMemData data, boolean readOnly) {
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
        this.pos = newPos;
        return this;
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        int len = src.remaining();
        if (len == 0) {
            return 0;
        }
        data.touch(readOnly);
        data.readWrite(position, src.array(),
                src.arrayOffset() + src.position(), len, true);
        src.position(src.position() + len);
        return len;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        int len = src.remaining();
        if (len == 0) {
            return 0;
        }
        data.touch(readOnly);
        pos = data.readWrite(pos, src.array(),
                src.arrayOffset() + src.position(), len, true);
        src.position(src.position() + len);
        return len;
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        int len = dst.remaining();
        if (len == 0) {
            return 0;
        }
        long newPos = data.readWrite(position, dst.array(),
                dst.arrayOffset() + dst.position(), len, false);
        len = (int) (newPos - position);
        if (len <= 0) {
            return -1;
        }
        dst.position(dst.position() + len);
        return len;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        int len = dst.remaining();
        if (len == 0) {
            return 0;
        }
        long newPos = data.readWrite(pos, dst.array(),
                dst.arrayOffset() + dst.position(), len, false);
        len = (int) (newPos - pos);
        if (len <= 0) {
            return -1;
        }
        dst.position(dst.position() + len);
        pos = newPos;
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
class FileMemData {

    private static final int CACHE_SIZE = 8;
    private static final int BLOCK_SIZE_SHIFT = 10;
    private static final int BLOCK_SIZE = 1 << BLOCK_SIZE_SHIFT;
    private static final int BLOCK_SIZE_MASK = BLOCK_SIZE - 1;
    private static final CompressLZF LZF = new CompressLZF();
    private static final byte[] BUFFER = new byte[BLOCK_SIZE * 2];
    private static final byte[] COMPRESSED_EMPTY_BLOCK;

    private static final Cache<CompressItem, CompressItem> COMPRESS_LATER =
        new Cache<>(CACHE_SIZE);

    private String name;
    private final int id;
    private final boolean compress;
    private long length;
    private AtomicReference<byte[]>[] data;
    private long lastModified;
    private boolean isReadOnly;
    private boolean isLockedExclusive;
    private int sharedLockCount;

    static {
        byte[] n = new byte[BLOCK_SIZE];
        int len = LZF.compress(n, BLOCK_SIZE, BUFFER, 0);
        COMPRESSED_EMPTY_BLOCK = Arrays.copyOf(BUFFER, len);
    }

    @SuppressWarnings("unchecked")
    FileMemData(String name, boolean compress) {
        this.name = name;
        this.id = name.hashCode();
        this.compress = compress;
        this.data = new AtomicReference[0];
        lastModified = System.currentTimeMillis();
    }

    /**
     * Get the page if it exists.
     *
     * @param page the page id
     * @return the byte array, or null
     */
    byte[] getPage(int page) {
        AtomicReference<byte[]>[] b = data;
        if (page >= b.length) {
            return null;
        }
        return b[page].get();
    }

    /**
     * Set the page data.
     *
     * @param page the page id
     * @param oldData the old data
     * @param newData the new data
     * @param force whether the data should be overwritten even if the old data
     *            doesn't match
     */
    void setPage(int page, byte[] oldData, byte[] newData, boolean force) {
        AtomicReference<byte[]>[] b = data;
        if (page >= b.length) {
            return;
        }
        if (force) {
            b[page].set(newData);
        } else {
            b[page].compareAndSet(oldData, newData);
        }
    }

    int getId() {
        return id;
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
    static class Cache<K, V> extends LinkedHashMap<K, V> {

        private static final long serialVersionUID = 1L;
        private final int size;

        Cache(int size) {
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
            c.file.compress(c.page);
            return true;
        }
    }

    /**
     * Points to a block of bytes that needs to be compressed.
     */
    static class CompressItem {

        /**
         * The file.
         */
        FileMemData file;

        /**
         * The page to compress.
         */
        int page;

        @Override
        public int hashCode() {
            return page ^ file.getId();
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof CompressItem) {
                CompressItem c = (CompressItem) o;
                return c.page == page && c.file == file;
            }
            return false;
        }

    }

    private void compressLater(int page) {
        CompressItem c = new CompressItem();
        c.file = this;
        c.page = page;
        synchronized (LZF) {
            COMPRESS_LATER.put(c, c);
        }
    }

    private byte[] expand(int page) {
        byte[] d = getPage(page);
        if (d.length == BLOCK_SIZE) {
            return d;
        }
        byte[] out = new byte[BLOCK_SIZE];
        if (d != COMPRESSED_EMPTY_BLOCK) {
            synchronized (LZF) {
                LZF.expand(d, 0, d.length, out, 0, BLOCK_SIZE);
            }
        }
        setPage(page, d, out, false);
        return out;
    }

    /**
     * Compress the data in a byte array.
     *
     * @param page which page to compress
     */
    void compress(int page) {
        byte[] old = getPage(page);
        if (old == null || old.length != BLOCK_SIZE) {
            // not yet initialized or already compressed
            return;
        }
        synchronized (LZF) {
            int len = LZF.compress(old, BLOCK_SIZE, BUFFER, 0);
            if (len <= BLOCK_SIZE) {
                byte[] d = Arrays.copyOf(BUFFER, len);
                // maybe data was changed in the meantime
                setPage(page, old, d, false);
            }
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
        changeLength(newLength);
        long end = MathUtils.roundUpLong(newLength, BLOCK_SIZE);
        if (end != newLength) {
            int lastPage = (int) (newLength >>> BLOCK_SIZE_SHIFT);
            byte[] d = expand(lastPage);
            byte[] d2 = Arrays.copyOf(d, d.length);
            for (int i = (int) (newLength & BLOCK_SIZE_MASK); i < BLOCK_SIZE; i++) {
                d2[i] = 0;
            }
            setPage(lastPage, d, d2, true);
            if (compress) {
                compressLater(lastPage);
            }
        }
    }

    private void changeLength(long len) {
        length = len;
        len = MathUtils.roundUpLong(len, BLOCK_SIZE);
        int blocks = (int) (len >>> BLOCK_SIZE_SHIFT);
        if (blocks != data.length) {
            AtomicReference<byte[]>[] n = Arrays.copyOf(data, blocks);
            for (int i = data.length; i < blocks; i++) {
                n[i] = new AtomicReference<>(COMPRESSED_EMPTY_BLOCK);
            }
            data = n;
        }
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
    long readWrite(long pos, byte[] b, int off, int len, boolean write) {
        long end = pos + len;
        if (end > length) {
            if (write) {
                changeLength(end);
            } else {
                len = (int) (length - pos);
            }
        }
        while (len > 0) {
            int l = (int) Math.min(len, BLOCK_SIZE - (pos & BLOCK_SIZE_MASK));
            int page = (int) (pos >>> BLOCK_SIZE_SHIFT);
            byte[] block = expand(page);
            int blockOffset = (int) (pos & BLOCK_SIZE_MASK);
            if (write) {
                byte[] p2 = Arrays.copyOf(block, block.length);
                System.arraycopy(b, off, p2, blockOffset, l);
                setPage(page, block, p2, true);
            } else {
                System.arraycopy(block, blockOffset, b, off, l);
            }
            if (compress) {
                compressLater(page);
            }
            off += l;
            pos += l;
            len -= l;
        }
        return pos;
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


