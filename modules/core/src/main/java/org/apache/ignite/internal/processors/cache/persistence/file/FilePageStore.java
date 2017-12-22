/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.file;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.IgniteDataIntegrityViolationException;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.PureJavaCrc32;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_SKIP_CRC;

/**
 * File page store.
 */
public class FilePageStore implements PageStore {
    /** Page store file signature. */
    private static final long SIGNATURE = 0xF19AC4FE60C530B8L;

    /** File version. */
    public static final int VERSION = 1;

    /** Allocated field offset. */
    public static final int HEADER_SIZE = 8/*SIGNATURE*/ + 4/*VERSION*/ + 1/*type*/ + 4/*page size*/;

    /** */
    private final File cfgFile;

    /** */
    private final byte type;

    /** Database configuration. */
    protected final DataStorageConfiguration dbCfg;

    /** Factory to provide I/O interfaces for read/write operations with files */
    private final FileIOFactory ioFactory;

    /** I/O interface for read/write operations with file */
    private FileIO fileIO;

    /** */
    private final AtomicLong allocated;

    /** */
    private final int pageSize;

    /** */
    private volatile boolean inited;

    /** */
    private volatile boolean recover;

    /** Partition file version, 1-based incrementing counter. For outdated pages tag has low value, and write does nothing */
    private volatile int tag;

    /** */
    private boolean skipCrc = IgniteSystemProperties.getBoolean(IGNITE_PDS_SKIP_CRC, false);

    /** */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * @param file File.
     */
    public FilePageStore(byte type, File file, FileIOFactory factory, DataStorageConfiguration cfg) {
        this.type = type;

        cfgFile = file;
        dbCfg = cfg;
        ioFactory = factory;

        allocated = new AtomicLong();

        pageSize = dbCfg.getPageSize();
    }

    /** {@inheritDoc} */
    @Override public boolean exists() {
        return cfgFile.exists() && cfgFile.length() > headerSize();
    }

    /**
     * Size of page store header.
     */
    public int headerSize() {
        return HEADER_SIZE;
    }

    /**
     * Page store version.
     */
    public int version() {
        return VERSION;
    }

    /**
     * Creates header for current version file store. Doesn't init the store.
     *
     * @param type Type.
     * @param pageSize Page size.
     * @return Byte buffer instance.
     */
    public ByteBuffer header(byte type, int pageSize) {
        ByteBuffer hdr = ByteBuffer.allocate(headerSize()).order(ByteOrder.LITTLE_ENDIAN);

        hdr.putLong(SIGNATURE);

        hdr.putInt(version());

        hdr.put(type);

        hdr.putInt(pageSize);

        hdr.rewind();

        return hdr;
    }

    /**
     *
     */
    private long initFile() {
        try {
            ByteBuffer hdr = header(type, dbCfg.getPageSize());

            while (hdr.remaining() > 0)
                fileIO.write(hdr);
        }
        catch (IOException e) {
            throw new IgniteException("Check file failed.", e);
        }

        //there is 'super' page in every file
        return headerSize() + dbCfg.getPageSize();
    }

    /**
     *
     */
    private long checkFile() throws IgniteCheckedException {
        try {
            ByteBuffer hdr = ByteBuffer.allocate(headerSize()).order(ByteOrder.LITTLE_ENDIAN);

            while (hdr.remaining() > 0)
                fileIO.read(hdr);

            hdr.rewind();

            long signature = hdr.getLong();

            if (SIGNATURE != signature)
                throw new IgniteCheckedException("Failed to verify store file (invalid file signature)" +
                    " [expectedSignature=" + U.hexLong(SIGNATURE) +
                    ", actualSignature=" + U.hexLong(signature) + ']');

            int ver = hdr.getInt();

            if (version() != ver)
                throw new IgniteCheckedException("Failed to verify store file (invalid file version)" +
                    " [expectedVersion=" + version() +
                    ", fileVersion=" + ver + "]");

            byte type = hdr.get();

            if (this.type != type)
                throw new IgniteCheckedException("Failed to verify store file (invalid file type)" +
                    " [expectedFileType=" + this.type +
                    ", actualFileType=" + type + "]");

            int pageSize = hdr.getInt();

            if (dbCfg.getPageSize() != pageSize)
                throw new IgniteCheckedException("Failed to verify store file (invalid page size)" +
                    " [expectedPageSize=" + dbCfg.getPageSize() +
                    ", filePageSize=" + pageSize + "]");

            long fileSize = cfgFile.length();

            if (fileSize == headerSize()) // Every file has a special meta page.
                fileSize = pageSize + headerSize();

            if ((fileSize - headerSize()) % pageSize != 0)
                throw new IgniteCheckedException("Failed to verify store file (invalid file size)" +
                    " [fileSize=" + U.hexLong(fileSize) +
                    ", pageSize=" + U.hexLong(pageSize) + ']');

            return fileSize;
        }
        catch (IOException e) {
            throw new IgniteCheckedException("File check failed", e);
        }
    }

    /**
     * @param cleanFile {@code True} to delete file.
     * @throws IgniteCheckedException If failed.
     */
    public void stop(boolean cleanFile) throws IgniteCheckedException {
        lock.writeLock().lock();

        try {
            if (!inited)
                return;

            fileIO.force();

            fileIO.close();

            if (cleanFile)
                cfgFile.delete();
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     *
     */
    public void truncate(int tag) throws IgniteCheckedException {
        lock.writeLock().lock();

        try {
            if (!inited)
                return;

            this.tag = tag;

            fileIO.clear();

            allocated.set(initFile());
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     *
     */
    public void beginRecover() {
        lock.writeLock().lock();

        try {
            recover = true;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     *
     */
    public void finishRecover() {
        lock.writeLock().lock();

        try {
            if (inited)
                allocated.set(fileIO.size());

            recover = false;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void read(long pageId, ByteBuffer pageBuf, boolean keepCrc) throws IgniteCheckedException {
        init();

        try {
            long off = pageOffset(pageId);

            assert pageBuf.capacity() == pageSize;
            assert pageBuf.position() == 0;
            assert pageBuf.order() == ByteOrder.nativeOrder();

            int len = pageSize;

            do {
                int n = fileIO.read(pageBuf, off);

                // If page was not written yet, nothing to read.
                if (n < 0) {
                    pageBuf.put(new byte[pageBuf.remaining()]);

                    return;
                }

                off += n;

                len -= n;
            }
            while (len > 0);

            int savedCrc32 = PageIO.getCrc(pageBuf);

            PageIO.setCrc(pageBuf, 0);

            pageBuf.position(0);

            if (!skipCrc) {
                int curCrc32 = PureJavaCrc32.calcCrc32(pageBuf, pageSize);

                if ((savedCrc32 ^ curCrc32) != 0)
                    throw new IgniteDataIntegrityViolationException("Failed to read page (CRC validation failed) " +
                        "[id=" + U.hexLong(pageId) + ", off=" + (off - pageSize) +
                        ", file=" + cfgFile.getAbsolutePath() + ", fileSize=" + fileIO.size() +
                        ", savedCrc=" + U.hexInt(savedCrc32) + ", curCrc=" + U.hexInt(curCrc32) + "]");
            }

            assert PageIO.getCrc(pageBuf) == 0;

            if (keepCrc)
                PageIO.setCrc(pageBuf, savedCrc32);
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Read error", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void readHeader(ByteBuffer buf) throws IgniteCheckedException {
        init();

        try {
            assert buf.remaining() == headerSize();

            int len = headerSize();

            long off = 0;

            do {
                int n = fileIO.read(buf, off);

                // If page was not written yet, nothing to read.
                if (n < 0)
                    return;

                off += n;

                len -= n;
            }
            while (len > 0);
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Read error", e);
        }
    }

    /**
     * @throws IgniteCheckedException If failed to initialize store file.
     */
    private void init() throws IgniteCheckedException {
        if (!inited) {
            lock.writeLock().lock();

            try {
                if (!inited) {
                    FileIO fileIO = null;

                    IgniteCheckedException err = null;

                    try {
                        this.fileIO = fileIO = ioFactory.create(cfgFile, CREATE, READ, WRITE);

                        if (cfgFile.length() == 0)
                            allocated.set(initFile());
                        else
                            allocated.set(checkFile());

                        inited = true;
                    }
                    catch (IOException e) {
                        throw err = new IgniteCheckedException("Can't open file: " + cfgFile.getName(), e);
                    }
                    finally {
                        if (err != null && fileIO != null)
                            try {
                                fileIO.close();
                            }
                            catch (IOException e) {
                                err.addSuppressed(e);
                            }
                    }
                }
            }
            finally {
                lock.writeLock().unlock();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void write(long pageId, ByteBuffer pageBuf, int tag, boolean calculateCrc) throws IgniteCheckedException {
        init();

        lock.readLock().lock();

        try {
            if (tag < this.tag)
                return;

            long off = pageOffset(pageId);

            assert (off >= 0 && off + pageSize <= allocated.get() + headerSize()) || recover :
                "off=" + U.hexLong(off) + ", allocated=" + U.hexLong(allocated.get()) + ", pageId=" + U.hexLong(pageId);

            assert pageBuf.capacity() == pageSize;
            assert pageBuf.position() == 0;
            assert pageBuf.order() == ByteOrder.nativeOrder();
            assert PageIO.getType(pageBuf) != 0 : "Invalid state. Type is 0! pageId = " + U.hexLong(pageId);
            assert PageIO.getVersion(pageBuf) != 0 : "Invalid state. Version is 0! pageId = " + U.hexLong(pageId);

            if (calculateCrc && !skipCrc) {
                assert PageIO.getCrc(pageBuf) == 0 : U.hexLong(pageId);

                PageIO.setCrc(pageBuf, calcCrc32(pageBuf, pageSize));
            }

            // Check whether crc was calculated somewhere above the stack if it is forcibly skipped.
            assert skipCrc || PageIO.getCrc(pageBuf) != 0 || calcCrc32(pageBuf, pageSize) == 0 :
                    "CRC hasn't been calculated, crc=0";

            assert pageBuf.position() == 0 : pageBuf.position();

            int len = pageSize;

            do {
                int n = fileIO.write(pageBuf, off);

                off += n;

                len -= n;
            }
            while (len > 0);

            PageIO.setCrc(pageBuf, 0);
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to write the page to the file store [pageId=" + pageId +
                ", file=" + cfgFile.getAbsolutePath() + ']', e);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * @param pageBuf Page buffer.
     * @param pageSize Page size.
     */
    private static int calcCrc32(ByteBuffer pageBuf, int pageSize) {
        try {
            pageBuf.position(0);

            return PureJavaCrc32.calcCrc32(pageBuf, pageSize);
        }
        finally {
            pageBuf.position(0);
        }
    }

    /** {@inheritDoc} */
    @Override public long pageOffset(long pageId) {
        return (long) PageIdUtils.pageIndex(pageId) * pageSize + headerSize();
    }

    /** {@inheritDoc} */
    @Override public void sync() throws IgniteCheckedException {
        lock.writeLock().lock();

        try {
            init();

            fileIO.force();
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Sync error", e);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized void ensure() throws IgniteCheckedException {
        init();
    }

    /** {@inheritDoc} */
    @Override public long allocatePage() throws IgniteCheckedException {
        init();

        long off = allocPage();

        return (off - headerSize()) / pageSize;
    }

    /**
     *
     */
    private long allocPage() {
        long off;

        do {
            off = allocated.get();

            if (allocated.compareAndSet(off, off + pageSize))
                break;
        }
        while (true);

        return off;
    }

    /** {@inheritDoc} */
    @Override public int pages() {
        if (!inited)
            return 0;

        return (int)((allocated.get() - headerSize()) / pageSize);
    }
}
