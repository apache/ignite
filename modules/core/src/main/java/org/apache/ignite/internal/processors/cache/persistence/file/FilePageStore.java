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
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.pagemem.store.PageWriteListener;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.IgniteDataIntegrityViolationException;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteOutClosure;

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
    private final IgniteOutClosure<Path> pathProvider;

    /**
     * Caches the existence state of storage file. After it is initialized, it will be not set to null
     * during FilePageStore lifecycle.
     */
    private volatile Boolean fileExists;

    /** */
    private final byte type;

    /** Database configuration. */
    protected final DataStorageConfiguration dbCfg;

    /** Factory to provide I/O interfaces for read/write operations with files */
    private final FileIOFactory ioFactory;

    /** I/O interface for read/write operations with file */
    protected volatile FileIO fileIO;

    /** */
    private final AtomicLong allocated;

    /** Region metrics updater. */
    private final LongAdderMetric allocatedTracker;

    /** List of listeners for current page store to handle. */
    private final List<PageWriteListener> lsnrs = new CopyOnWriteArrayList<>();

    /** */
    protected final int pageSize;

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

    /** */
    public FilePageStore(
        byte type,
        IgniteOutClosure<Path> pathProvider,
        FileIOFactory factory,
        DataStorageConfiguration cfg,
        LongAdderMetric allocatedTracker
    ) {
        this.type = type;
        this.pathProvider = pathProvider;
        this.dbCfg = cfg;
        this.ioFactory = factory;
        this.allocated = new AtomicLong();
        this.pageSize = dbCfg.getPageSize();
        this.allocatedTracker = allocatedTracker;
    }

    /** {@inheritDoc} */
    @Override public void addWriteListener(PageWriteListener lsnr) {
        lsnrs.add(lsnr);
    }

    /** {@inheritDoc} */
    @Override public void removeWriteListener(PageWriteListener lsnr) {
        lsnrs.remove(lsnr);
    }

    /** {@inheritDoc} */
    @Override public int getPageSize() {
        return pageSize;
    }

    /** {@inheritDoc} */
    @Override public int getBlockSize() {
        return -1; // Header is unaligned in this version.
    }

    /** {@inheritDoc} */
    @Override public long size() {
        try {
            FileIO io = fileIO;

            return io == null ? 0 : io.size();
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long getSparseSize() {
        return -1;
    }

    /** {@inheritDoc} */
    @Override public void punchHole(long pageId, int usefulBytes) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean exists() {
        if (fileExists == null) {
            lock.writeLock().lock();

            try {
                if (fileExists == null) {
                    File file = pathProvider.apply().toFile();

                    fileExists = file.exists() && file.length() > headerSize();
                }
            }
            finally {
                lock.writeLock().unlock();
            }
        }

        return fileExists;
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
    @Override public int version() {
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
        ByteBuffer hdr = ByteBuffer.allocate(headerSize()).order(ByteOrder.nativeOrder());

        hdr.putLong(SIGNATURE);

        hdr.putInt(version());

        hdr.put(type);

        hdr.putInt(pageSize);

        hdr.rewind();

        return hdr;
    }

    /**
     * Initializes header and writes it into the file store.
     *
     * @return Next available position in the file to store a data.
     * @throws IOException If initialization is failed.
     */
    private long initFile(FileIO fileIO) throws IOException {
        try {
            ByteBuffer hdr = header(type, dbCfg.getPageSize());

            fileIO.writeFully(hdr);

            //there is 'super' page in every file
            return headerSize() + dbCfg.getPageSize();
        }
        catch (ClosedByInterruptException e) {
            // If thread was interrupted written header can be inconsistent.
            lock.writeLock().lock();

            try {
                Files.delete(pathProvider.apply());

                fileExists = false;
            }
            finally {
                lock.writeLock().unlock();
            }

            throw e;
        }
    }

    /**
     * Checks that file store has correct header and size.
     *
     * @return Next available position in the file to store a data.
     * @throws IOException If check has failed.
     */
    private long checkFile(FileIO fileIO, File cfgFile) throws IOException {
        ByteBuffer hdr = ByteBuffer.allocate(headerSize()).order(ByteOrder.nativeOrder());

        fileIO.readFully(hdr);

        hdr.rewind();

        long signature = hdr.getLong();

        String prefix = "Failed to verify, file=" + cfgFile.getAbsolutePath() + "\" ";

        if (SIGNATURE != signature)
            throw new IOException(prefix + "(invalid file signature)" +
                " [expectedSignature=" + U.hexLong(SIGNATURE) +
                ", actualSignature=" + U.hexLong(signature) + ']');

        int ver = hdr.getInt();

        if (version() != ver)
            throw new IOException(prefix + "(invalid file version)" +
                " [expectedVersion=" + version() +
                ", fileVersion=" + ver + "]");

        byte type = hdr.get();

        if (this.type != type)
            throw new IOException(prefix + "(invalid file type)" +
                " [expectedFileType=" + this.type +
                ", actualFileType=" + type + "]");

        int pageSize = hdr.getInt();

        if (dbCfg.getPageSize() != pageSize)
            throw new IOException(prefix + "(invalid page size)" +
                " [expectedPageSize=" + dbCfg.getPageSize() +
                ", filePageSize=" + pageSize + "]");

        long fileSize = cfgFile.length();

        if (fileSize == headerSize()) // Every file has a special meta page.
            fileSize = pageSize + headerSize();

        if (fileSize % pageSize != 0) // In the case of compressed pages we can miss the tail of the page.
            fileSize = (fileSize / pageSize + 1) * pageSize;

        return fileSize;
    }

    /**
     * @param delete {@code True} to delete file.
     * @throws IOException If fails.
     */
    private void stop0(boolean delete) throws IOException {
        lock.writeLock().lock();

        try {
            if (!inited) {
                if (fileIO != null) // Ensure the file is closed even if not initialized yet.
                    fileIO.close();

                if (delete && exists())
                    Files.delete(pathProvider.apply().toAbsolutePath());

                return;
            }

            fileIO.force();

            fileIO.close();

            fileIO = null;

            if (delete) {
                Files.delete(pathProvider.apply());

                fileExists = false;
            }
        }
        finally {
            allocatedTracker.add(-1L * allocated.getAndSet(0) / pageSize);

            inited = false;

            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean delete) throws StorageException {
        try {
            stop0(delete);
        }
        catch (IOException e) {
            throw new StorageException("Failed to stop serving partition file [file=" + getFileAbsolutePath()
                + ", delete=" + delete + "]", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        stop0(false);
    }

    /** {@inheritDoc} */
    @Override public void truncate(int tag) throws StorageException {
        init();

        Path filePath = pathProvider.apply();

        lock.writeLock().lock();

        try {
            this.tag = tag;

            fileIO.clear();

            fileIO.close();

            fileIO = null;

            Files.delete(filePath);

            fileExists = false;
        }
        catch (IOException e) {
            throw new StorageException("Failed to truncate partition file [file=" + filePath.toAbsolutePath() + "]", e);
        }
        finally {
            allocatedTracker.add(-1L * allocated.getAndSet(0) / pageSize);

            inited = false;

            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void beginRecover() {
        lock.writeLock().lock();

        try {
            recover = true;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void finishRecover() throws StorageException {
        lock.writeLock().lock();

        try {
            // Since we always have a meta-page in the store, never revert allocated counter to a value smaller than page.
            if (inited) {
                long newSize = Math.max(pageSize, fileIO.size() - headerSize());

                // In the case of compressed pages we can miss the tail of the page.
                if (newSize % pageSize != 0)
                    newSize += pageSize - newSize % pageSize;

                long delta = newSize - allocated.getAndSet(newSize);

                assert delta % pageSize == 0 : delta;

                allocatedTracker.add(delta / pageSize);
            }

            recover = false;
        }
        catch (IOException e) {
            throw new StorageException("Failed to finish recover partition file [file=" + getFileAbsolutePath() + "]", e);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * @param pageId Page ID.
     * @param pageBuf Page buffer.
     * @return Number of bytes to calculate CRC on.
     */
    private int getCrcSize(long pageId, ByteBuffer pageBuf) throws IOException {
        int compressedSize = PageIO.getCompressedSize(pageBuf);

        if (compressedSize == 0)
            return pageSize; // Page is not compressed.

        if (compressedSize < 0 || compressedSize > pageSize) {
            throw new IgniteDataIntegrityViolationException("Failed to read page (CRC validation failed) " +
                "[id=" + U.hexLong(pageId) + ", file=" + getFileAbsolutePath() + ", fileSize=" + fileIO.size() +
                ", page=" + U.toHexString(pageBuf) + "]");
        }

        return compressedSize;
    }

    /** {@inheritDoc} */
    @Override public boolean read(long pageId, ByteBuffer pageBuf, boolean keepCrc) throws IgniteCheckedException {
        init();

        try {
            long off = pageOffset(pageId);

            assert pageBuf.capacity() == pageSize;
            assert pageBuf.remaining() == pageSize;
            assert pageBuf.position() == 0;
            assert pageBuf.order() == ByteOrder.nativeOrder();
            assert off <= allocated.get() : "calculatedOffset=" + off +
                ", allocated=" + allocated.get() + ", headerSize=" + headerSize() + ", cfgFile=" +
                pathProvider.apply().toAbsolutePath();

            int n = readWithFailover(pageBuf, off);

            // If page was not written yet, nothing to read.
            if (n < 0) {
                pageBuf.put(new byte[pageBuf.remaining()]);

                return false;
            }

            int savedCrc32 = PageIO.getCrc(pageBuf);

            PageIO.setCrc(pageBuf, 0);

            pageBuf.position(0);

            if (!skipCrc) {
                int curCrc32 = FastCrc.calcCrc(pageBuf, getCrcSize(pageId, pageBuf));

                if ((savedCrc32 ^ curCrc32) != 0)
                    throw new IgniteDataIntegrityViolationException("Failed to read page (CRC validation failed) " +
                        "[id=" + U.hexLong(pageId) + ", off=" + (off - pageSize) +
                        ", file=" + getFileAbsolutePath() + ", fileSize=" + fileIO.size() +
                        ", savedCrc=" + U.hexInt(savedCrc32) + ", curCrc=" + U.hexInt(curCrc32) +
                        ", page=" + U.toHexString(pageBuf) +
                        "]");
            }

            assert PageIO.getCrc(pageBuf) == 0;

            if (keepCrc)
                PageIO.setCrc(pageBuf, savedCrc32);

            return true;
        }
        catch (IOException e) {
            throw new StorageException("Failed to read page [file=" + getFileAbsolutePath() + ", pageId=" + pageId + "]", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void readHeader(ByteBuffer buf) throws IgniteCheckedException {
        init();

        try {
            assert buf.remaining() == headerSize();

            readWithFailover(buf, 0);
        }
        catch (IOException e) {
            throw new StorageException("Failed to read header [file=" + getFileAbsolutePath() + "]", e);
        }
    }

    /**
     * @throws StorageException If failed to initialize store file.
     */
    public void init() throws StorageException {
        if (!inited) {
            lock.writeLock().lock();

            try {
                if (!inited) {
                    FileIO fileIO = null;

                    StorageException err = null;

                    long newSize;

                    try {
                        boolean interrupted = false;

                        while (true) {
                            try {
                                File cfgFile = pathProvider.apply().toFile();

                                this.fileIO = fileIO = ioFactory.create(cfgFile, CREATE, READ, WRITE);

                                fileExists = true;

                                newSize = (cfgFile.length() == 0 ? initFile(fileIO) : checkFile(fileIO, cfgFile)) - headerSize();

                                if (interrupted)
                                    Thread.currentThread().interrupt();

                                break;
                            }
                            catch (ClosedByInterruptException e) {
                                interrupted = true;

                                Thread.interrupted();
                            }
                        }

                        assert allocated.get() == 0;

                        allocated.set(newSize);

                        inited = true;

                        // Order is important, update of total allocated pages must be called after allocated update
                        // and setting inited to true, because it affects pages() returned value.
                        allocatedTracker.add(pages());
                    }
                    catch (IOException e) {
                        err = new StorageException(
                            "Failed to initialize partition file: " + getFileAbsolutePath(), e);

                        throw err;
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

    /**
     * Reinit page store after file channel was closed by thread interruption.
     *
     * @param fileIO Old fileIO.
     */
    private void reinit(FileIO fileIO) throws IOException {
        if (!inited)
            return;

        if (fileIO != this.fileIO)
            return;

        lock.writeLock().lock();

        try {
            if (fileIO != this.fileIO)
                return;

            try {
                boolean interrupted = false;

                while (true) {
                    try {
                        fileIO = null;

                        File cfgFile = pathProvider.apply().toFile();

                        fileIO = ioFactory.create(cfgFile, CREATE, READ, WRITE);

                        fileExists = true;

                        checkFile(fileIO, cfgFile);

                        this.fileIO = fileIO;

                        if (interrupted)
                            Thread.currentThread().interrupt();

                        break;
                    }
                    catch (ClosedByInterruptException e) {
                        interrupted = true;

                        Thread.interrupted();
                    }
                }
            }
            catch (IOException e) {
                try {
                    if (fileIO != null)
                        fileIO.close();
                }
                catch (IOException e0) {
                    e.addSuppressed(e0);
                }

                throw e;
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void write(long pageId, ByteBuffer pageBuf, int tag, boolean calculateCrc) throws IgniteCheckedException {
        init();

        boolean interrupted = false;

        while (true) {
            FileIO fileIO = this.fileIO;

            try {
                lock.readLock().lock();

                try {
                    if (tag < this.tag)
                        return;

                    long off = pageOffset(pageId);

                    assert (off >= 0 && off <= allocated.get()) || recover :
                        "off=" + U.hexLong(off) + ", allocated=" + U.hexLong(allocated.get()) +
                            ", pageId=" + U.hexLong(pageId) + ", file=" + getFileAbsolutePath();

                    assert pageBuf.position() == 0;
                    assert pageBuf.order() == ByteOrder.nativeOrder() : "Page buffer order " + pageBuf.order()
                        + " should be same with " + ByteOrder.nativeOrder();
                    assert PageIO.getType(pageBuf) != 0 : "Invalid state. Type is 0! pageId = " + U.hexLong(pageId);
                    assert PageIO.getVersion(pageBuf) != 0 : "Invalid state. Version is 0! pageId = " + U.hexLong(pageId);

                    if (calculateCrc && !skipCrc) {
                        assert PageIO.getCrc(pageBuf) == 0 : U.hexLong(pageId);

                        PageIO.setCrc(pageBuf, calcCrc32(pageBuf, getCrcSize(pageId, pageBuf)));
                    }

                    // Check whether crc was calculated somewhere above the stack if it is forcibly skipped.
                    assert skipCrc || PageIO.getCrc(pageBuf) != 0 || calcCrc32(pageBuf, pageSize) == 0 :
                        "CRC hasn't been calculated, crc=0";

                    assert pageBuf.position() == 0 : pageBuf.position();

                    for (PageWriteListener lsnr : lsnrs) {
                        lsnr.accept(pageId, pageBuf);

                        pageBuf.rewind();
                    }

                    fileIO.writeFully(pageBuf, off);

                    PageIO.setCrc(pageBuf, 0);

                    if (interrupted)
                        Thread.currentThread().interrupt();

                    return;
                }
                finally {
                    lock.readLock().unlock();
                }
            }
            catch (IOException e) {
                if (e instanceof ClosedChannelException) {
                    try {
                        if (e instanceof ClosedByInterruptException) {
                            interrupted = true;

                            Thread.interrupted();
                        }

                        reinit(fileIO);

                        pageBuf.position(0);

                        PageIO.setCrc(pageBuf, 0);

                        continue;
                    }
                    catch (IOException e0) {
                        e0.addSuppressed(e);

                        e = e0;
                    }
                }

                throw new StorageException("Failed to write page [file=" + getFileAbsolutePath()
                    + ", pageId=" + pageId + ", tag=" + tag + "]", e);
            }
        }
    }

    /**
     * @param pageBuf Page buffer.
     * @param pageSize Page size.
     */
    private static int calcCrc32(ByteBuffer pageBuf, int pageSize) {
        try {
            pageBuf.position(0);

            return FastCrc.calcCrc(pageBuf, pageSize);
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
    @Override public void sync() throws StorageException {
        lock.writeLock().lock();

        try {
            init();

            FileIO fileIO = this.fileIO;

            if (fileIO != null)
                fileIO.force();
        }
        catch (IOException e) {
            throw new StorageException("Failed to fsync partition file [file=" + getFileAbsolutePath() + ']', e);
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

        return allocPage() / pageSize;
    }

    /**
     * @return File absolute path.
     */
    public String getFileAbsolutePath() {
        return pathProvider.apply().toAbsolutePath().toString();
    }

    /**
     *
     */
    private long allocPage() {
        long off;

        do {
            off = allocated.get();

            if (allocated.compareAndSet(off, off + pageSize)) {
                allocatedTracker.increment();

                break;
            }
        }
        while (true);

        return off;
    }

    /** {@inheritDoc} */
    @Override public int pages() {
        if (!inited)
            return 0;

        return (int)(allocated.get() / pageSize);
    }

    /**
     * @param destBuf Destination buffer.
     * @param position Position.
     * @return Number of read bytes.
     */
    private int readWithFailover(ByteBuffer destBuf, long position) throws IOException {
        boolean interrupted = false;

        int bufPos = destBuf.position();

        while (true) {
            FileIO fileIO = this.fileIO;

            if (fileIO == null)
                throw new IOException("FileIO has stopped");

            try {
                assert destBuf.remaining() > 0;

                int bytesRead = fileIO.readFully(destBuf, position);

                if (interrupted)
                    Thread.currentThread().interrupt();

                return bytesRead;
            }
            catch (ClosedChannelException e) {
                destBuf.position(bufPos);

                if (e instanceof ClosedByInterruptException) {
                    interrupted = true;

                    Thread.interrupted();
                }

                reinit(fileIO);
            }
        }
    }
}
