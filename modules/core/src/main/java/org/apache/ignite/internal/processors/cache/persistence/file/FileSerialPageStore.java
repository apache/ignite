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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.zip.CRC32;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class FileSerialPageStore implements Closeable {
    /** Ignite logger to use. */
    @GridToStringExclude
    private final IgniteLogger log;

    /** Configuration file path provider. */
    private final Supplier<Path> cfgPath;

    /** Factory to produce an IO interface over underlying file. */
    @GridToStringExclude
    private final FileIOFactory factory;

    /** Storage size. */
    private final AtomicLong pages = new AtomicLong();

    /** Page size of stored pages. */
    private final int pageSize;

    /** Buse lock to perform write opertions. */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** IO over the underlying file */
    private volatile FileIO fileIo;

    /** Allow write to storage flag. */
    private volatile boolean writable = true;

    /**
     * @param log Ignite logger to use.
     * @param cfgPath Configuration file path provider.
     * @param factory Factory to produce an IO interface over underlying file.
     * @param pageSize Page size of stored pages.
     */
    public FileSerialPageStore(IgniteLogger log, Supplier<Path> cfgPath, FileIOFactory factory, int pageSize) {
        A.notNull(cfgPath, "Configurations path cannot be empty");
        A.notNull(factory, "File configuration factory cannot be empty");

        this.log = log.getLogger(FileSerialPageStore.class);
        this.cfgPath = cfgPath;
        this.factory = factory;
        this.pageSize = pageSize;
    }

    /**
     * @throws IOException If failed to initialize store file.
     */
    public FileSerialPageStore init() throws IOException {
        if (fileIo == null)
            fileIo = factory.create(cfgPath.get().toFile());

        return this;
    }

    /**
     * @param pageId Page ID.
     * @param pageBuf Page buffer to write.
     * @throws IOException If page writing failed (IO error occurred).
     */
    public void writePage(long pageId, ByteBuffer pageBuf) throws IOException {
        assert fileIo != null : "Delta pages storage is not inited: " + this;

        if (!writable())
            return;

        if (!lock.readLock().tryLock())
            return;

        try {
            assert pageBuf.position() == 0;
            assert pageBuf.order() == ByteOrder.nativeOrder() : "Page buffer order " + pageBuf.order()
                + " should be same with " + ByteOrder.nativeOrder();
            assert PageIdUtils.flag(pageId) == PageMemory.FLAG_DATA;

            int crc = PageIO.getCrc(pageBuf);
            int crc32 = FastCrc.calcCrc(new CRC32(), pageBuf, pageBuf.limit());

            // TODO remove debug
            System.out.println("onPageWrite [pageId=" + pageId +
                ", pageIdBuff=" + PageIO.getPageId(pageBuf) +
                ", part=" + cfgPath.get().toAbsolutePath() +
                ", fileSize=" + fileIo.size() +
                ", crcBuff=" + crc32 +
                ", crcPage=" + crc + ']');

            pageBuf.rewind();

            // Write buffer to the end of the file.
            fileIo.writeFully(pageBuf);

            pages.incrementAndGet();
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * @param pageBuf Buffer to read page into.
     * @param seq Page sequence in serial storage.
     * @throws IgniteCheckedException If fails.
     */
    public void readPage(ByteBuffer pageBuf, long seq) throws IgniteCheckedException {
        assert fileIo != null : cfgPath.get();
        assert pageBuf.capacity() == pageSize : pageBuf.capacity();
        assert pageBuf.order() == ByteOrder.nativeOrder() : pageBuf.order();
        assert pageBuf.position() == 0 : pageBuf.position();

        lock.readLock().lock();

        try {
            long readed = fileIo.readFully(pageBuf, seq * pageSize);

            assert readed == pageBuf.capacity();

            pageBuf.flip();

            long pageId = PageIO.getPageId(pageBuf);
            int crc32 = FastCrc.calcCrc(new CRC32(), pageBuf, pageBuf.limit());
            int crc = PageIO.getCrc(pageBuf);

            U.log(log, "Read page from serial storage [path=" + cfgPath.get().toFile().getName() +
                ", pageId=" + pageId + ", seq=" + seq + ", pages=" + pages.get() + ", crcBuff=" + crc32 +
                ", crcPage=" + crc + ']');

            pageBuf.rewind();
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Error reading page from serial storage [seq=" + seq + ']');
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * @return {@code true} if writes to the storage is allowed.
     */
    public boolean writable() {
        return writable;
    }

    /**
     * Disable page writing to this storage.
     */
    public void disableWrites() {
        writable = false;
    }

    /**
     * @return Total number of pages for this serial page storage.
     */
    public long pages() {
        return pages.get();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(FileSerialPageStore.class, this);
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        if (fileIo == null)
            return;

        lock.writeLock().lock();

        try {
            U.closeQuiet(fileIo);
        }
        finally {
            fileIo = null;

            lock.writeLock().unlock();
        }

    }
}
