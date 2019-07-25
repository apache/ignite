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

package org.apache.ignite.internal.processors.cache.persistence.backup;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Path;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.zip.CRC32;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class DeltaPagesStorage implements Closeable {
    /** Ignite logger to use. */
    private final IgniteLogger log;

    /** Configuration file path provider. */
    private final Supplier<Path> cfgPath;

    /** Factory to produce an IO interface over underlying file. */
    private final FileIOFactory factory;

    /** Storage size. */
    private final LongAdder storageSize = new LongAdder();

    /** Page size of stored pages. */
    private final int pageSize;

    /** Buse lock to perform write opertions. */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** IO over the underlying file */
    private volatile FileIO fileIo;

    /** Allow write to storage flag. */
    private volatile boolean writable = true;

    // TODO create a mask based on total allocated pages within this partition.

    /**
     * @param log Ignite logger to use.
     * @param cfgPath Configuration file path provider.
     * @param factory Factory to produce an IO interface over underlying file.
     * @param pageSize Page size of stored pages.
     */
    public DeltaPagesStorage(IgniteLogger log, Supplier<Path> cfgPath, FileIOFactory factory, int pageSize) {
        A.notNull(cfgPath, "Configurations path cannot be empty");
        A.notNull(factory, "File configuration factory cannot be empty");

        this.log = log.getLogger(DeltaPagesStorage.class);
        this.cfgPath = cfgPath;
        this.factory = factory;
        this.pageSize = pageSize;
    }

    /**
     * @throws IgniteCheckedException If failed to initialize store file.
     */
    public DeltaPagesStorage init() throws IgniteCheckedException {
        if (fileIo != null)
            return this;

        IgniteCheckedException err = null;
        FileIO fileIo = null;

        lock.writeLock().lock();

        try {
            boolean interrupted = false;

            while (true) {
                try {
                    fileIo = factory.create(cfgPath.get().toFile());

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
            err = new IgniteCheckedException("Failed to initialize backup partition file: " +
                cfgPath.get().toAbsolutePath(), e);

            throw err;
        }
        finally {
            if (err == null)
                this.fileIo = fileIo;
            else
                U.closeQuiet(fileIo);

            lock.writeLock().unlock();
        }

        return this;
    }

    /**
     * @param pageId Page ID.
     * @param pageBuf Page buffer to write.
     * @throws IOException If page writing failed (IO error occurred).
     */
    public void write(long pageId, ByteBuffer pageBuf, long off) throws IOException {
        if (fileIo == null)
            return;

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
                ", crcPage=" + crc +
                ", pageOffset=" + off + ']');

            pageBuf.rewind();

            // Write buffer to the end of the file.
            fileIo.writeFully(pageBuf);

            storageSize.add(pageBuf.capacity());
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * @param store File page store to apply deltas to.
     */
    public void apply(PageStore store) throws IOException, IgniteCheckedException {
        assert !writable;
        assert fileIo != null;

        // Will perform a copy delta file page by page simultaneously with merge pages operation.
        ByteBuffer pageBuff = ByteBuffer.allocate(pageSize);

        pageBuff.clear();

        long readed;
        long position = 0;
        long size = storageSize.sum();

        while ((readed = fileIo.readFully(pageBuff, position)) > 0 && position < size) {
            position += readed;

            pageBuff.flip();

            long pageId = PageIO.getPageId(pageBuff);
            long pageOffset = store.pageOffset(pageId);
            int crc32 = FastCrc.calcCrc(new CRC32(), pageBuff, pageBuff.limit());
            int crc = PageIO.getCrc(pageBuff);

            U.log(log, "handle partition delta [pageId=" + pageId +
                    ", pageOffset=" + pageOffset +
                    ", partSize=" + store.size() +
                    ", skipped=" + (pageOffset >= store.size()) +
                    ", position=" + position +
                    ", size=" + size +
                    ", crcBuff=" + crc32 +
                    ", crcPage=" + crc + ']');

            pageBuff.rewind();

            // Other pages are not related to handled partition file and must be ignored.
            if (pageOffset < store.size())
                store.write(pageId, pageBuff, 0, false);

            pageBuff.clear();
        }
    }

    /**
     * @return {@code true} if writes to the storage is allowed.
     */
    public boolean writable() {
        return writable;
    }

    /**
     * @param writable {@code true} if writes to the storage is allowed.
     */
    public void writable(boolean writable) {
        this.writable = writable;
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
