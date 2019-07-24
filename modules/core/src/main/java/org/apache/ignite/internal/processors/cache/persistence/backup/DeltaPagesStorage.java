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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.zip.CRC32;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public class DeltaPagesStorage implements Closeable {
    /** */
    private final Supplier<Path> cfgPath;

    /** */
    private final FileIOFactory factory;

    /** */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** */
    private volatile FileIO fileIo;

    /** */
    private volatile boolean writable = true;


    // TODO create a mask based on total allocated pages within this partition.

    /**
     * @param factory Facotry.
     */
    public DeltaPagesStorage(Supplier<Path> cfgPath, FileIOFactory factory) {
        A.notNull(cfgPath, "Configurations path cannot be empty");
        A.notNull(factory, "File configuration factory cannot be empty");

        this.cfgPath = cfgPath;
        this.factory = factory;
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
