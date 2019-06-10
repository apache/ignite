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
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.CRC32;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public class PartitionDeltaPageStore implements Closeable {
    /** */
    private final File file;

    /** */
    private final FileIOFactory factory;

    /** */
    private final int pageSize;

    /** */
    private final Set<Long> writtenPagesCount = new HashSet<>();

    /** */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** */
    private final Object mux = new Object();

    /** */
    private volatile FileIO fileIO;

    /** */
    private volatile boolean init;

    /** */
    private volatile boolean writable = true;

    /**
     * @param file File to store.
     * @param factory Facotry.
     */
    public PartitionDeltaPageStore(File file, FileIOFactory factory, int pageSize) {
        this.file = file;
        this.factory = factory;
        this.pageSize = pageSize;
    }

    /**
     * @throws IgniteCheckedException If failed to initialize store file.
     */
    public void init() throws IgniteCheckedException {
        if (init)
            return;

        synchronized (mux) {
            if (init)
                return;

            FileIO fileIO = null;
            IgniteCheckedException err = null;

            try {
                boolean interrupted = false;

                while (true) {
                    try {
                        this.fileIO = fileIO = factory.create(file);

                        if (interrupted)
                            Thread.currentThread().interrupt();

                        break;
                    }
                    catch (ClosedByInterruptException e) {
                        interrupted = true;

                        Thread.interrupted();
                    }
                }

                init = true;
            }
            catch (IOException e) {
                err = new IgniteCheckedException("Failed to initialize backup partition file: " +
                    file.getAbsolutePath(), e);

                throw err;
            }
            finally {
                if (err != null)
                    U.closeQuiet(fileIO);
            }
        }
    }

    /**
     * @param pageId Page ID.
     * @param pageBuf Page buffer to write.
     * @throws IgniteCheckedException If page writing failed (IO error occurred).
     */
    public void write(long pageId, ByteBuffer pageBuf) throws IgniteCheckedException {
        init();

        if (!writable())
            return;

        //TODO write pages for parallel backup processes
        if (writtenPagesCount.contains(pageId))
            return;

        synchronized (mux) {
            if (writtenPagesCount.add(pageId)) {
                try {
                    assert pageBuf.position() == 0;
                    assert pageBuf.order() == ByteOrder.nativeOrder() : "Page buffer order " + pageBuf.order()
                        + " should be same with " + ByteOrder.nativeOrder();
                    assert pageBuf.limit() == pageSize : pageBuf.limit();
                    assert PageIdUtils.flag(pageId) == PageMemory.FLAG_DATA;

                    int crc = PageIO.getCrc(pageBuf);
                    int crc32 = FastCrc.calcCrc(new CRC32(), pageBuf, pageBuf.limit());

                    // TODO remove debug
                    System.out.println("onPageWrite [pageId=" + pageId +
                        ", pageIdBuff=" + PageIO.getPageId(pageBuf) +
                        ", part=" + file.getName() +
                        ", fileSize=" + fileIO.size() +
                        ", crcBuff=" + crc32 +
                        ", crcPage=" + crc +
                        ", pageOffset=" + pageOffset(pageId) + ']');

                    pageBuf.rewind();

                    // Write buffer to the end of the file.
                    fileIO.writeFully(pageBuf);
                }
                catch (IOException e) {
                    writtenPagesCount.remove(pageId);

                    throw new IgniteCheckedException("Backup write failed.", e);
                }
            }
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
        synchronized (mux) {
            this.writable = writable;
        }
    }

    /**
     * @param pageId Page id to evaluate.
     * @return Page id offset.
     */
    public long pageOffset(long pageId) {
        return (long)PageIdUtils.pageIndex(pageId) * pageSize + pageSize;
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void truncate() throws IgniteCheckedException {
        synchronized (mux) {
            try {
                writtenPagesCount.clear();

                if (fileIO != null)
                    fileIO.clear();
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Truncate store failed", e);
            }
        }
    }

    /**
     * @return The value of pages successfully written to the temporary store.
     */
    public int writtenPagesCount() {
        synchronized (mux) {
            return writtenPagesCount.size();
        }
    }

    /**
     * @return Partition resource file link.
     */
    public File getFile() {
        return file;
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        if (!init)
            return;

        synchronized (mux) {
            fileIO.close();

            fileIO = null;

            Files.delete(file.toPath());
        }
    }
}
