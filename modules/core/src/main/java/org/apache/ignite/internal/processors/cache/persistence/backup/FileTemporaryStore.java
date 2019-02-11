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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public class FileTemporaryStore implements TemporaryStore {
    /** */
    private final File file;

    /** */
    private final FileIOFactory factory;

    /** */
    private final Set<Long> savedPages = new HashSet<>();

    /** */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** */
    private volatile FileIO fileIO;

    /** */
    private volatile boolean init;

    /**
     * @param file File to store.
     * @param factory Facotry.
     */
    public FileTemporaryStore(File file, FileIOFactory factory) {
        this.file = file;
        this.factory = factory;
    }

    /**
     * @throws IgniteCheckedException If failed to initialize store file.
     */
    public void init() throws IgniteCheckedException {
        if (!init) {
            lock.writeLock().lock();

            try {
                if (!init) {
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
            finally {
                lock.writeLock().unlock();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void read(ByteBuffer pageBuf) throws IgniteCheckedException {
        init();

    }

    /** {@inheritDoc} */
    @Override public void write(long pageId, ByteBuffer pageBuf) throws IgniteCheckedException {
        init();

        if (savedPages.contains(pageId))
            return;

        lock.writeLock().lock();

        try {
            if (savedPages.add(pageId)) {
                try {
                    // Write buffer to the end of the file.
                    fileIO.write(pageBuf);
                }
                catch (IOException e) {
                    savedPages.remove(pageId);

                    throw new IgniteCheckedException("Backup write failed.", e);
                }
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void truncate() throws IgniteCheckedException {
        lock.writeLock().lock();

        try {
            savedPages.clear();

            if (fileIO != null)
                fileIO.clear();
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Truncate store failed", e);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        lock.writeLock().lock();

        try {
            if (!init)
                return;

            fileIO.close();

            fileIO = null;

            Files.delete(file.toPath());
        }
        finally {
            lock.writeLock().unlock();
        }
    }
}
