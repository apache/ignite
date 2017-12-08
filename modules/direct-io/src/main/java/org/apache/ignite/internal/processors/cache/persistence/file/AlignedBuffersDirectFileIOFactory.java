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
import java.nio.file.OpenOption;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.jsr166.ConcurrentHashMap8;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Direct native IO factory for block IO operations on aligned memory structures.<br>
 * This limited functionality is used for page store operations.<br>
 * <b>Note: </b> This type of IO not applicable for WAL or other files.<br> <br>
 * This IO tries to minimize cache effects of the I/O (page caching by OS). <br> <br>
 * In general this will degrade performance, but it is useful in special
 * situations, such as when applications do their own caching.<br>
 */
public class AlignedBuffersDirectFileIOFactory implements FileIOFactory {
    /** Logger. */
    private final IgniteLogger log;

    /** Page size from durable memory. */
    private final int pageSize;

    /** Backup factory for files in case native is not available or not applicable. */
    private final FileIOFactory backupFactory;

    /** File system/os block size, negative value if library init was failed. */
    private final int fsBlockSize;

    /** Use backup factory, {@code true} if direct IO setup failed. */
    private boolean useBackupFactory;

    /** Thread local with buffers with capacity = one page {@code pageSize} and aligned using {@code fsBlockSize}. */
    private ThreadLocal<ByteBuffer> tlbOnePageAligned;

    /**
     * Managed aligned buffers. This collection is used to free buffers, an for checking if buffer is known to be
     * already aligned.
     */
    private final ConcurrentHashMap8<Long, Thread> managedAlignedBuffers = new ConcurrentHashMap8<>();

    /**
     * Creates direct native IO factory.
     *
     * @param log Logger.
     * @param storePath Storage path, used to check FS settings.
     * @param pageSize durable memory page size.
     * @param backupFactory fallback factory if init failed.
     */
    public AlignedBuffersDirectFileIOFactory(
        final IgniteLogger log,
        final File storePath,
        final int pageSize,
        final FileIOFactory backupFactory) {

        this.log = log;
        this.pageSize = pageSize;
        this.backupFactory = backupFactory;
        fsBlockSize = IgniteNativeIoLib.getFsBlockSize(storePath.getAbsolutePath(), log);

        useBackupFactory = true;
        if (fsBlockSize > 0) {
            int blkSize = fsBlockSize;

            if (pageSize % blkSize != 0) {
                U.warn(log,
                    String.format("Unable to setup Direct IO for Ignite page size [%d] bytes" +
                            " on file system block size [%d]." +
                            " For speeding up Ignite consider setting %s.setPageSize(%d)." +
                            " Direct IO is disabled",
                        pageSize, blkSize, DataStorageConfiguration.class.getSimpleName(), blkSize));
            }
            else {
                useBackupFactory = false;

                tlbOnePageAligned = new ThreadLocal<ByteBuffer>() {
                    /** {@inheritDoc} */
                    @Override protected ByteBuffer initialValue() {
                        return createManagedBuffer(pageSize);
                    }
                };

                if (log.isInfoEnabled())
                    log.info("Direct IO is enabled, " +
                        "using block size [" + blkSize + "] and durable memory page size [" + pageSize + "]");
            }
        }
        else {
            if (log.isInfoEnabled())
                log.info("Direct IO library is not available on current operating system " +
                    "[" + System.getProperty("os.version") + "]. Direct IO is not enabled.");
        }

    }

    /**
     * <b>Note: </b> Use only if {@link #isDirectIoAvailable()}.
     *
     * @param size buffer size to allocate.
     * @return new byte buffer.
     */
    @NotNull public ByteBuffer createManagedBuffer(int size) {
        assert !useBackupFactory : "Direct IO is disabled, aligned managed buffer creation is disabled now";

        final ByteBuffer allocate = AlignedBuffers.allocate(fsBlockSize, size).order(ByteOrder.nativeOrder());

        managedAlignedBuffers.put(GridUnsafe.bufferAddress(allocate), Thread.currentThread());

        return allocate;
    }

    /** {@inheritDoc} */
    @Override public FileIO create(File file) throws IOException {
        return create(file, CREATE, READ, WRITE);
    }

    /** {@inheritDoc} */
    @Override public FileIO create(File file, OpenOption... modes) throws IOException {
        if (useBackupFactory)
            return backupFactory.create(file, modes);

        return new AlignedBuffersDirectFileIO(fsBlockSize, pageSize, file, modes, tlbOnePageAligned, managedAlignedBuffers, log);

    }

    /**
     * @return {@code true} if Direct IO can be used on current OS and file system settings
     */
    boolean isDirectIoAvailable() {
        return !useBackupFactory;
    }

    /**
     * Managed aligned buffers and its associated threads. This collection is used to free buffers, an for checking if
     * buffer is known to be already aligned.
     *
     * @return map address->thread.
     */
    ConcurrentHashMap8<Long, Thread> managedAlignedBuffers() {
        return managedAlignedBuffers;
    }
}
