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

public class AlignedBuffersDirectFileIOFactory implements FileIOFactory {

    /** Logger. */
    private final IgniteLogger log;

    /** Page size from durable memory. */
    private final int pageSize;

    /** Backup factory for files in case native is not available or not applicable. */
    private final FileIOFactory backupFactory;

    /** File system/os block size, negative value if library init was failed. */
    private final int fsBlockSize;

    private boolean useBackupFactory;

    private ThreadLocal<ByteBuffer> tblOnePageAligned;

    /** Managed aligned buffers. */
    private final ConcurrentHashMap8<Long, Thread> managedAlignedBuffers = new ConcurrentHashMap8<>();

    public AlignedBuffersDirectFileIOFactory(
        final IgniteLogger log,
        final File storePath,
        final int pageSize,
        final FileIOFactory backupFactory) {

        this.log = log;
        this.pageSize = pageSize;
        this.backupFactory = backupFactory;
        fsBlockSize = IgniteNativeIoLib.getFsBlockSize(storePath.getAbsolutePath(), log);

        this.useBackupFactory = true;
        if (fsBlockSize > 0) {
            int blkSize = fsBlockSize;

            if (pageSize % blkSize != 0) {
                U.warn(log, "Unable to apply DirectIO for page size [" + pageSize + "] bytes" +
                    " on file system block size [" + blkSize + "]." +
                    " For speeding up Ignite consider setting " + DataStorageConfiguration.class.getSimpleName()
                    + ".setPageSize(" + blkSize + "). Direct IO is disabled");
            }
            else {
                useBackupFactory = false;

                tblOnePageAligned = new ThreadLocal<ByteBuffer>() {
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
                log.info("Direct IO library is not available on current system " +
                    "[" + System.getProperty("os.version") + "]. Direct IO is disabled");
        }

    }

    /**
     * <b>Note: </b> Use only if {@link #isDirectIoAvailable()}.
     *
     * @param capacity buffer size to allocate.
     * @return new byte buffer.
     */
    @NotNull public ByteBuffer createManagedBuffer(int capacity) {
        assert !useBackupFactory : "Direct IO is disabled, aligned managed buffer creation is disabled now";

        final ByteBuffer allocate = AlignedBuffers.allocate(fsBlockSize, capacity).order(ByteOrder.nativeOrder());

        managedAlignedBuffers.put(GridUnsafe.bufferAddress(allocate), Thread.currentThread());

        return allocate;
    }

    /** {@inheritDoc} */
    @Override public FileIO create(File file) throws IOException {
        return create(file, CREATE, READ, WRITE);
    }

    /** {@inheritDoc} */
    @Override public FileIO create(File file, OpenOption... modes) throws IOException {
        if (useBackupFactory) {
            return backupFactory.create(file, modes);
        }

        return new AlignedBuffersDirectFileIO(fsBlockSize, pageSize, file, modes, tblOnePageAligned, managedAlignedBuffers);

    }

    /**
     * @return {@code true} if Direct IO can be used on current OS and file system settings
     */
    public boolean isDirectIoAvailable() {
        return !useBackupFactory;
    }

    public ConcurrentHashMap8<Long, Thread> managedAlignedBuffers() {
        return managedAlignedBuffers;
    }
}
