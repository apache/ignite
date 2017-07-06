/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.wal.reader;

import java.io.File;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.jetbrains.annotations.NotNull;

/**
 * Factory for creating iterator over WAL files
 */
public class IgniteWalIteratorFactory {
    /** Logger. */
    private final IgniteLogger log;
    /** Page size, in standalone iterator mode this value can't be taken from memory configuration */
    private final int pageSize;

    /**
     * Creates WAL files iterator factory
     * @param log Logger.
     * @param pageSize Page size, size is validated
     */
    public IgniteWalIteratorFactory(@NotNull final IgniteLogger log, final int pageSize) {
        this.log = log;
        this.pageSize = pageSize;
        new MemoryConfiguration().setPageSize(pageSize); // just for validate
    }

    /**
     * Creates iterator for (archive) directory scan mode.
     * Note in this mode total scanned files at end of iteration may be wider that initial files in directory.
     * This mode does not support work directory scan because work directory contains unpredictable number in file name.
     * Such file may broke iteration.
     *
     * @param walDirWithConsistentId directory with WAL files. Should already contain node consistent ID as subfolder
     * @return closable WAL records iterator, should be closed when non needed
     * @throws IgniteCheckedException if failed to read folder
     */
    public WALIterator iteratorArchiveDirectory(@NotNull final File walDirWithConsistentId) throws IgniteCheckedException {
        return new StandaloneWalRecordsIterator(walDirWithConsistentId, log, prepareSharedCtx());
    }

    /**
     * Creates iterator for file by file scan mode.
     * This method may be used only for archive folder (not for work).
     * In this mode only provided WAL segments will be scanned. New WAL files created during iteration will be ignored
     * @param files files to scan. Order it not important, but is significant to provide all segments without omissions
     * @return closable WAL records iterator, should be closed when non needed
     * @throws IgniteCheckedException if failed to read files
     */
    public WALIterator iteratorArchiveFiles(@NotNull final File ...files) throws IgniteCheckedException {
        return new StandaloneWalRecordsIterator(log, prepareSharedCtx(), false, files);
    }

    /**
     * Creates iterator for file by file scan mode.
     * This method may be used for work folder, file indexes are scanned from the file context.
     * In this mode only provided WAL segments will be scanned. New WAL files created during iteration will be ignored.
     * @param files files to scan. Order it not important, but is significant to provide all segments without omissions
     * @return closable WAL records iterator, should be closed when non needed
     * @throws IgniteCheckedException if failed to read files
     */
    public WALIterator iteratorWorkFiles(@NotNull final File ...files) throws IgniteCheckedException {
        return new StandaloneWalRecordsIterator(log, prepareSharedCtx(), true, files);
    }

    /**
     * @return fake shared context required for create minimal services for record reading
     */
    @NotNull private GridCacheSharedContext prepareSharedCtx() {
        final GridKernalContext kernalCtx = new StandaloneGridKernalContext(log);

        final StandaloneIgniteCacheDatabaseSharedManager dbMgr = new StandaloneIgniteCacheDatabaseSharedManager();

        dbMgr.setPageSize(pageSize);
        return new GridCacheSharedContext<>(
            kernalCtx, null, null, null,
            null, null, dbMgr, null,
            null, null, null, null,
            null, null, null);
    }
}
