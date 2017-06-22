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
 * Created by dpavlov on 22.06.2017
 */
public class IgniteWalIteratorFactory {
    private final IgniteLogger log;
    private final int pageSize;

    public IgniteWalIteratorFactory(IgniteLogger log, int pageSize) {
        this.log = log;
        this.pageSize = pageSize;
        new MemoryConfiguration().setPageSize(pageSize); // just for validate
    }

    public WALIterator iteratorDirectory(@NotNull final File walDirWithConsistentId) throws IgniteCheckedException {
        return new StandaloneWalRecordsIterator(walDirWithConsistentId, log, prepareSharedCtx());
    }

    public WALIterator iteratorFiles(@NotNull final File ...files) throws IgniteCheckedException {
        return new StandaloneWalRecordsIterator(log, prepareSharedCtx(), files);
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
