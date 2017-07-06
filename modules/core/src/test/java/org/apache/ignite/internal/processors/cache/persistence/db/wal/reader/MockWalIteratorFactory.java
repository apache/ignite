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

package org.apache.ignite.internal.processors.cache.persistence.db.wal.reader;

import java.io.File;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.jetbrains.annotations.Nullable;
import org.mockito.Mockito;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

/**
 * Mockito based WAL iterator provider
 */
public class MockWalIteratorFactory {
    /** Logger. */
    private final IgniteLogger log;

    /** Page size. */
    private final int pageSize;

    /** Consistent node id. */
    private final String consistentId;

    /** Segments count in work dir. */
    private int segments;

    /**
     * Creates factory
     * @param log Logger.
     * @param pageSize Page size.
     * @param consistentId Consistent id.
     * @param segments Segments.
     */
    public MockWalIteratorFactory(@Nullable IgniteLogger log, int pageSize, String consistentId, int segments) {
        this.log = log == null ? Mockito.mock(IgniteLogger.class) : log;
        this.pageSize = pageSize;
        this.consistentId = consistentId;
        this.segments = segments;
    }

    /**
     * Creates iterator
     * @param wal WAL directory without node id
     * @param walArchive WAL archive without node id
     * @return iterator
     * @throws IgniteCheckedException if IO failed
     */
    public WALIterator iterator(File wal, File walArchive) throws IgniteCheckedException {
        final PersistentStoreConfiguration persistentCfg1 = Mockito.mock(PersistentStoreConfiguration.class);

        when(persistentCfg1.getWalStorePath()).thenReturn(wal.getAbsolutePath());
        when(persistentCfg1.getWalArchivePath()).thenReturn(walArchive.getAbsolutePath());
        when(persistentCfg1.getWalSegments()).thenReturn(segments);
        when(persistentCfg1.getTlbSize()).thenReturn(PersistentStoreConfiguration.DFLT_TLB_SIZE);
        when(persistentCfg1.getWalRecordIteratorBufferSize()).thenReturn(PersistentStoreConfiguration.DFLT_WAL_RECORD_ITERATOR_BUFFER_SIZE);

        final IgniteConfiguration cfg = Mockito.mock(IgniteConfiguration.class);

        when(cfg.getPersistentStoreConfiguration()).thenReturn(persistentCfg1);

        final GridKernalContext ctx = Mockito.mock(GridKernalContext.class);

        when(ctx.config()).thenReturn(cfg);
        when(ctx.clientNode()).thenReturn(false);

        final GridDiscoveryManager disco = Mockito.mock(GridDiscoveryManager.class);

        when(disco.consistentId()).thenReturn(consistentId);
        when(ctx.discovery()).thenReturn(disco);

        final IgniteWriteAheadLogManager mgr = new FileWriteAheadLogManager(ctx);
        final GridCacheSharedContext sctx = Mockito.mock(GridCacheSharedContext.class);

        when(sctx.kernalContext()).thenReturn(ctx);
        when(sctx.discovery()).thenReturn(disco);

        final GridCacheDatabaseSharedManager database = Mockito.mock(GridCacheDatabaseSharedManager.class);

        when(database.pageSize()).thenReturn(pageSize);
        when(sctx.database()).thenReturn(database);
        when(sctx.logger(any(Class.class))).thenReturn(log);

        mgr.start(sctx);

        return mgr.replay(null);
    }
}
