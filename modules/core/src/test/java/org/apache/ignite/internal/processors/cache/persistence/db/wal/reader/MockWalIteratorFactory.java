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
import java.io.Serializable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderSettings;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFoldersResolver;
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
    private final Serializable consistentId;

    /** DB storage subfolder based node index and consistent node ID. */
    private String subfolderName;

    /** Segments count in work dir. */
    private int segments;

    /**
     * Creates factory
     * @param log Logger.
     * @param pageSize Page size.
     * @param consistentId Consistent id.
     * @param subfolderName Subfolder name.
     * @param segments Segments.
     */
    public MockWalIteratorFactory(@Nullable IgniteLogger log,
        int pageSize,
        Serializable consistentId,
        String subfolderName,
        int segments) {
        this.log = log == null ? Mockito.mock(IgniteLogger.class) : log;
        this.pageSize = pageSize;
        this.consistentId = consistentId;
        this.subfolderName = subfolderName;
        this.segments = segments;
    }

    /**
     * Creates iterator
     * @param wal WAL directory without node consistent id
     * @param walArchive WAL archive without node consistent id
     * @return iterator
     * @throws IgniteCheckedException if IO failed
     */
    @SuppressWarnings("unchecked")
    public WALIterator iterator(File wal, File walArchive) throws IgniteCheckedException {
        final DataStorageConfiguration persistentCfg1 = Mockito.mock(DataStorageConfiguration.class);

        when(persistentCfg1.getWalPath()).thenReturn(wal.getAbsolutePath());
        when(persistentCfg1.getWalArchivePath()).thenReturn(walArchive.getAbsolutePath());
        when(persistentCfg1.getWalSegments()).thenReturn(segments);
        when(persistentCfg1.getWalBufferSize()).thenReturn(DataStorageConfiguration.DFLT_WAL_BUFF_SIZE);
        when(persistentCfg1.getWalRecordIteratorBufferSize()).thenReturn(DataStorageConfiguration.DFLT_WAL_RECORD_ITERATOR_BUFFER_SIZE);
        when(persistentCfg1.getWalSegmentSize()).thenReturn(DataStorageConfiguration.DFLT_WAL_SEGMENT_SIZE);

        final FileIOFactory fileIOFactory = new DataStorageConfiguration().getFileIOFactory();
        when(persistentCfg1.getFileIOFactory()).thenReturn(fileIOFactory);

        final IgniteConfiguration cfg = Mockito.mock(IgniteConfiguration.class);

        when(cfg.getDataStorageConfiguration()).thenReturn(persistentCfg1);

        final GridKernalContext ctx = Mockito.mock(GridKernalContext.class);

        when(ctx.config()).thenReturn(cfg);
        when(ctx.clientNode()).thenReturn(false);
        when(ctx.pdsFolderResolver()).thenReturn(new PdsFoldersResolver() {
            @Override public PdsFolderSettings resolveFolders() {
                return new PdsFolderSettings(new File("."), subfolderName, consistentId, null, false);
            }
        });

        final GridDiscoveryManager disco = Mockito.mock(GridDiscoveryManager.class);
        when(ctx.discovery()).thenReturn(disco);

        final IgniteWriteAheadLogManager mgr = new FileWriteAheadLogManager(ctx);
        final GridCacheSharedContext sctx = Mockito.mock(GridCacheSharedContext.class);

        when(sctx.kernalContext()).thenReturn(ctx);
        when(sctx.discovery()).thenReturn(disco);
        when(sctx.gridConfig()).thenReturn(cfg);

        final GridCacheDatabaseSharedManager db = Mockito.mock(GridCacheDatabaseSharedManager.class);

        when(db.pageSize()).thenReturn(pageSize);
        when(sctx.database()).thenReturn(db);
        when(sctx.logger(any(Class.class))).thenReturn(log);

        mgr.start(sctx);

        return mgr.replay(null);
    }
}
