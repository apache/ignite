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
import org.mockito.Mockito;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

/**
 * Mockito based WAL iterator provider
 */
public class MockWalIteratorFactory {
    private final IgniteLogger log;
    private final int pageSize;
    private final String consistentId;
    private int segments;

    public MockWalIteratorFactory(IgniteLogger log, int pageSize, String consistentId, int segments) {
        this.log = log;
        this.pageSize = pageSize;
        this.consistentId = consistentId;
        this.segments = segments;
    }

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
        when(sctx.logger(any(Class.class))).thenReturn(Mockito.mock(IgniteLogger.class));

        mgr.start(sctx);

        return mgr.replay(null);
    }
}
