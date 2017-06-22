package org.apache.ignite.internal.processors.cache.persistence.db.wal.reader;

import java.io.File;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
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
    }

    public WALIterator iterator(@NotNull final File walDirWithConsistentId) throws IgniteCheckedException {
        final GridKernalContext kernalCtx = new StandaloneGridKernalContext(log);
        final StandaloneIgniteCacheDatabaseSharedManager dbMgr = new StandaloneIgniteCacheDatabaseSharedManager();
        dbMgr.setPageSize(pageSize);
        final GridCacheSharedContext sharedCtx = new GridCacheSharedContext(
            kernalCtx, null, null, null,
            null, null, dbMgr, null,
            null, null, null, null,
            null, null, null);

        StandaloneWalRecordsIterator stIt = new StandaloneWalRecordsIterator(walDirWithConsistentId, log, sharedCtx);
        return stIt;
    }
}
