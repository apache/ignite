package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheMvccEntryInfo;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorClosure;
import org.apache.ignite.internal.util.typedef.F;

/** */
class RebuildIndexFromHashClosure implements SchemaIndexCacheVisitorClosure {
    /** */
    private final GridCacheQueryManager qryMgr;

    /** MVCC status flag. */
    private final boolean mvccEnabled;

    /** Last encountered key. */
    private KeyCacheObject prevKey = null;

    /** MVCC version of previously encountered row with the same key. */
    private GridCacheMvccEntryInfo mvccVer = null;

    /**
     * @param qryMgr Query manager.
     * @param mvccEnabled MVCC status flag.
     */
    RebuildIndexFromHashClosure(GridCacheQueryManager qryMgr, boolean mvccEnabled) {
        this.qryMgr = qryMgr;
        this.mvccEnabled = mvccEnabled;
    }

    /** {@inheritDoc} */
    @Override public void apply(CacheDataRow row) throws IgniteCheckedException {
        if (mvccEnabled && !F.eq(prevKey, row.key())) {
            prevKey = row.key();

            mvccVer = null;
        }

        // prevRowAvailable is always true with MVCC on, and always false *on index rebuild* with MVCC off.
        qryMgr.store(row, mvccVer, null, mvccEnabled, true);

        if (mvccEnabled) {
            mvccVer = new GridCacheMvccEntryInfo();

            mvccVer.mvccCoordinatorVersion(row.mvccCoordinatorVersion());

            mvccVer.mvccCounter(row.mvccCounter());
        }
    }
}
