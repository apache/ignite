package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;

/** */
public abstract class AbstractSnapshotCacheAffectingFuture<T> extends AbstractSnapshotFuture<T> {
    /** */
    protected final GridCacheSharedContext<?, ?> cctx;

    /**
     * Ctor.
     *
     * @param log Logger.
     * @param sharedCacheCtx Shared cache context.
     * @param srcNodeId Snapshot operation originator node id.
     * @param reqId Snapshot operation request id.
     * @param snpName Snapshot name.
     */
    protected AbstractSnapshotCacheAffectingFuture(
        GridCacheSharedContext<?, ?> sharedCacheCtx,
        IgniteLogger log,
        UUID srcNodeId,
        UUID reqId,
        String snpName
    ) {
        super(log, srcNodeId, reqId, snpName);

        this.cctx = sharedCacheCtx;
    }

    /**
     * @return Set of cache groups included into snapshot operation.
     */
    public abstract Set<Integer> affectedCacheGroups();
}
