package org.apache.ignite.internal.processors.cache.preload;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;

public class PartitionSwitchModeManager implements DbCheckpointListener {
    /** */
    private final IgniteLogger log;

    /** */
    private final GridCacheSharedContext<?, ?> cctx;

    /** */
    private final ConcurrentLinkedQueue<SwitchModeRequest> switchReqs = new ConcurrentLinkedQueue<>();

    /**
     * @param cctx Shared context.
     */
    public PartitionSwitchModeManager(GridCacheSharedContext<?, ?> cctx) {
        this.cctx = cctx;
        this.log = cctx.logger(PartitionSwitchModeManager.class);
    }

//    /**
//     * @param p The condition to check.
//     * @return The number of pending switch request satisfyed by given condition.
//     */
//    public int pendingRequests(Predicate<CacheDataStoreEx.StorageMode> p) {
//        int cnt = 0;
//
//        for (SwitchModeRequest rq : switchReqs) {
//            if (p.test(rq.nextMode))
//                cnt++;
//        }
//
//        return cnt;
//    }

    /** {@inheritDoc} */
    @Override public void onMarkCheckpointBegin(Context ctx) {
        SwitchModeRequest rq;

        while ((rq = switchReqs.poll()) != null) {
            for (Map.Entry<Integer, Set<Integer>> e : rq.parts.entrySet()) {
                CacheGroupContext grp = cctx.cache().cacheGroup(e.getKey());

                for (Integer partId : e.getValue()) {
                    GridDhtLocalPartition locPart = grp.topology().localPartition(partId);

                    if (locPart.readOnly() == rq.nextReadOnly)
                        continue;

                    //TODO invalidate partition

//                    IgniteCacheOffheapManager.CacheDataStore currStore = locPart.dataStore(locPart.readOnlyMode());

                    // Pre-init the new storage.
//                    locPart.dataStore(rq.nextReadOnly).init(currStore.updateCounter());

                    // Switching mode under the write lock.
                    locPart.readOnly(rq.nextReadOnly);
                }
            }

            rq.rqFut.onDone();
        }
    }

    /** {@inheritDoc} */
    @Override public void onCheckpointBegin(Context ctx) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void beforeCheckpointBegin(Context ctx) {
        // No-op.
    }

    /**
     * @param readOnly The storage mode to switch to.
     * @param parts The set of partitions to change storage mode.
     * @return The future which will be completed when request is done.
     */
    public GridFutureAdapter<Void> offerSwitchRequest(
        boolean readOnly,
        Map<Integer, Set<Integer>> parts
    ) {
        SwitchModeRequest req = new SwitchModeRequest(readOnly, parts);

        boolean offered = switchReqs.offer(req);

        assert offered;

        U.log(log, "Change partition mode request registered [mode=" + readOnly + ", parts=" + parts + ']');

        return req.rqFut;
    }

    /**
     *
     */
    private static class SwitchModeRequest {
        /** The storage mode to switch to. */
        private final boolean nextReadOnly;

        /** The map of cache groups and corresponding partition to switch mode to. */
        private final Map<Integer, Set<Integer>> parts;

        /** The future will be completed when the request has been processed. */
        private final GridFutureAdapter<Void> rqFut = new GridFutureAdapter<>();

        /**
         * @param nextReadOnly The mode to set to.
         * @param parts The partitions to switch mode to.
         */
        public SwitchModeRequest(
            boolean nextReadOnly,
            Map<Integer, Set<Integer>> parts
        ) {
            this.nextReadOnly = nextReadOnly;
            this.parts = parts;
        }
    }
}
