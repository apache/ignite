/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.typedef.*;

import java.util.*;
import java.util.concurrent.atomic.*;

/**
 */
public class GridCacheSizeFuture extends GridFutureAdapter<Integer> {
    /** */
    private GridCacheContext cctx;

    /** */
    private Set<UUID> nodeIds;

    /** */
    private AtomicInteger size = new AtomicInteger();

    /**
     * @param cctx Cache context.
     */
    public GridCacheSizeFuture(GridCacheContext cctx) {
        super(cctx.kernalContext());

        this.cctx = cctx;
    }

    /**
     * @param reqId Request ID.
     * @param set Set.
     * @throws GridException If failed.
     */
    @SuppressWarnings("unchecked")
    void init(final long reqId, final GridCacheSetImpl<?> set) throws GridException {
        final long topVer = cctx.discovery().topologyVersion();

        Collection<GridNode> nodes = set.dataNodes(topVer);

        nodeIds = new GridConcurrentHashSet<>(F.viewReadOnly(nodes, F.node2id()));

        for (GridNode node : nodes) {
            if (node.isLocal()) {
                cctx.closures().runLocalSafe(new Runnable() {
                    @Override public void run() {
                        GridCacheEnterpriseDataStructuresManager ds =
                            (GridCacheEnterpriseDataStructuresManager)cctx.dataStructures();

                        ds.processSetDataRequest(cctx.localNodeId(),
                            new GridCacheSetDataRequest<>(reqId, set.id(), topVer, 0, true));
                    }
                }, false);
            }
            else
                cctx.io().send(node, new GridCacheSetDataRequest<>(reqId, set.id(), topVer, 0, true));
        }
    }

    /**
     * @param nodeId Node ID.
     */
    void onNodeLeft(UUID nodeId) {
        if (nodeIds.remove(nodeId))
            onDone(new GridException("Failed to get set size, node left grid: " + nodeId));
    }

    /**
     * @param res Response.
     */
    void onResponse(GridCacheSetDataResponse res) {
        assert res.nodeId() != null;

        size.addAndGet(res.size());

        if (nodeIds.remove(res.nodeId()) && nodeIds.isEmpty())
            onDone(size.get());
    }
}
