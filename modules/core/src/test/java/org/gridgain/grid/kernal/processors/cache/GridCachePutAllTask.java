package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Puts all the passed data into partitioned cache in small chunks.
 */
class GridCachePutAllTask extends GridComputeTaskAdapter<Collection<Integer>, Void> {
    /** Number of entries per put. */
    private static final int TX_BOUND = 30;

    /** Preferred node. */
    private final UUID preferredNode;

    /** Cache name. */
    private final String cacheName;

    /**
     *
     * @param preferredNode A node that we'd prefer to take from grid.
     * @param cacheName A name of the cache to work with.
     */
    GridCachePutAllTask(UUID preferredNode, String cacheName) {
        this.preferredNode = preferredNode;
        this.cacheName = cacheName;
    }

    /** {@inheritDoc} */
    @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid,
        @Nullable final Collection<Integer> data) throws GridException {
        assert !subgrid.isEmpty();

        // Give preference to wanted node. Otherwise, take the first one.
        GridNode targetNode = F.find(subgrid, subgrid.get(0), new GridPredicate<GridNode>() {
            /** {@inheritDoc} */
            @Override public boolean apply(GridNode e) {
                return preferredNode.equals(e.id());
            }
        });

        return Collections.singletonMap(
            new GridComputeJobAdapter() {
                @GridLoggerResource
                private GridLogger log;

                @GridInstanceResource
                private Ignite ignite;

                @Override public Object execute() throws GridException {
                    log.info("Going to put data: " + data);

                    GridCacheProjection<Object, Object> cache = ignite.cache(cacheName);

                    assert cache != null;

                    HashMap<Integer, Integer> putMap = U.newLinkedHashMap(TX_BOUND);

                    Iterator<Integer> it = data.iterator();

                    int cnt = 0;

                    while (it.hasNext()) {
                        Integer val = it.next();

                        putMap.put(val, val);

                        if (++cnt == TX_BOUND) {
                            log.info("Putting keys to cache: " + putMap.keySet());

                            cache.putAll(putMap);

                            cnt = 0;

                            putMap = U.newLinkedHashMap(TX_BOUND);
                        }
                    }

                    assert cnt < TX_BOUND;
                    assert putMap.size() == (data.size() % TX_BOUND) : "putMap.size() = " + putMap.size();

                    log.info("Putting keys to cache: " + putMap.keySet());

                    cache.putAll(putMap);

                    log.info("Finished putting data: " + data);

                    return data;
                }
            },
            targetNode);
    }

    /** {@inheritDoc} */
    @Override public GridComputeJobResultPolicy result(GridComputeJobResult res, List<GridComputeJobResult> rcvd) throws GridException {
        if (res.getException() != null)
            return GridComputeJobResultPolicy.FAILOVER;

        return GridComputeJobResultPolicy.WAIT;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Void reduce(List<GridComputeJobResult> results) throws GridException {
        return null;
    }
}
