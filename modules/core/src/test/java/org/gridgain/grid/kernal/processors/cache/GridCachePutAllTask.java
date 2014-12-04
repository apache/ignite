package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Puts all the passed data into partitioned cache in small chunks.
 */
class GridCachePutAllTask extends ComputeTaskAdapter<Collection<Integer>, Void> {
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
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable final Collection<Integer> data) throws GridException {
        assert !subgrid.isEmpty();

        // Give preference to wanted node. Otherwise, take the first one.
        ClusterNode targetNode = F.find(subgrid, subgrid.get(0), new IgnitePredicate<ClusterNode>() {
            /** {@inheritDoc} */
            @Override public boolean apply(ClusterNode e) {
                return preferredNode.equals(e.id());
            }
        });

        return Collections.singletonMap(
            new ComputeJobAdapter() {
                @IgniteLoggerResource
                private IgniteLogger log;

                @IgniteInstanceResource
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
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws GridException {
        if (res.getException() != null)
            return ComputeJobResultPolicy.FAILOVER;

        return ComputeJobResultPolicy.WAIT;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Void reduce(List<ComputeJobResult> results) throws GridException {
        return null;
    }
}
