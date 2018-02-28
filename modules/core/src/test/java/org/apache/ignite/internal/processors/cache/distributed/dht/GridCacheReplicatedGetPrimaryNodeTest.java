package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;

/**
 *
 */
public class GridCacheReplicatedGetPrimaryNodeTest extends GridCacheAbstractReplicatedAffinityNodeTest {
    @Override
    protected boolean readFromBackup() {
        return false;
    }

    public void testGetPrimaryNode() {
        final Ignite ignite = getClientNode();
        final Affinity<Object> affinity = ignite.affinity(DEFAULT_CACHE_NAME);
        final IgniteCache<Object, Object> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache.put(getKey(), getKey());
        final ClusterNode primaryNode = affinity.mapKeyToNode(getKey());
        communication(ignite).setExpectedNode(primaryNode);
        cache.get(getKey());
    }
}
