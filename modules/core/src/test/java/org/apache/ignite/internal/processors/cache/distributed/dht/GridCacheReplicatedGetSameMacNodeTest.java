package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class GridCacheReplicatedGetSameMacNodeTest extends GridCacheAbstractReplicatedAffinityNodeTest {
    private static final String CLIENT_MAC = "CLIENT_MAC";

    @Override
    protected boolean readFromBackup() {
        return true;
    }

    public void testGetSameMacNode() {
        final Map<String, Object> userAttributes = new HashMap<>();
        userAttributes.put(IgniteNodeAttributes.ATTR_MACS, CLIENT_MAC);

        final Ignite ignite = getClientNode();
        final ClusterNode clientNode = ignite.cluster().forClients().node();
        ((TcpDiscoveryNode)clientNode).setAttributes(userAttributes);
        ClusterNode randomServerNode = ignite.cluster().forServers().forRandom().node();
        ((TcpDiscoveryNode)randomServerNode).setAttributes(userAttributes);

        final IgniteCache<Object, Object> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache.put(getKey(), getKey());
        communication(ignite).setExpectedNode(randomServerNode);
        cache.get(getKey());
    }
}
