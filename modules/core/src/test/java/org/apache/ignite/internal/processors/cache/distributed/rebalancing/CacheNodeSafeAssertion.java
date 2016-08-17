package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import java.util.Collection;
import java.util.Iterator;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.testframework.assertions.Assertion;

/**
 * {@link Assertion} that checks that the primary and backup partitions are distributed such that we won't lose any data
 * if we lose a single node. This implies that the cache in question was configured with a backup count of at least one
 * and that all partitions are backed up to a different node from the primary.
 */
public class CacheNodeSafeAssertion implements Assertion {

    /** The {@link Ignite} instance. */
    private final Ignite ignite;

    /** The cache name. */
    private final String cacheName;

    /**
     * Construct a new {@link CacheNodeSafeAssertion} for the given {@code cacheName}.
     *
     * @param ignite The Ignite instance.
     * @param cacheName The cache name.
     */
    public CacheNodeSafeAssertion(Ignite ignite, String cacheName) {
        this.ignite = ignite;
        this.cacheName = cacheName;
    }

    protected Ignite ignite() {
        return this.ignite;
    }

    @Override public void test() throws AssertionError {
        Ignite ignite = this.ignite;
        Affinity<?> affinity = ignite.affinity(this.cacheName);
        int partCount = affinity.partitions();

        boolean hostSafe = true;
        boolean nodeSafe = true;
        for (int x = 0; x < partCount; ++x) {
            // Results are returned with the primary node first and backups after. We want to ensure that there is at
            // least one backup on a different host.
            Collection<ClusterNode> results = affinity.mapPartitionToPrimaryAndBackups(x);
            Iterator<ClusterNode> nodes = results.iterator();

            boolean newHostSafe = false;
            boolean newNodeSafe = false;
            if (nodes.hasNext()) {
                ClusterNode primary = nodes.next();

                // For host safety, get all nodes on the same host as the primary node and ensure at least one of the
                // backups is on a different host. For node safety, make sure at least of of the backups is not the
                // primary.
                Collection<ClusterNode> neighbors = hostSafe ? ignite.cluster().forHost(primary).nodes() : null;
                while (nodes.hasNext()) {
                    ClusterNode backup = nodes.next();
                    if (hostSafe) {
                        if (!neighbors.contains(backup)) {
                            newHostSafe = true;
                        }
                    }
                    if (nodeSafe) {
                        if (!backup.equals(primary)) {
                            newNodeSafe = true;
                        }
                    }
                }
            }

            hostSafe = newHostSafe;
            nodeSafe = newNodeSafe;
            if (!hostSafe && !nodeSafe) {
                break;
            }
        }

        if (hostSafe) {
            return;
        }
        if (nodeSafe) {
            return;
        }
        throw new AssertionError("Cache " + this.cacheName + " is endangered!");
    }
}
