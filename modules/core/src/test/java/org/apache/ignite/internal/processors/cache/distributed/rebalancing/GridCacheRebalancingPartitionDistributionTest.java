package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.assertions.Assertion;
import org.apache.ignite.testframework.junits.common.GridRollingRestartAbstractTest;


/**
 * Test the behavior of the partition rebalancing during a rolling restart.
 */
public class GridCacheRebalancingPartitionDistributionTest extends GridRollingRestartAbstractTest {

    /** The maximum allowable deviation from a perfect distribution. */
    private static final double MAX_DEVIATION = 0.20;

    /** Test cache name. */
    private static final String CACHE_NAME = "PARTITION_DISTRIBUTION_TEST";

    @Override protected CacheConfiguration<Integer, Integer> getCacheConfiguration() {
        return new CacheConfiguration<Integer, Integer>(CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setCacheMode(CacheMode.PARTITIONED)
                .setBackups(1)
                .setAffinity(new RendezvousAffinityFunction(true /* machine-safe */, 271))
                .setAtomicWriteOrderMode(CacheAtomicWriteOrderMode.CLOCK)
                .setRebalanceMode(CacheRebalanceMode.SYNC)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
    }

    /**
     * The test performs rolling restart and checks no server drops out and the partitions are balanced during
     * redistribution.
     */
    public void testRollingRestart() throws InterruptedException {
        awaitPartitionMapExchange();

        RollingRestartThread restartThread = super.rollingRestartThread;
        restartThread.join();
        assertEquals(getMaxRestarts(), restartThread.getRestartTotal());
    }

    @Override public int serverCount() {
        return 5;
    }

    @Override public int getMaxRestarts() {
        return 5;
    }

    @Override public IgnitePredicate<Ignite> getRestartCheck() {
        return new IgnitePredicate<Ignite>() {
            @Override
            public boolean apply(final Ignite ignite) {
                Collection<ClusterNode> servers = ignite.cluster().forServers().nodes();
                if (servers.size() < serverCount()) {
                    return false;
                }

                for (ClusterNode node : servers) {
                    int[] primaries = ignite.affinity(CACHE_NAME).primaryPartitions(node);
                    if (primaries == null || primaries.length == 0) {
                        return false;
                    }
                }
                return true;
            }
        };
    }

    @Override public Assertion getRestartAssertion() {
        return new FairDistributionAssertion();
    }

    /**
     * Assertion for {@link RollingRestartThread} to perform prior to each restart to test
     * the Partition Distribution.
     */
    private class FairDistributionAssertion extends CacheNodeSafeAssertion {

        /** Construct a new FairDistributionAssertion. */
        public FairDistributionAssertion() {
            super(grid(0), CACHE_NAME);
        }

        @Override public void test() throws AssertionError {
            super.test();

            Affinity<?> affinity = ignite().affinity(CACHE_NAME);
            int partCount = affinity.partitions();
            Map<ClusterNode, Integer> partMap = new HashMap<>(serverCount());

            for (int i = 0; i < partCount; i++) {
                ClusterNode node = affinity.mapPartitionToNode(i);
                int count = partMap.containsKey(node) ? partMap.get(node) : 0;
                partMap.put(node, count + 1);
            }

            int fairCount = partCount / serverCount();
            for (int count : partMap.values()) {
                double deviation = Math.abs(fairCount - count) / (double) fairCount;
                if (deviation > MAX_DEVIATION) {
                    throw new AssertionError("partition distribution deviation exceeded max: fair count=" + fairCount
                            + ", actual count=" + count + ", deviation=" + deviation);
                }
            }
        }
    }
}
