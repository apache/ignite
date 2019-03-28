package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CacheDiscoveryExchangeTest extends GridCommonAbstractTest {

    @Override
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration("cache-" + igniteInstanceName)
            .setAffinity(new RendezvousAffinityFunction(false, 256));

        ccfg.setNodeFilter(new OnlyOneNodeFilter(igniteInstanceName));

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    @Before
    public void before() {
        stopAllGrids();
    }

    @After
    public void after() {
        stopAllGrids();
    }

    @Test(timeout = 60 * 1000 * 30)
    public void test() throws Exception {
        IgniteEx crd = startGrid(0);
        IgniteEx second = startGrid(1);

        try {
            crd.cache("cache-" + second.name()).put(1, 1);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        crd.getOrCreateCache(new CacheConfiguration<>("temp").setNodeFilter(new OnlyOneNodeFilter(second.name())));

        int k = 2;
    }

    private static class OnlyOneNodeFilter implements IgnitePredicate<ClusterNode> {

        private final String consistentId;

        private OnlyOneNodeFilter(String consistentId) {
            this.consistentId = consistentId;
        }

        @Override public boolean apply(ClusterNode node) {
            return node.consistentId().equals(consistentId);
        }
    }
}
