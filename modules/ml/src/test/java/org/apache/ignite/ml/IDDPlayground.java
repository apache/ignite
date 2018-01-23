package org.apache.ignite.ml;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.idd.IDD;
import org.apache.ignite.ml.idd.IDDFactory;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public class IDDPlayground extends GridCommonAbstractTest {
    /** Number of nodes in grid */
    private static final int NODE_COUNT = 4;

    /** */
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 1; i <= NODE_COUNT; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() {
        stopAllGrids();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        /* Grid instance. */
        ignite = grid(NODE_COUNT);
        ignite.configuration().setPeerClassLoadingEnabled(true);
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
    }

    public void testTest() {
        CacheConfiguration<Integer, Integer> cc = new CacheConfiguration<>();
        cc.setAffinity(new RendezvousAffinityFunction(false, 10));
        cc.setName("TEST");
        IgniteCache<Integer, Integer> cache = ignite.createCache(cc);
        for (int i = 0; i < 100; i++) {
            cache.put(i, i);
        }

        IDD<Integer, Integer, Void, int[]> idd = IDDFactory.createIDD(ignite, cache, (c, p) -> {
            System.err.println("LOAD " + p);
            ScanQuery<Integer, Integer> q = new ScanQuery<>();
            q.setLocal(true);
            q.setPartition(p);
            List<Integer> values = new ArrayList<>();
            c.query(q).forEach(e -> values.add(e.getValue()));
            int[] res = new int[values.size()];
            for (int i = 0; i < res.length; i++) {
                res[i] = values.get(i);
            }
            return res;
        });

        for (int i = 0; i < 3; i++) {
            idd.compute(part -> {
                System.err.println("PART " + part.getDistributedSegment().getPartId() + " : "
                    + Arrays.toString(part.getLocalSegment()));
            });
        }
    }
}
