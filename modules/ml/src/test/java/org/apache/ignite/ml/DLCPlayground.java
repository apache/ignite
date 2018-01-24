package org.apache.ignite.ml;

import java.io.Serializable;
import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.dlc.DLC;
import org.apache.ignite.ml.dlc.DLCFactory;
import org.apache.ignite.ml.dlc.dataset.part.recoverable.DLCDatasetPartitionRecoverable;
import org.apache.ignite.ml.dlc.dataset.transformer.DLCTransformers;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/** */
public class DLCPlayground extends GridCommonAbstractTest {
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

    /** */
    public void testTest() {
        CacheConfiguration<Integer, Integer> cc = new CacheConfiguration<>();
        cc.setAffinity(new RendezvousAffinityFunction(false, 2));
        cc.setName("TEST");
        IgniteCache<Integer, Integer> cache = ignite.createCache(cc);
        for (int i = 0; i < 40; i++)
            cache.put(i, i);

        DLC<Integer, Integer, Serializable, DLCDatasetPartitionRecoverable> idd = DLCFactory.createIDD(
            ignite,
            cache,
            (iter, cnt) -> null,
            DLCTransformers.upstreamToDataset(e -> new double[]{1, 2, 3}, 3)
        );

        for (int i = 0; i < 3; i++) {
            idd.compute(part -> {
                StringBuilder builder = new StringBuilder();
                builder.append("PART \n");
                for (double[] row : part.getRecoverableData().getData())
                    builder.append(Arrays.toString(row)).append("\n");
                System.err.println(builder.toString());
            });
        }
    }
}
