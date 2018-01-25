package org.apache.ignite.ml;

import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.dlc.dataset.DLCDataset;
import org.apache.ignite.ml.dlc.dataset.transformer.DLCTransformers;
import org.apache.ignite.ml.dlc.impl.cache.CacheBasedDLCFactory;
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

        DLCDataset<Integer, Integer, ?> dataset = new CacheBasedDLCFactory<>(ignite, cache)
            .createDLC(
                (data, size) -> null,
                DLCTransformers.upstreamToDataset((k, v) -> new double[]{1, 2, 3}, 3),
                DLCDataset::new
            );

        // Calculation of the mean value. This calculation will be performed in map-reduce manner.
        double[] mean = dataset.mean();
        System.out.println("Mean \n\t" + Arrays.toString(mean));

        // Calculation of the standard deviation. This calculation will be performed in map-reduce manner.
        double[] std = dataset.std();
        System.out.println("Standard deviation \n\t" + Arrays.toString(std));

        // Calculation of the covariance matrix.  This calculation will be performed in map-reduce manner.
        double[][] cov = dataset.cov();
        System.out.println("Covariance matrix ");
        for (double[] row : cov)
            System.out.println("\t" + Arrays.toString(row));

        // Calculation of the correlation matrix.  This calculation will be performed in map-reduce manner.
        double[][] corr = dataset.corr();
        System.out.println("Correlation matrix ");
        for (double[] row : corr)
            System.out.println("\t" + Arrays.toString(row));
    }
}
