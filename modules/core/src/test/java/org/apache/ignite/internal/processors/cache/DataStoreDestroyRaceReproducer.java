package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class DataStoreDestroyRaceReproducer extends GridCommonAbstractTest {
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>("cache1")
            .setIndexedTypes(Integer.class, Integer.class);

        CacheConfiguration<?, ?> ccfg2 = new CacheConfiguration<>("cache2")
            .setIndexedTypes(Integer.class, String.class);

        cfg.setCacheConfiguration(ccfg, ccfg2);

        return cfg;
    }

    @Test
    public void test() throws Exception {
        startGrid(0);
        startGrid(1);

        awaitPartitionMapExchange();

        stopGrid(0);
        stopGrid(1);
    }
}
