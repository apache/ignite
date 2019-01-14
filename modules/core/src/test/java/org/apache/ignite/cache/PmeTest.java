package org.apache.ignite.cache;

import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.List;

@RunWith(JUnit4.class)
public class PmeTest extends GridCommonAbstractTest {

    @Override
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        List<CacheConfiguration> list = new ArrayList<>();

        for(int i = 0; i < 2000; i++) {
            CacheConfiguration<String, String> cacheConfiguration = new CacheConfiguration<>();

            cacheConfiguration.setName("cache" + i);
            cacheConfiguration.setGroupName("grp" + (i % 10));
            cacheConfiguration.setAffinity(new RendezvousAffinityFunction(true, 8192));
            cacheConfiguration.setBackups(3);
            cacheConfiguration.setCacheMode(CacheMode.PARTITIONED);
            cacheConfiguration.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

            list.add(cacheConfiguration);
        }

        cfg.setCacheConfiguration(list.toArray(new CacheConfiguration[]{}));

        return cfg;
    }


    @Test
    public void testNodeLeave() throws Exception {
        IgniteEx igniteEx = startGrid(4);

        igniteEx.cluster().active(true);

        awaitPartitionMapExchange();

        stopGrid(2);

        awaitPartitionMapExchange();
    }
}
