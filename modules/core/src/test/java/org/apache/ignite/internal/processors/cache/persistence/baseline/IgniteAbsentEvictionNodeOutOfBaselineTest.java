/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache.persistence.baseline;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test absenting eviction for joined node if it is out of baseline.
 */
public class IgniteAbsentEvictionNodeOutOfBaselineTest extends GridCommonAbstractTest {
    /** */
    private static final String TEST_CACHE_NAME = "test";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setWalSegmentSize(512 * 1024)
            .setWalSegments(4)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(256 * 1024 * 1024)
                    .setPersistenceEnabled(true))
            .setWalMode(WALMode.LOG_ONLY));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Removed partitions if node is out of baseline.
     */
    public void testPartitionsRemovedIfJoiningNodeNotInBaseline() throws Exception {
        //given: start 3 nodes with data
        Ignite ignite0 = startGrids(3);

        ignite0.cluster().active(true);

        IgniteCache<Object, Object> cache = ignite0.getOrCreateCache(TEST_CACHE_NAME);

        for(int i = 0; i< 100; i++)
            cache.put(i, i);

        //when: stop one node and reset baseline topology
        stopGrid(2);

        resetBaselineTopology();

        awaitPartitionMapExchange();

        for(int i = 0; i< 200; i++)
            cache.put(i, i);

        //then: after returning stopped node to grid its partitions should be removed
        IgniteEx ignite2 = startGrid(2);

        assertTrue(ignite2.cachex(TEST_CACHE_NAME).context().topology().localPartitions().isEmpty());
    }
}
