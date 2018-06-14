package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import java.util.ArrayList;
import java.util.Collections;

/**
 */
public class RemoveWalSegmentAndRestartTest extends GridCommonAbstractTest {
    /** Client index. */
    private static final int CLIENT_IDX = 33;

    /** Count. */
    public static final int COUNT = 8;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setRebalanceThreadPoolSize(33)
            .setConnectorConfiguration(new ConnectorConfiguration())
            .setClientMode(igniteInstanceName.equals(getTestIgniteInstanceName(CLIENT_IDX)))
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setCheckpointFrequency(2_000)
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)))
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setMaxSize(200L * 1024 * 1024)
                            .setPersistenceEnabled(true)));
    }

    public void test() throws Exception {
        IgniteEx ignite0 = (IgniteEx)startGrids(COUNT);
        IgniteEx client = startGrid(CLIENT_IDX);

        client.cluster().active(true);

        startCaches(ignite0);

        IgniteCache cache1 = client.cache(DEFAULT_CACHE_NAME);

        GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                int iter = 0;

                while (true) {
                    for (int i = 0; i < 1_000; i++)
                        cache1.put(i, new CacheValueObj("Val " + i + " iter " + iter));

                    iter++;
                }
            }
        }, COUNT, "sql-exec");

        stopAllGrids();
    }

    private void startCaches(IgniteEx ignite0) {
        ArrayList<CacheConfiguration> list = getCConfigurations();

        ignite0.createCaches(list);
    }

    @NotNull private ArrayList<CacheConfiguration> getCConfigurations() {
        ArrayList<CacheConfiguration> list = new ArrayList<>(2);

        list.add(new CacheConfiguration(DEFAULT_CACHE_NAME)
            .setStatisticsEnabled(false)
            .setCacheMode(CacheMode.REPLICATED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setRebalanceMode(CacheRebalanceMode.SYNC)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setQueryEntities(Collections.singletonList(
                new QueryEntity(Integer.class.getName(), CacheValueObj.class.getName())
                    .addQueryField("marker", String.class.getName(), null)
                    .addQueryField("id", Long.class.getName(), null)
                    .setIndexes(Collections.singletonList(new QueryIndex()
                        .setFieldNames(Collections.singletonList("marker"), true)))))
            .setAffinity(new RendezvousAffinityFunction(false, 32)));
        /*list.add(new CacheConfiguration(CACHE_2)
            .setStatisticsEnabled(false)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setRebalanceMode(CacheRebalanceMode.SYNC)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setQueryEntities(Collections.singletonList(
                new QueryEntity(Integer.class.getName(), CacheValueObj2.class.getName())
                    .addQueryField("name", String.class.getName(), null)
                    .addQueryField("id", Long.class.getName(), null)
                    .setIndexes(Collections.singletonList(new QueryIndex()
                        .setFieldNames(Collections.singletonList("name"), true)))))
            .setBackups(3)
            .setAffinity(new RendezvousAffinityFunction(false, 32)));*/
        return list;
    }

    /**
     *
     */
    private static class CacheValueObj {
        /** Payload. */
        private byte[] payload = new byte[10 * 1024];
        /** Marker. */
        private String marker;

        private long id;

        /**
         * @param marker Marker.
         */
        public CacheValueObj(String marker) {
            this.marker = marker;
            this.id = marker.hashCode();
        }
    }
}
