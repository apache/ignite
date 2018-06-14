package org.apache.ignite.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 */
public class CachesImpactOnExchangeTest extends GridCommonAbstractTest {
    /** Nodes. */
    public static final int NODES = 3;
    /** Cachecs. */
    public static final int CACHECS = 300;
    /** Restarted. */
    private boolean restarted;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration[] ccfgs = new CacheConfiguration[CACHECS];

        for (int i=0; i<ccfgs.length; i++)
            ccfgs[i] = new CacheConfiguration("cache-" + i)
                .setCacheMode(CacheMode.PARTITIONED)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setRebalanceMode(CacheRebalanceMode.SYNC)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setBackups(3)
                .setAffinity(new RendezvousAffinityFunction(false, 32));

        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setMaxSize(200L * 1024 * 1024)
                            .setPersistenceEnabled(true)))
            .setCacheConfiguration(ccfgs)
            .setUserAttributes(restarted ? Collections.singletonMap("restarted", 1) : null);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        U.resolveWorkDirectory(U.defaultWorkDirectory(), "cp", true);
        U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", true);
    }

    /**
     *
     */
    public void test() throws Exception {
        Ignite ignite0 = startGrids(NODES);
        Ignite ignite1 = ignite(1);

        ignite0.cluster().active(true);

        awaitPartitionMapExchange();

        ExecutorService pool = Executors.newFixedThreadPool(10);

        for (int i=0; i< CACHECS; i++) {
            final String cacheName = "cache-" + i;

            pool.submit(new Runnable() {
                @Override public void run() {
                    try (IgniteDataStreamer streamer = ignite0.dataStreamer(cacheName)) {
                        for (int j=0; j<32; j++)
                            streamer.addData(j, cacheName + " val " + j);
                    }
                }
            });
        }

        pool.shutdown();

        String ignite1ConsistId = ignite1.cluster().localNode().consistentId().toString();

        ignite1.close();

        clearDbFolders(ignite1ConsistId);

        awaitPartitionMapExchange();

        restarted = true;

        ignite1 = startGrid(1);

        awaitPartitionMapExchange();
    }

    private void clearDbFolders(String consistencePath) throws Exception {
        U.resolveWorkDirectory(U.defaultWorkDirectory(), "db/" + U.maskForFileName(consistencePath), true);
        U.resolveWorkDirectory(U.defaultWorkDirectory(), "db/wal/" + U.maskForFileName(consistencePath), true);
        U.resolveWorkDirectory(U.defaultWorkDirectory(), "db/wal/archive/" + U.maskForFileName(consistencePath), true);
    }

}
