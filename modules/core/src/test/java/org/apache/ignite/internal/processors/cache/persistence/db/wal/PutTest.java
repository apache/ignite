package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

public class PutTest extends GridCommonAbstractTest {
    /** Cache name. */
    private final String cacheName = "cache";

    /** */
    private int pageSize = 4 * 1024;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(2 * 1024 * 1024 * 1024L).setPersistenceEnabled(true).setCheckpointPageBufferSize(256 * 1024 * 1024L))
            .setWalMode(WALMode.LOG_ONLY)
            .setPageSize(pageSize)
            .setWalHistorySize(1);


        cfg.setDataStorageConfiguration(memCfg);

        CacheConfiguration ccfg = new CacheConfiguration(cacheName);
        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setBackups(0);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
    }

    public void testPut() throws Exception {
        final Ignite ignite = startGrid(0);

        ignite.active(true);

        final AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                IgniteCache<TestKey, TestValue> cache = ignite.cache(cacheName);

                int cnt = 0;

                while (!stop.get()) {
                    cache.put(new TestKey(), new TestValue());

                    if (++cnt % 10_000 == 0)
                        log.info(cnt + " entries");
                }
            }
        }, 16, "put-thread");

        long startTime = System.currentTimeMillis();

        while (System.currentTimeMillis() - startTime < 60000)
            U.sleep(100);

        stop.set(true);

        try {
            fut.get();
        }
        finally {
            stopAllGrids();
        }
    }

    private static class TestKey {
        long id = ThreadLocalRandom.current().nextLong();
        String value = Long.toString(id);
    }

    private static class TestValue {
        String s0 = UUID.randomUUID().toString();
        String s1 = UUID.randomUUID().toString();
        String s2 = UUID.randomUUID().toString();
        String s3 = UUID.randomUUID().toString();
        String s4 = s2 + s3;
        String s5 = UUID.randomUUID().toString();
        String s6 = UUID.randomUUID().toString();
        String s7 = UUID.randomUUID().toString();
        String s8 = UUID.randomUUID().toString();
        String s9 = s7 + s8;
    }
}
