package org.apache.ignite.internal.processors.cache.distributed;

import com.google.common.util.concurrent.Callables;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 *
 */
public class Reproducer extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 5;

    /** */
    private static final String CACHE_NAME_PREF = "cache-";

    /**
     * @throws Exception If failed.
     */
    public void testClientCacheCreateDestroyFailover() throws Exception {
        startGridsMultiThreaded(GRID_CNT);

        Ignition.setClientMode(true);

        Ignite client = startGrid(GRID_CNT);

        Ignition.setClientMode(false);

        final AtomicBoolean finished = new AtomicBoolean();

        IgniteInternalFuture<?> fut = restartThread(finished);

        long stop = System.currentTimeMillis() + 60_000;

        IgniteCache<String, Boolean> defCache = client.createCache(ccfg(CACHE_NAME_PREF));

        try {
            int iter = 0;

            while (System.currentTimeMillis() < stop) {
                log.info("Iteration: " + iter++);

                try {
                    for (int i = 0; i < 100; i++) {
                        String cacheName = CACHE_NAME_PREF + i;

                        CacheConfiguration<?, ?> ccfg = ccfg(cacheName);

                        defCache.put(cacheName, Boolean.FALSE.equals(defCache.get(cacheName)));

                        client.getOrCreateCache(ccfg).destroy();
                    }
                }
                catch (IgniteClientDisconnectedException e) {
                    e.reconnectFuture().get();
                }
                catch (CacheException e) {
                    if (e.getCause() instanceof IgniteClientDisconnectedException)
                        ((IgniteClientDisconnectedException)e.getCause()).reconnectFuture().get();
                    else
                        throw e;
                }
            }

            finished.set(true);

            fut.get();
        }
        finally {
            finished.set(true);

            IgniteCache c = client.cache(CACHE_NAME_PREF);

            if (c != null)
                c.destroy();
        }
    }

    /**
     * @param name Cache name.
     * @return Cache configuration.
     */
    private <K,V> CacheConfiguration<K,V> ccfg(String name) {
        CacheConfiguration<K,V> cacheCfg = new CacheConfiguration<>(name);

        cacheCfg.setBackups(1);
        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        return cacheCfg;
    }

    /**
     * @param finished Finished flag.
     * @return Future.
     */
    private IgniteInternalFuture<?> restartThread(final AtomicBoolean finished) {
        return GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                while (!finished.get()) {
                    for (int i = 0; i < GRID_CNT; i++) {
                        log.info("Stop node: " + i);

                        stopGrid(i);

                        U.sleep(500);

                        log.info("Start node: " + i);

                        startGrid(i);

                        if (finished.get())
                            break;
                    }
                }

                return null;
            }
        }, "restart-thread");
    }
}