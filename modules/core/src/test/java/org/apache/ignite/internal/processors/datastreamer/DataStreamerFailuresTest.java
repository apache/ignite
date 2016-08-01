package org.apache.ignite.internal.processors.datastreamer;

import java.util.Collection;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.*;
import org.apache.ignite.stream.StreamReceiver;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Test failures for Data streamer.
 */
public class DataStreamerFailuresTest extends GridCommonAbstractTest {
    /** Cache name. */
    public static final String CACHE_NAME = "cacheName";

    /** Timeout. */
    public static final int TIMEOUT = 1_000;

    /** Amount of entries. */
    public static final int ENTRY_AMOUNT = 1_000;

    /** Client id. */
    public static final int CLIENT_ID = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (getTestGridName(CLIENT_ID).equals(gridName))
            cfg.setClientMode(true);
        else
            cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    /**
     * Gets cache configuration.
     *
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration() {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setBackups(1);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setName(CACHE_NAME);

        return cacheCfg;
    }

    /**
     * Test fail on receiver, when streamer invokes from server node.
     * @throws Exception If fail.
     */
    public void testFromServer() throws Exception {
        try {
            Ignite ignite1 = startGrid(0);
            Ignite ignite2 = startGrid(1);

            checkTopology(2);

            IgniteFuture[] futures = loadData(ignite1);

            checkFeatures(futures);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Test fail on receiver, when streamer invokes from client node.
     * @throws Exception If fail.
     */
    public void testFromClient() throws Exception {
        try {
            Ignite ignite1 = startGrid(0);
            Ignite ignite2 = startGrid(1);
            Ignite client = startGrid(CLIENT_ID);

            checkTopology(3);

            IgniteFuture[] futures = loadData(client);

            checkFeatures(futures);
        }
        finally {
            stopAllGrids();
        }
    }

    private void checkFeatures(IgniteFuture[] futures) {
        boolean thrown = false;

        for (int i = 0; i < ENTRY_AMOUNT; i++) {
            try {
                try {
                    futures[i].get(TIMEOUT);
                }
                catch (IgniteFutureTimeoutException e) {
                    info("Check future with id " + i + " timeout");
                    info("Check again " + i);
                    try {
                        futures[i].get(10 * TIMEOUT);
                    }
                    catch (IgniteFutureTimeoutException e1) {
                        info("Timeout future " + i);
                    }
                }
            }
            catch (CacheException e) {
                thrown = true;
            }
        }

        assertTrue(thrown);
    }

    /**
     * @param ignite Ignite.
     */
    private IgniteFuture[] loadData(Ignite ignite) {
        boolean thrown = false;

        IgniteDataStreamer ldr = ignite.dataStreamer(CACHE_NAME);

        IgniteFuture[] futures = new IgniteFuture[ENTRY_AMOUNT];

        try {
            ldr.receiver(new TestDataReceiver());
            ldr.perNodeBufferSize(ENTRY_AMOUNT/6);
            ldr.perNodeParallelOperations(1);
            ((DataStreamerImpl)ldr).maxRemapCount(0);

            for (int i = 0; i < ENTRY_AMOUNT; i++)
                futures[i] = ldr.addData(i, i);
        }
        finally {
            try {
                ldr.close();
            } catch (Exception e) {
                thrown = true;
            }
        }

        assertTrue(thrown);

        return futures;
    }

    /**
     * Test receiver for timeout expiration emulation.
     */
    private static class TestDataReceiver implements StreamReceiver {

        /** Is first. */
        boolean isFirst = true;

        /** {@inheritDoc} */
        @Override public void receive(IgniteCache cache, Collection collection) throws IgniteException {
            throw new IgniteException("Error in TestDataReceiver.");
        }
    }

}
