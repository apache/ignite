package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

/**
 * Reproducer for atomic cache inconsistency state.
 * WriteSynchronizationMode: PRIMARY_SYNC
 */
@SuppressWarnings("ErrorNotRethrown")
public class ReproducerAtomicCacheInconsistentState2Test extends GridCommonAbstractTest {
    public void testWriteFullAsync() {
        Integer key = 100;

        IgniteCache<Integer, Integer> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        cache.put(key, 0);

        singleCommunicationFail(key);

        cache.put(key, 5);

        doSleep(2_000);

        cache = cacheFromBackupNode(key);

        assertNotNull(cache);

        assertEquals(5, (int)cacheFromBackupNode(key).get(key));
    }

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setNetworkSendRetryCount(1)
            .setCacheConfiguration(cacheConfiguration())
            .setCommunicationSpi(
                new TestCommunicationSpi()
                    .setSharedMemoryPort(-1)
            );
    }

    protected CacheConfiguration cacheConfiguration() {
        return new CacheConfiguration(DEFAULT_CACHE_NAME)
            .setCacheMode(PARTITIONED)
            .setBackups(1)
            .setWriteSynchronizationMode(PRIMARY_SYNC)
            .setRebalanceMode(SYNC)
            .setReadFromBackup(true);
    }

    @Override protected void beforeTest() throws Exception {
        startGrids(2);
    }

    private IgniteCache<Integer, Integer> cacheFromBackupNode(Integer key) {
        Affinity<Integer> aff = affinity(grid(0).cache(DEFAULT_CACHE_NAME));
        for (int i = 0; i < 2; i++) {
            if (aff.isBackup(grid(i).localNode(), key))
                return grid(i).cache(DEFAULT_CACHE_NAME);
        }
        return null;
    }

    private void singleCommunicationFail(Integer key) {
        Affinity<Integer> aff = affinity(grid(0).cache(DEFAULT_CACHE_NAME));
        for (int i = 0; i < 2; i++) {
            if (aff.isPrimary(grid(i).localNode(), key)) {
                TestCommunicationSpi spi = (TestCommunicationSpi)ignite(i).configuration().getCommunicationSpi();
                spi.fail = true;
            }
        }
    }

    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        private volatile boolean fail = false;

        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackClosure)
            throws IgniteSpiException {
            if (msg instanceof GridIoMessage) {
                Object msg0 = ((GridIoMessage)msg).message();

                if (fail && msg0 instanceof GridDhtAtomicAbstractUpdateRequest) {
                    fail = false;
                    throw new IgniteSpiException("Test error");
                }
            }

            super.sendMessage(node, msg, ackClosure);
        }
    }
}
