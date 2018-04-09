package org.apache.ignite.internal;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsAbstractMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

public class IgniteClientReconnectDelayedSpiTest extends IgniteClientReconnectAbstractTest {

    /** */
    private static final int SRV_CNT = 3;

    /** */
    private static final String PRECONFIGURED_CACHE = "preconfigured-cache";

    /** */
    private static final Map<UUID, Runnable> recordedMessages = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TestCommunicationDelayedSpi delayedCommSpi = new TestCommunicationDelayedSpi();
        delayedCommSpi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(delayedCommSpi);
        cfg.setCacheConfiguration(new CacheConfiguration(PRECONFIGURED_CACHE));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected int serverCount() {
        return SRV_CNT;
    }

    /**
     * @throws Exception If failed.
     */
    public void testReconnectCacheDestroyedWithDelayedSpiMessages() throws Exception {
        clientMode = true;

        final Ignite client = startGrid();
        final Ignite srv = clientRouter(client);

        client.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        reconnectClientNode(client, srv, new Runnable() {
            @Override public void run() {
                srv.destroyCache(DEFAULT_CACHE_NAME);

                CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

                ccfg.setAtomicityMode(TRANSACTIONAL);

                srv.getOrCreateCache(ccfg);
            }
        });

        IgniteCache<Object, Object> clientCache0 = client.cache(DEFAULT_CACHE_NAME);

        // Resend delayed GridDhtPartitionsSingleMessage
        for (Runnable r : recordedMessages.values())
            r.run();

        final GridDiscoveryManager srvDisco = ((IgniteKernal)srv).context().discovery();
        final ClusterNode clientNode = ((IgniteKernal)client).localNode();

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return F.eq(true, srvDisco.cacheClientNode(clientNode, DEFAULT_CACHE_NAME));
            }
        }, 5000));

        clientCache0.put(1, 1);

        assertEquals(1, clientCache0.get(1));
    }

    /**
     *
     */
    private static class TestCommunicationDelayedSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackClosure)
            throws IgniteSpiException {
            final Object msg0 = ((GridIoMessage)msg).message();

            if (msg0 instanceof GridDhtPartitionsSingleMessage &&
                ((GridDhtPartitionsAbstractMessage)msg0).exchangeId() == null)
                recordedMessages.putIfAbsent(node.id(), new Runnable() {
                    @Override
                    public void run() {
                        TestCommunicationDelayedSpi.super.sendMessage(node, msg, ackClosure);
                    }
                });
            else
                try {
                    super.sendMessage(node, msg, ackClosure);
                }
                catch (Exception e) {
                    U.log(null, e);
                }
        }
    }
}
