package org.apache.ignite.internal.processors.cache.query.continuous;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.CacheQueryEntryEvent;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.continuous.GridContinuousMessage;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.eventstorage.memory.MemoryEventStorageSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class CacheContinuousQueryIsPrimaryFlagTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static volatile boolean err;

    /** */
    private CacheMode cacheMode;

    /** */
    private CacheAtomicityMode atomicityMode;

    /** */
    private boolean client;

    /** */
    private int backups = 1;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setLateAffinityAssignment(true);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setForceServerMode(true);
        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        TestCommunicationSpi commSpi = new TestCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(commSpi);

        MemoryEventStorageSpi evtSpi = new MemoryEventStorageSpi();
        evtSpi.setExpireCount(50);

        cfg.setEventStorageSpi(evtSpi);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(cacheMode);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setBackups(backups);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 8 * 60_000;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        err = false;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionedAtomic() throws Exception {
        cacheMode = PARTITIONED;
        atomicityMode = ATOMIC;

        leftPrimaryNode();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionedTx() throws Exception {
        cacheMode = PARTITIONED;
        atomicityMode = TRANSACTIONAL;

        leftPrimaryNode();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedAtomic() throws Exception {
        cacheMode = REPLICATED;
        atomicityMode = ATOMIC;

        leftPrimaryNode();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedTx() throws Exception {
        cacheMode = REPLICATED;
        atomicityMode = TRANSACTIONAL;

        leftPrimaryNode();
    }

    /**
     *
     */
    public void leftPrimaryNode() throws Exception {
        this.backups = 2;

        final int SRV_NODES = 3;

        startGridsMultiThreaded(SRV_NODES);

        client = true;

        final Ignite qryClient = startGrid(SRV_NODES);

        client = false;

        IgniteCache<Object, Object> qryClientCache = qryClient.cache(DEFAULT_CACHE_NAME);

        if (cacheMode != REPLICATED)
            assertEquals(backups, qryClientCache.getConfiguration(CacheConfiguration.class).getBackups());

        assertNull(qryClientCache.getConfiguration(CacheConfiguration.class).getNearConfiguration());

        ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

        final CacheEventListener lsnr = new CacheEventListener();

        qry.setLocalListener(lsnr);

        QueryCursor<?> cur = qryClientCache.query(qry);

        Object key = new Integer(1);

        qryClientCache.put(key, key);

        Ignite primary = primaryNode(key, qryClientCache.getName());

        TestCommunicationSpi spi = (TestCommunicationSpi)primary.configuration().getCommunicationSpi();

        spi.clientNode = qryClient.cluster().localNode();

        spi.skipFirstMsg = new AtomicBoolean(true);

        qryClientCache.put(key, key);

        spi.latch.await(5000L, TimeUnit.MILLISECONDS);

        stopGrid(primary.name(), true);

        awaitPartitionMapExchange();

        assert GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return qryClient.cluster().nodes().size() == (SRV_NODES + 1 /** client node */)
                    - 1 /** Primary node */;
            }
        }, 5000L);

        assertTrue(lsnr.isBackup.get());

        cur.close();

        stopAllGrids();
    }

    /**
     *
     */
    public static class CacheEventListener implements CacheEntryUpdatedListener<Object, Object> {
        /** Keys. */
        GridConcurrentHashSet<Integer> keys = new GridConcurrentHashSet<>();

        /** */
        private volatile static AtomicBoolean isBackup = new AtomicBoolean(false);

        @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> evts) throws CacheEntryListenerException {
            CacheEntryEvent<?, ?> e = evts.iterator().next();

            isBackup.set(e.unwrap(CacheQueryEntryEvent.class).isBackup());
        }
    }

    /**
     *
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** */
        private volatile static AtomicBoolean skipFirstMsg;

        /** */
        private volatile static ClusterNode clientNode = null;

        /** */
        private final static CountDownLatch latch = new CountDownLatch(1);

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
            throws IgniteSpiException {
            Object msg0 = ((GridIoMessage)msg).message();

            if (msg0 instanceof GridContinuousMessage) {
                if (clientNode != null && node != null) {
                    if (clientNode.id().toString().equals(node.id().toString()) && skipFirstMsg.compareAndSet(true, false)) {
                        assertEquals(clientNode.id().toString(), node.id().toString());

                        if (log.isDebugEnabled())
                            log.debug("Skip continuous message: " + msg0);

                        latch.countDown();

                        return;
                    }
                }
            }

            super.sendMessage(node, msg, ackC);
        }
    }
}
