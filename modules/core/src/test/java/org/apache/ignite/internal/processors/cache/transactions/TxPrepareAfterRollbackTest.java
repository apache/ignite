package org.apache.ignite.internal.processors.cache.transactions;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

public class TxPrepareAfterRollbackTest extends GridCommonAbstractTest {
    /**
     * Ip finder.
     */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** Nodes count. */
    private static final int NODES_CNT = 2;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        DataRegionConfiguration memCfg = new DataRegionConfiguration();
        memCfg.setPersistenceEnabled(true);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();
        dsCfg.setDefaultDataRegionConfiguration(memCfg);

        cfg.setDataStorageConfiguration(dsCfg);

        cfg.setCacheConfiguration(new CacheConfiguration(CACHE_NAME)
                .setAffinity(new RendezvousAffinityFunction(false, 32))
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setBackups(1));

        return cfg;
    }


    /**
     *
     */
    public void test() throws Exception {
        IgniteEx ig = (IgniteEx)startGrids(NODES_CNT);

        ig.cluster().active(true);

        IgniteCache<Object, Object> cache = ig.cache(CACHE_NAME);

        AtomicBoolean stop = new AtomicBoolean(false);

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                while (!stop.get()) {
                    try (Transaction tx = ig.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ, 0, 100)) {
                        int k = rnd.nextInt();
                        int v = rnd.nextInt();

                        cache.put(k, v);

                        k = rnd.nextInt();
                        v = rnd.nextInt();

                        cache.put(k, v);


                        tx.commit();
                    }
                }
            }
        }, 3, "tx-loader");

        U.sleep(3_000);

        IgniteEx node = startGrid(NODES_CNT);

        Collection<ClusterNode> nodes = ig.cluster().forServers().nodes();

        ig.cluster().setBaselineTopology(nodes);

        U.sleep(3_000);

        stop.set(true);

        fut.get();
    }
}
