package org.apache.ignite.cache.affinity;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public abstract class AbstractAffinityRebalancingTest extends GridCommonAbstractTest {

    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int ITERATIONS = 64;

    /** partitioned cache name. */
    protected static final String CACHE_NAME_DHT_PARTITIONED = "cacheP";

    /** replicated cache name. */
    protected static final String CACHE_NAME_DHT_REPLICATED = "cacheR";

    /** Ignite. */
    private static Ignite ignite1;

    /** Ignite. */
    private static Ignite ignite2;

    /** Ignite. */
    private static Ignite ignite3;

    /**
     * Should be parametrized by inheritances.
     * @param ignite Affinity function would be set in it.
     * @return Affinity function to test.
     */
    protected abstract AffinityFunction affinityFunction(Ignite ignite);

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        ignite1 = startGrid(0);
        ignite2 = startGrid(1);
        ignite3 = startGrid(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TimeUnit.MINUTES.toMillis(5);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration iCfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)iCfg.getDiscoverySpi()).setIpFinder(ipFinder);
        iCfg.setRebalanceThreadPoolSize(2);

        return iCfg;
    }

    public void testCacheStopping() throws Exception {
        affinityFunction(ignite1);
        affinityFunction(ignite2);
        affinityFunction(ignite3);

        final int delta = 5;

        GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {

                for(int start = 0; Ignition.allGrids().contains(ignite1); start += delta) {
                    fillWithCache(ignite2, delta, start);

                    for (String victim : ignite2.cacheNames())
                        ignite2.getOrCreateCache(victim).put(start, delta);

                    for (String victim : ignite1.cacheNames())
                        ignite1.destroyCache(victim);
                }

                return null;
            }
        }, "CacheSerialKiller");

        for(int i = 5; i < ITERATIONS + 5; i++) {
            Ignite ignite4 = startGrid(i);
            affinityFunction(ignite4);

            stopGrid(i);
        }
    }

    private static void fillWithCache(Ignite ignite, int iterations, int start) {
        for(int i = start; i < iterations + start; i++) {
            CacheConfiguration<Integer, Integer> cachePCfg = new CacheConfiguration<>();

            cachePCfg.setName(CACHE_NAME_DHT_PARTITIONED + i);
            cachePCfg.setCacheMode(CacheMode.PARTITIONED);
            cachePCfg.setBackups(1);

            ignite.getOrCreateCache(cachePCfg);

            CacheConfiguration<Integer, Integer> cacheRCfg = new CacheConfiguration<>();

            cacheRCfg.setName(CACHE_NAME_DHT_REPLICATED + i);
            cacheRCfg.setCacheMode(CacheMode.REPLICATED);
            cachePCfg.setBackups(0);

            ignite.getOrCreateCache(cacheRCfg);
        }
    }
}