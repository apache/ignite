package org.apache.ignite.internal.processors.cache.distributed;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Created by gridgain on 19.07.17.
 */
public class CacheInitOnCoordinatorFailureTest extends GridCommonAbstractTest {

    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder();

    static {
        IP_FINDER.setAddresses(Collections.singletonList("127.0.0.1:47500..47509"));
    }

    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        cfg.setPeerClassLoadingEnabled(false);

        return cfg;
    }


    private final ExecutorService executor = Executors.newFixedThreadPool(2);

    public void testCreateAndDestroyCaches() throws Exception {
        log().info("Starting grids...");
        final IgniteEx i1 = startGrid(1);
        final IgniteEx i2 = startGrid(2);
        final IgniteEx i3 = startGrid(3);

        log().info(
            "Cluster nodes count: "
                + i1.cluster().forServers().nodes().size());

        final String c1Name = "cache-1";

        final Runnable destroyProc = new Runnable() {
            @Override public void run() {
                log().info("Awaiting 0.5 sec...");
                try {
                    Thread.sleep(500);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                log().info("Destroying cache['" + c1Name + "']...");
                i1.destroyCache(c1Name);
                log().info("Cache[" + c1Name + "] destroyed.");
            }
        };

        executor.submit(new Runnable() {
            @Override public void run() {
                log().info("Creating cache['" + c1Name + "']...");
                executor.submit(destroyProc);
                final long startMillis = System.currentTimeMillis();
                final long startNanos = System.nanoTime();
                i2.getOrCreateCache(c1Name);
                final long elapsedNanos = System.nanoTime() - startNanos;
                final long elapsedMillis = System.currentTimeMillis() - startMillis;
                log().info("Cache[" + c1Name + "] created in "
                    + elapsedMillis + " ms ("
                    + elapsedNanos + " ns)");
            }
        });

        log().info("Awaiting 5 sec...");
        try {
            Thread.sleep(5000);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        log().info("Complete.");
        stopAllGrids();
    }
}
