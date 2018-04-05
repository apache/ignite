package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.pagemem.DataStructureSizeManager;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GatewayProtectedCacheProxy;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.pagemem.DataStructureSizeManager.INTERNAL;
import static org.apache.ignite.internal.pagemem.DataStructureSizeManager.PURE_DATA;
import static org.apache.ignite.internal.pagemem.DataStructureSizeManager.TOTAL;

public class SimpleTest extends GridCommonAbstractTest {
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setConsistentId("NODE");

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(new TcpDiscoveryVmIpFinder(true)));

        cfg.setCacheConfiguration(
            new CacheConfiguration("myCache")
                .setAffinity(
                    new RendezvousAffinityFunction(false, 1)
                )
        );

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
        );

        return cfg;
    }

    public void test() throws Exception {
        Ignite ig = startGrid();

        ig.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ig.cache("myCache");

        GatewayProtectedCacheProxy<Integer, Integer> cacheProxy = (GatewayProtectedCacheProxy<Integer, Integer>)cache;

        CacheGroupContext groupContext = cacheProxy.context().group();

        System.out.println("Before put");

        printSizes(groupContext);

        for (int i = 0; i < 1; i++)
            cache.put(i, i);

        System.out.println("After put");

        printSizes(groupContext);

        GridCacheDatabaseSharedManager db =
            (GridCacheDatabaseSharedManager)cacheProxy.context().kernalContext().cache().context().database();

        db.waitForCheckpoint(null);

        for (int i = 0; i < 1; i++)
            cache.remove(i);

        System.out.println("After remove");

        printSizes(groupContext);

        ig.cluster().active(false);
    }

    private void printSizes(CacheGroupContext groupContext) {
        String leftAlignFormat = "| %-7s | %-9d | %-6d | %-28s |%n";

        System.out.format("+---------+-----------+--------+------------------------------+%n");
        System.out.format("|  pages  |   bytes   |   kb   |       name                   |%n");
        System.out.format("+---------+-----------+--------+------------------------------+%n");

        groupContext.getDsMgr().structureSizes().forEach((k, v) -> {
            long size = v.size();
            long byteSize = size * 4096;
            long kbSize = byteSize / 1024;

          /*  if (!k.contains("pure") && !k.contains("internal"))
                System.out.println("\t[" + v.size() + " pages | " + byteSize + " b | " + kbSize + " kb]" + "  " + k);
            else
                System.out.println("\t[" + size + " b | " + size / 1024 + " kb]" + "  " + k);*/

            if (!k.contains(PURE_DATA) && !k.contains(INTERNAL) && !k.contains(TOTAL))
                System.out.format(leftAlignFormat, String.valueOf(v.size()), byteSize, kbSize, k);
            else
                System.out.format(leftAlignFormat, "N/A", v.size(), v.size() / 1024, k);

        });

        System.out.format("+---------+-----------+--------+------------------------------+%n");
    }

    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
    }
}
