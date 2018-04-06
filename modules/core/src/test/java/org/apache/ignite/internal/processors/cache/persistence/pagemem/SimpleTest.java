package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.pagemem.DataStructureSize;
import org.apache.ignite.internal.pagemem.DataStructureSizeNode;
import org.apache.ignite.internal.pagemem.DataStructureSizeNodeRootLevel;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GatewayProtectedCacheProxy;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.pagemem.DataStructureSizeUtils.INTERNAL;
import static org.apache.ignite.internal.pagemem.DataStructureSizeUtils.PURE_DATA;
import static org.apache.ignite.internal.pagemem.DataStructureSizeUtils.TOTAL;

public class SimpleTest extends GridCommonAbstractTest {
    /** */
    private static final String IN_MEMORY_REGION = "in_memory_region";

    /** */
    private static final String IN_MEMORY_GROUP_NAME = "in_memory_group";

    /** */
    private static final String IN_MEMORY_CACHE = "in_memory_cache";

    /** */
    private static final String IN_MEMORY_CACHE_1_GROUP = "in_memory_cache_group_1";

    /** */
    private static final String IN_MEMORY_CACHE_2_GROUP = "in_memory_cache_group_2";

    //-----------------------------------------

    /** */
    private static final String PERSISTENT_REGION = "persistent_region";

    /** */
    private static final String PERSISTENT_GROUP_NAME = "persistent_group";

    /** */
    private static final String PERSISTENT_CACHE = "persistent_cache";

    /** */
    private static final String PERSISTENT_CACHE_1_GROUP = "persistent_cache_group_1";

    /** */
    private static final String PERSISTENT_CACHE_2_GROUP = "persistent_cache_group_2";

    //-----------------------------------------

    /** */
    private static final String NODE_CONST_ID = "NODE";

    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setConsistentId(NODE_CONST_ID);

        cfg.setDiscoverySpi(
            new TcpDiscoverySpi()
                .setIpFinder(new TcpDiscoveryVmIpFinder(true))
        );

        cfg.setCacheConfiguration(
         /*   new CacheConfiguration(IN_MEMORY_CACHE)
                .setDataRegionName(IN_MEMORY_REGION)
                .setAffinity(new RendezvousAffinityFunction(false, 1)),
            new CacheConfiguration(IN_MEMORY_CACHE_1_GROUP)
                .setDataRegionName(IN_MEMORY_REGION)
                .setGroupName(IN_MEMORY_GROUP_NAME)
                .setAffinity(new RendezvousAffinityFunction(false, 1)),
            new CacheConfiguration(IN_MEMORY_CACHE_2_GROUP)
                .setDataRegionName(IN_MEMORY_REGION)
                .setGroupName(IN_MEMORY_GROUP_NAME)
                .setAffinity(new RendezvousAffinityFunction(false, 1)),*/
            //------------------------------------------------
            new CacheConfiguration(PERSISTENT_CACHE)
                .setDataRegionName(PERSISTENT_REGION)
                .setAffinity(new RendezvousAffinityFunction(false, 1))
       /*     new CacheConfiguration(PERSISTENT_CACHE_1_GROUP)
                .setDataRegionName(PERSISTENT_REGION)
                .setGroupName(PERSISTENT_GROUP_NAME)
                .setAffinity(new RendezvousAffinityFunction(false, 1)),
            new CacheConfiguration(PERSISTENT_CACHE_2_GROUP)
                .setDataRegionName(PERSISTENT_REGION)
                .setGroupName(PERSISTENT_GROUP_NAME)
                .setAffinity(new RendezvousAffinityFunction(false, 1))*/
        );

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDataRegionConfigurations(
                    new DataRegionConfiguration()
                        .setName(IN_MEMORY_REGION),
                    new DataRegionConfiguration()
                        .setName(PERSISTENT_REGION)
                        .setPersistenceEnabled(true)
                )
        );

        return cfg;
    }

    public void test() throws Exception {
        Ignite ig = startGrid();

        ig.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ig.cache(PERSISTENT_CACHE);

        GatewayProtectedCacheProxy<Integer, Integer> cacheProxy = (GatewayProtectedCacheProxy<Integer, Integer>)cache;

        CacheGroupContext groupContext = cacheProxy.context().group();

        System.out.println("Before put");

        printSizes(groupContext);

        for (int i = 0; i < 1_000_000; i++)
            cache.put(i, i);

        System.out.println("After put");

        printSizes(groupContext);

        GridCacheDatabaseSharedManager db =
            (GridCacheDatabaseSharedManager)cacheProxy.context().kernalContext().cache().context().database();

        db.waitForCheckpoint(null);

        for (int i = 0; i < 1_000_000; i++)
            cache.remove(i);

        System.out.println("After remove");

        printSizes(groupContext);

        ig.cluster().active(false);
    }

    private void printSizes(CacheGroupContext groupContext) {
        String leftAlignFormat = "| %-7s | %-9d | %-6d | %-6d | %-28s |%n";

        System.out.format("+---------+-----------+--------+--------+------------------------------+%n");
        System.out.format("|  pages  |   bytes   |   kb   |   mb   |       name                   |%n");
        System.out.format("+---------+-----------+--------+--------+------------------------------+%n");

        DataStructureSizeNodeRootLevel dsSize = groupContext.shared().database().dataStructureSize();

        DataStructureSizeNode group = null;

        for (DataStructureSizeNode ds : dsSize.childes()) {
            Collection<DataStructureSizeNode> childes = ds.childes();

            for (DataStructureSizeNode ds1 : childes) {
                if (ds1.name().equals(groupContext.cacheOrGroupName())) {
                    group = ds1;
                    break;
                }
            }
        }

        Collection<DataStructureSize> structures = group.structures();

        structures.forEach((ds) -> {
            String name = ds.name();

            long size = ds.size();
            long byteSize = size * 4096;
            long kbSize = byteSize / 1024;
            long mbSize = kbSize / 1024;

          /*  if (!k.contains("pure") && !k.contains("internal"))
                System.out.println("\t[" + v.size() + " pages | " + byteSize + " b | " + kbSize + " kb]" + "  " + k);
            else
                System.out.println("\t[" + size + " b | " + size / 1024 + " kb]" + "  " + k);*/

            if (!name.contains(PURE_DATA) && !name.contains(INTERNAL) && !name.contains(TOTAL))
                System.out.format(leftAlignFormat, String.valueOf(size), byteSize, kbSize, mbSize, name);
            else
                System.out.format(leftAlignFormat, "N/A", size, size / 1024, (size / 1024) / 1024, name);

        });

        System.out.format("+---------+-----------+--------+--------+------------------------------+%n");
    }

    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
    }
}
