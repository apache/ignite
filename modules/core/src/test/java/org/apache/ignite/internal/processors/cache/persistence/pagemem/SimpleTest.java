package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.size.DataStructureSize;
import org.apache.ignite.internal.pagemem.size.DataStructureSizeContext;
import org.apache.ignite.internal.pagemem.size.DataStructureSizeNode;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.INTERNAL;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.PURE_DATA;
import static org.apache.ignite.internal.pagemem.size.DataStructureSizeUtils.TOTAL;

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
            new CacheConfiguration(IN_MEMORY_CACHE)
                .setDataRegionName(IN_MEMORY_REGION)
                .setAffinity(new RendezvousAffinityFunction(false, 1)),
            new CacheConfiguration(IN_MEMORY_CACHE_1_GROUP)
                .setDataRegionName(IN_MEMORY_REGION)
                .setGroupName(IN_MEMORY_GROUP_NAME)
                .setAffinity(new RendezvousAffinityFunction(false, 1)),
            new CacheConfiguration(IN_MEMORY_CACHE_2_GROUP)
                .setDataRegionName(IN_MEMORY_REGION)
                .setGroupName(IN_MEMORY_GROUP_NAME)
                .setAffinity(new RendezvousAffinityFunction(false, 1)),
            //------------------------------------------------
            new CacheConfiguration(PERSISTENT_CACHE)
                .setDataRegionName(PERSISTENT_REGION)
                .setAffinity(new RendezvousAffinityFunction(false, 1)),
            new CacheConfiguration(PERSISTENT_CACHE_1_GROUP)
                .setDataRegionName(PERSISTENT_REGION)
                .setGroupName(PERSISTENT_GROUP_NAME)
                .setAffinity(new RendezvousAffinityFunction(false, 1)),
            new CacheConfiguration(PERSISTENT_CACHE_2_GROUP)
                .setDataRegionName(PERSISTENT_REGION)
                .setGroupName(PERSISTENT_GROUP_NAME)
                .setAffinity(new RendezvousAffinityFunction(false, 1))
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
        foo(1000);
    }

    public void foo(int entries) throws Exception {
        IgniteEx ig = (IgniteEx)startGrid();

        ig.cluster().active(true);

        System.out.println("Before put");

        printSizes(ig.context().cache().context());

        Collection<String> cacheNames = ig.cacheNames();

        for (int i = 0; i < entries; i++) {
            for (String name : cacheNames)
                ig.cache(name).put(i, i);
        }

        System.out.println("After put");

        printSizes(ig.context().cache().context());

        for (int i = 0; i < entries; i++) {
            for (String name : cacheNames)
                ig.cache(name).remove(i);
        }

        System.out.println("After remove");

        printSizes(ig.context().cache().context());

        ig.cluster().active(false);
    }

    private void printSizes(GridCacheSharedContext ctx) {
        DataStructureSizeNode nodeLevel = ctx.database().dataStructureSize();

        //print(nodeLevel.structures(), "NODE [" + ctx.localNode().consistentId() + "]");

        List<DataStructureSize> regions = new ArrayList<>();

        List<DataStructureSizeContext> groups = new ArrayList<>();

        List<DataStructureSize> groupsSize = new ArrayList<>();

        for (DataStructureSizeContext region : nodeLevel.childes()) {
            regions.addAll(region.structures());

            groups.addAll(region.childes());
        }

        print(regions, "REGION");

        for (DataStructureSizeContext group : groups)
            groupsSize.addAll(group.structures());

        print(groupsSize, "GROUP");
    }

    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
    }

    private void print(Collection<DataStructureSize> sizes, String msg) {
        String leftAlignFormat = "| %-7s | %-9d | %-6d | %-6d | %-28s |%n";

        System.out.println(msg);
        System.out.format("+---------+-----------+--------+--------+------------------------------+%n");
        System.out.format("|  pages  |   bytes   |   kb   |   mb   |       name                   |%n");
        System.out.format("+---------+-----------+--------+--------+------------------------------+%n");

        sizes.forEach((ds) -> {
            String name = ds.name();

            if (!name.contains(PURE_DATA) && !name.contains(INTERNAL) && !name.contains(TOTAL)) {
                long size = ds.size();
                long byteSize = size * 4096;
                long kbSize = byteSize / 1024;
                long mbSize = kbSize / 1024;

                System.out.format(leftAlignFormat, String.valueOf(size), byteSize, kbSize, mbSize, name);
            }
            else {
                long size = ds.size();
                long kbSize = size / 1024;
                long mbSize = kbSize / 1024;

                System.out.format(leftAlignFormat, "N/A", size, kbSize, mbSize, name);
            }

        });

        System.out.format("+---------+-----------+--------+--------+------------------------------+%n\n");
    }
}
