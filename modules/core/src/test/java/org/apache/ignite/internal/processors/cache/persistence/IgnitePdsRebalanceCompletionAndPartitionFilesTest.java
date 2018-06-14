package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 */
public class IgnitePdsRebalanceCompletionAndPartitionFilesTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Cache name. */
    private static final String cacheName = "cache";
    public static boolean clientMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = new CacheConfiguration(cacheName)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setRebalanceMode(CacheRebalanceMode.SYNC)
            .setBackups(1)
            .setRebalanceDelay(10_000)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_SAFE);

        DataRegionConfiguration memPlcCfg = new DataRegionConfiguration()
            .setName("dfltDataRegion")
            .setMaxSize(150 * 1024 * 1024)
            .setInitialSize(100 * 1024 * 1024)
            .setPersistenceEnabled(true);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setConcurrencyLevel(Runtime.getRuntime().availableProcessors())
            .setPageSize(1024)
            .setWalMode(WALMode.LOG_ONLY)
            .setDefaultDataRegionConfiguration(memPlcCfg);

        cfg.setClientMode(clientMode)
            .setConsistentId(gridName)
            .setDataStorageConfiguration(memCfg)
            .setCacheConfiguration(ccfg)
            .setDiscoverySpi(
                new TcpDiscoverySpi()
                    .setIpFinder(IP_FINDER)
            );

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testOwningPartitionsOnRebalance() throws Exception {
        IgniteEx ignite0 = (IgniteEx)startGrids(3);
        IgniteEx ignite1 = (IgniteEx)ignite(1);
        IgniteEx ignite2 = (IgniteEx)ignite(2);

        ignite0.cluster().active(true);

        try (IgniteDataStreamer streamer = ignite0.dataStreamer(cacheName)) {
            for (int i = 0; i < 1_000; i++)
                streamer.addData(i, "Value " + i);
        }

        assertEquals(1, ignite0.cacheNames().size());

        waitForRebalancing();
        awaitPartitionMapExchange(true, true, null);

        checkPartFiles(ignite0);
        checkPartFiles(ignite1);
        checkPartFiles(ignite2);

        ignite2.close();

        ignite0.cluster().setBaselineTopology(ignite0.cluster().topologyVersion());

        waitForRebalancing();
        awaitPartitionMapExchange(true, true, null);

        checkPartFiles(ignite0);
        checkPartFiles(ignite1);
//        checkPartFiles(ignite2);

        ignite2 = startGrid(2);

        ignite0.cluster().setBaselineTopology(ignite0.cluster().topologyVersion());

        waitForRebalancing();
        awaitPartitionMapExchange(true, true, null);

        checkPartFiles(ignite0);
        checkPartFiles(ignite1);
        checkPartFiles(ignite2);
    }

    /**
     * @param ignite Ignite.
     */
    private void checkPartFiles(IgniteEx ignite) throws Exception {
        int[] parts = ignite.affinity(cacheName).allPartitions(ignite.localNode());

        Path dirPath = Paths.get(U.defaultWorkDirectory(), "db", ignite.configuration().getIgniteInstanceName().replace(".", "_"), "cache-" + cacheName);

        info("Path: " + dirPath.toString());

        assertTrue(Files.exists(dirPath));

        for (File f : dirPath.toFile().listFiles()) {
            if (f.getName().startsWith("part-"))
                assertTrue("Node should contains only partiotns "
                    + Arrays.toString(parts)
                    + ", but the file is redundant: "
                    + f.toPath(), Arrays.stream(parts).anyMatch(part -> f.getName().equals("part-" + part + ".bin")));
        }

    }
}
