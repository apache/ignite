package org.apache.ignite.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.EVICTED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;

/**
 */
public class RebalanceWhenRemoveWalSegment extends GridCommonAbstractTest {
    /** Ignite 0. */
    IgniteEx ignite0;

    /** Ignite 1. */
    IgniteEx ignite1;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setRebalanceThreadPoolSize(3)
            .setConsistentId(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setMaxWalArchiveSize(1024 * 1024 * 1024)
                .setWalSegments(2)
                .setWalSegmentSize(512 * 1024)
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)))
            .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setAffinity(new RendezvousAffinityFunction(false, 32))
                .setBackups(1));
    }

    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        stopAllGrids();
        cleanPersistenceDir();

        System.setProperty(IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD, "1");

        ignite0 = startGrid(0);
        ignite1 = startGrid(1);

        ignite0.cluster().active(true);
    }

    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     *
     */
    public void test() throws Exception {
        try (IgniteDataStreamer streamer = ignite0.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < 1_000; i++)
                streamer.addData(i, new byte[1024]);
        }

        forceCheckpoint();

        ignite1.close();

        try (IgniteDataStreamer streamer = ignite0.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 1_000; i < 5_000; i++)
                streamer.addData(i, new byte[1024]);
        }

        forceCheckpoint();

//        if (false)
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(U.defaultWorkDirectory())
            .resolve("db/wal/archive/" + U.maskForFileName(getTestIgniteInstanceName(0))))) {
            stream.forEach((path) -> {
                info("!!! Delating file: " + path.toString());

                U.delete(path);
            });
        }

        ignite1 = startGrid(1);

//        manualCacheRebalancing(ignite0, DEFAULT_CACHE_NAME);
//        manualCacheRebalancing(ignite1, DEFAULT_CACHE_NAME);

        awaitPartitionMapExchange();

        checkAllPartitionsState(DEFAULT_CACHE_NAME, OWNING);

        IdleVerifyResultV2 res = idleVerify(ignite0, DEFAULT_CACHE_NAME);

        if (res.hasConflicts()) {
            StringBuilder b = new StringBuilder();

            res.print(b::append);

            fail(b.toString());
        }

        IgniteCache cache = ignite0.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 5_000; i++)
            assertNotNull("Entry by key " + i + " is empty.", cache.get(i));
    }

    /**
     * @param cacheName Cache name.
     * @param expState Expected state.
     */
    private void checkAllPartitionsState(String cacheName, GridDhtPartitionState expState) {
        for (Ignite node : G.allGrids()) {
            ClusterNode clusterNode = node.cluster().localNode();

            GridCacheAdapter cache = ((IgniteEx)node).context().cache().internalCache(cacheName);

            if (clusterNode.isClient() || clusterNode.isDaemon() ||
                !ignite0.context().discovery().cacheAffinityNode(clusterNode, cacheName) ||
                cache.isLocal() ||
                !ignite0.context().cache().cacheDescriptor(cacheName).groupDescriptor().persistenceEnabled())
                continue;

            log.info("Test state [node=" + clusterNode.id() +
                ", name= " + node.name() +
                ", cache=" + cacheName +
                ", parts=" + cache.affinity().allPartitions(clusterNode).length +
                ", state=" + expState +
                ", primary=" + Arrays.toString(ignite0.affinity(cacheName).primaryPartitions(clusterNode)) + ']');

            GridDhtPartitionTopology top = cache.context().topology();

            GridDhtPartitionMap partsMap = top.partitions(clusterNode.id());

            for (int p = 0; p < cache.affinity().partitions(); p++) {
                GridDhtPartitionState state = partsMap.get(p);

                if (state == EVICTED || state == null)
                    continue;

                info("Prt: " + p + " sate: " + state);

                assertEquals("Unexpected state [checkNode=" + clusterNode.id() +
                        ", node=" + node.name() +
                        ", state=" + state + ']',
                    expState, state);
            }
        }
    }

}
