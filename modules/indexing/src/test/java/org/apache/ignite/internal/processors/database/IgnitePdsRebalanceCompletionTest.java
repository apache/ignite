package org.apache.ignite.internal.processors.database;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopologyImpl;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 */
public class IgnitePdsRebalanceCompletionTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Cache name. */
    private static final String cacheName = "cache";
    /** Cache name 2. */
    private static final String cacheName2 = "cache2";
    /** Cache group name. */
    public static final String GRP = "grp1";
    /** Client mode. */
    public static boolean clientMode;
    /** Data center identity. */
    private static final String DATA_CENTER_IDENTITY = "dc";
    /** Dc attr. */
    private String dcAttr = "a";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = new CacheConfiguration(cacheName)
            .setGroupName(GRP)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setRebalanceMode(CacheRebalanceMode.SYNC)
            .setBackups(1)
            .setRebalanceDelay(10_000)
            .setQueryEntities(Collections.singletonList(
                new QueryEntity(Integer.class.getName(), CacheValueObj.class.getName())
                    .addQueryField("marker", String.class.getName(), null)
//                    .addQueryField("id", Long.class.getName(), null)
                    .setIndexes(Collections.singletonList(new QueryIndex()
                        .setFieldNames(Collections.singletonList("marker"), true)))))
            .setAffinity(new RendezvousAffinityFunction(false, 64)
                .setAffinityBackupFilter(new DPLAffinityBackupFilter(1, 2)))
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_SAFE);

        CacheConfiguration ccfg2 = new CacheConfiguration(cacheName2)
            .setGroupName(GRP)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setRebalanceMode(CacheRebalanceMode.SYNC)
            .setBackups(1)
            .setRebalanceDelay(10_000)
            .setQueryEntities(Collections.singletonList(
                new QueryEntity(Integer.class.getName(), CacheValueObj2.class.getName())
                    .addQueryField("name", String.class.getName(), null)
                    .addQueryField("id", Long.class.getName(), null)
                    .setIndexes(Collections.singletonList(new QueryIndex()
                        .setFieldNames(Collections.singletonList("name"), true)))))
            .setAffinity(new RendezvousAffinityFunction(false, 64)
                .setAffinityBackupFilter(new DPLAffinityBackupFilter(1, 2)))
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
            .setUserAttributes(Collections.singletonMap(DATA_CENTER_IDENTITY, dcAttr))
            .setDataStorageConfiguration(memCfg)
            .setCacheConfiguration(ccfg, ccfg2)
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
        dcAttr = "a";
        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        dcAttr = "b";
        IgniteEx ignite2 = startGrid(2);
        IgniteEx ignite3 = startGrid(3);

        ignite0.cluster().active(true);

        try (IgniteDataStreamer streamer = ignite0.dataStreamer(cacheName)) {
            for (int i = 0; i < 1_000; i++)
                streamer.addData(i, new CacheValueObj("Val " + i));
        }

        try (IgniteDataStreamer streamer = ignite0.dataStreamer(cacheName2)) {
            for (int i = 0; i < 1_000; i++)
                streamer.addData(i, new CacheValueObj2("Name " + i));
        }

        assertEquals(2, ignite0.cacheNames().size());

        String colQry = "ALTER TABLE " + CacheValueObj.class.getSimpleName().toUpperCase() + " ADD COLUMN id BIGINT";

//        ignite0.context().query()
//            .querySqlFieldsNoCache(new SqlFieldsQuery(colQry).setSchema(cacheName), true).getAll();

        String idxQry = "CREATE INDEX idx ON " + CacheValueObj.class.getSimpleName().toUpperCase() + "(id)";

//        ignite0.context().query()
//            .querySqlFieldsNoCache(new SqlFieldsQuery(idxQry).setSchema(cacheName), true).getAll();

        IgniteCache<Integer, CacheValueObj> cache0 = ignite0.cache(cacheName);

        for (int i = 1_000; i < 2_000; i++)
            cache0.put(i, new CacheValueObj("Val " + i));

        IgniteCache<Integer, CacheValueObj2> cache2 = ignite0.cache(cacheName2);

        for (int i = 1_000; i < 2_000; i++)
            cache2.put(i, new CacheValueObj2("Name " + i));

        stopAllGrids();

        info("WHOLE GRID STOPPED");

        dcAttr = "a";
        ignite0 = startGrid(0);
        ignite1 = startGrid(1);

        dcAttr = "b";
        ignite2 = startGrid(2);
        ignite3 = startGrid(3);

        Thread.sleep(1_000);

        int[] partsNode1_before = ignite3.affinity(cacheName).allPartitions(ignite3.localNode());

        int local1_before = partsNode1_before.length;

        info("___ blt 1 ___");

        pribtWholePartitionMap();

        Thread.sleep(10_000);

        ignite2.close();

        ignite0.cluster().setBaselineTopology(ignite0.cluster().topologyVersion());

        waitForRebalancing();
        awaitPartitionMapExchange(true, true, null);

        info("___ blt 2 ___");

        pribtWholePartitionMap();

        Thread.sleep(10_000);

        int[] partsNode1_in = ignite3.affinity(cacheName).allPartitions(ignite3.localNode());

        int local1_in = partsNode1_in.length;

        /*assertTrue("Owning partition count on node1 should have increased. Before = "
            + local1_before + ", in = " + local1_in, local1_in > local1_before);*/

        dcAttr = "b";
        ignite2 = startGrid(2);

        new Timer().schedule(new TimerTask() {
            @Override public void run() {
                pribtWholePartitionMap();
            }
        }, 0, 60_000);
//        printPartitionState(ignite2);

        clientMode = true;

        IgniteEx client = startGrid(30);

        ignite0.cluster().setBaselineTopology(ignite0.cluster().topologyVersion());

        waitForRebalancing();
        awaitPartitionMapExchange(true, true, null);

        for (; ; ) {
            int[] partsNode1_after = ignite3.affinity(cacheName).allPartitions(ignite3.localNode());

            int local1_after = partsNode1_after.length; //group1.topology().localPartitions().size();

            Map<Thread, StackTraceElement[]> stacks = Thread.getAllStackTraces();

            if (local1_in > local1_after) {
                log.info("!!!! Node " + ignite3.localNode().consistentId());
                log.info("!!!! In = " + local1_in + ", after = " + local1_after + ", before = " + local1_before);
                log.info("!!!! Parts In = " + Arrays.toString(partsNode1_in) + "\n after = " + Arrays.toString(partsNode1_after) + "\n before = " + Arrays.toString(partsNode1_before));

                break;
            }
            else {
                log.warning("Owning partition count on node1 should have decreased. In = "
                    + local1_in + ", after = " + local1_after + ", before = " + local1_before);
            }

            Thread.sleep(1_000);
        }

        IgniteCache cache = client.cache(cacheName);
        IgniteCache cache22 = client.cache(cacheName2);

        for (int n = 0; n < 100; n++) {
            try (Transaction tx = client.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED)) {
                for (int i = 0; i < 10; i++) {
                    CacheValueObj val = (CacheValueObj)cache.get(i + n);

                    cache.put(i + n, new CacheValueObj("Val " + (val == null ? i : i + 1)));
                    cache22.put(i + n, new CacheValueObj("Name " + (val == null ? i : i + 1)));
                }

                tx.commit();
            }
        }

//        assertTrue("Owning partition count on node1 should have decreased. In = "
//            + local1_in + ", after = " + local1_after, local1_in > local1_after);
//        assertEquals(local1_before, local1_after);

    }

    private void pribtWholePartitionMap() {
        for (Ignite ign: G.allGrids()) {
            if (!ign.configuration().isClientMode())
                printPartitionState((IgniteEx)ign);
        }
    }

    private void printPartitionState(IgniteEx ign) {
        final Collection<CacheGroupContext> ctxs = ign.context().cache().cacheGroups();

        List<Integer> grpIds = new ArrayList<>();

        for (CacheGroupContext ctx : ctxs) {
            if (ctx.systemCache())
                continue;

            grpIds.add(ctx.groupId());
        }


        for (Integer id : grpIds) {
            final DynamicCacheDescriptor desc = ign.context().cache().cacheDescriptor(id);

            final CacheGroupContext grpCtx = ign.context().cache().cacheGroup(desc == null ? id : desc.groupId());

            String grpName = grpCtx.cacheOrGroupName();

            GridDhtPartitionTopologyImpl top = (GridDhtPartitionTopologyImpl)grpCtx.topology();

            List<GridDhtLocalPartition> locParts = top.localPartitions();

            StringBuilder sb = new StringBuilder()
                .append(":::: " + grpName + " " + " instance name " + ign.configuration().getIgniteInstanceName() + "\n");

            for (int i = 0; i < locParts.size(); i++) {
                GridDhtLocalPartition part = locParts.get(i);

                if (part == null)
                    continue;

                sb.append(part.id() + " : " + part.state() + "\n");
            }

            info(sb.toString());
        }

    }

    @Override protected long getPartitionMapExchangeTimeout() {
        return 300_000;
    }

    @Override protected long getTestTimeout() {
        return 600_000;
    }

    /**
     *
     */
    private static class CacheValueObj {
        /** Payload. */
        private byte[] payload = new byte[10 * 1024];
        /** Marker. */
        private String marker;

        private long id;

        /**
         * @param marker Marker.
         */
        public CacheValueObj(String marker) {
            this.marker = marker;
            this.id = marker.hashCode();
        }
    }

    /**
     *
     */
    private static class CacheValueObj2 {
        /** Payload. */
        private byte[] payload = new byte[10 * 1024];
        /** Marker. */
        private String name;

        private long id;

        /**
         * @param name Marker.
         */
        public CacheValueObj2(String name) {
            this.name = name;
            this.id = name.hashCode();
        }
    }

    /**
     */
    private static class DPLAffinityBackupFilter implements IgniteBiPredicate<ClusterNode, List<ClusterNode>> {
        /** Backups. */
        private final int backups;

        /** Data centers. */
        private final int dataCenters;

        /**
         * @param backups Backups.
         * @param dataCenters Data centers.
         */
        public DPLAffinityBackupFilter(int backups, int dataCenters) {
            this.backups = backups;
            this.dataCenters = dataCenters;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode testNode, List<ClusterNode> primaryAndBackups) {
            int targetCopiesCnt = backups + 1;

            if (primaryAndBackups.size() >= targetCopiesCnt)
                return false;

            Object selectedDC = testNode.attribute(DATA_CENTER_IDENTITY);

            int copiesInDC = 0;
            int maxAllowedForDC = targetCopiesCnt / dataCenters;

            for (ClusterNode clusterNode : primaryAndBackups) {
                Object nodeDC = clusterNode.attribute(DATA_CENTER_IDENTITY);

                if (nodeDC.equals(selectedDC)) {
                    copiesInDC++;

                    if (copiesInDC >= maxAllowedForDC)
                        break;
                }
            }

            return copiesInDC < maxAllowedForDC;
        }
    }
}
