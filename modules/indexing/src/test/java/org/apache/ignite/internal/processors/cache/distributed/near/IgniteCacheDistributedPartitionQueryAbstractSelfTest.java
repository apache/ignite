/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.Cache;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.GridRandom;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.util.AttributeNodeFilter;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Abstract test for queries over explicit partitions.
 */
public abstract class IgniteCacheDistributedPartitionQueryAbstractSelfTest extends GridCommonAbstractTest {
    /** Join query for test. */
    private static final String JOIN_QRY = "select cl._KEY, de.depositId, de.regionId from " +
        "\"cl\".Client cl, \"de\".Deposit de, \"re\".Region re where cl.clientId=de.clientId and de.regionId=re._KEY";

    /** Region node attribute name. */
    private static final String REGION_ATTR_NAME = "reg";

    /** Grids count. */
    protected static final int GRIDS_COUNT = 10;

    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Partitions per region distribution. */
    protected static final int[] PARTS_PER_REGION = new int[] {10, 20, 30, 40, 24};

    /** Unmapped region id. */
    protected static final int UNMAPPED_REGION = PARTS_PER_REGION.length;

    /** Clients per partition. */
    protected static final int CLIENTS_PER_PARTITION = 1;

    /** Total clients. */
    private static final int TOTAL_CLIENTS;

    /** Affinity function to use on partitioned caches. */
    private static final AffinityFunction AFFINITY = new RegionAwareAffinityFunction();

    /** Partitions count. */
    private static final int PARTS_COUNT;

    /** Regions to partitions mapping. */
    protected static final NavigableMap<Integer, List<Integer>> REGION_TO_PART_MAP = new TreeMap<>();

    /** Query threads count. */
    protected static final int QUERY_THREADS_CNT = 4;

    /** Restarting threads count. */
    protected static final int RESTART_THREADS_CNT = 2;

    /** Node stop time. */
    protected static final int NODE_RESTART_TIME = 1_000;

    static {
        int total = 0, parts = 0, p = 0, regionId = 1;

        for (int regCnt : PARTS_PER_REGION) {
            total += regCnt * CLIENTS_PER_PARTITION;

            parts += regCnt;

            REGION_TO_PART_MAP.put(regionId++, Arrays.asList(p, regCnt));

            p += regCnt;
        }

        /** Last region was left empty intentionally, see {@link #UNMAPPED_REGION} */
        TOTAL_CLIENTS = total - PARTS_PER_REGION[PARTS_PER_REGION.length - 1] * CLIENTS_PER_PARTITION;

        PARTS_COUNT = parts;
    }

    /** Deposits per client. */
    public static final int DEPOSITS_PER_CLIENT = 10;

    /** Rnd. */
    protected GridRandom rnd = new GridRandom();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setMaxSize(20L * 1024 * 1024));

        cfg.setDataStorageConfiguration(memCfg);

        TcpDiscoverySpi spi = (TcpDiscoverySpi)cfg.getDiscoverySpi();
        spi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(spi);

        /** Clients cache */
        CacheConfiguration<ClientKey, Client> clientCfg = new CacheConfiguration<>();
        clientCfg.setName("cl");
        clientCfg.setWriteSynchronizationMode(FULL_SYNC);
        clientCfg.setAtomicityMode(TRANSACTIONAL);
        clientCfg.setRebalanceMode(SYNC);
        clientCfg.setBackups(2);
        clientCfg.setAffinity(AFFINITY);
        clientCfg.setIndexedTypes(ClientKey.class, Client.class);

        /** Deposits cache */
        CacheConfiguration<DepositKey, Deposit> depoCfg = new CacheConfiguration<>();
        depoCfg.setName("de");
        depoCfg.setWriteSynchronizationMode(FULL_SYNC);
        depoCfg.setAtomicityMode(TRANSACTIONAL);
        depoCfg.setRebalanceMode(SYNC);
        depoCfg.setBackups(2);
        depoCfg.setAffinity(AFFINITY);
        depoCfg.setIndexedTypes(DepositKey.class, Deposit.class);

        /** Regions cache. Uses default affinity. */
        CacheConfiguration<Integer, Region> regionCfg = new CacheConfiguration<>();
        regionCfg.setName("re");
        regionCfg.setWriteSynchronizationMode(FULL_SYNC);
        regionCfg.setAtomicityMode(TRANSACTIONAL);
        regionCfg.setRebalanceMode(SYNC);
        regionCfg.setCacheMode(CacheMode.REPLICATED);
        regionCfg.setIndexedTypes(Integer.class, Region.class);

        cfg.setCacheConfiguration(clientCfg, depoCfg, regionCfg);

        if ("client".equals(gridName))
            cfg.setClientMode(true);
        else {
            Integer reg = regionForGrid(gridName);

            cfg.setUserAttributes(F.asMap(REGION_ATTR_NAME, reg));

            log().info("Assigned region " + reg + " to grid " + gridName);
        }

        return cfg;
    }

    /** */
    private static final class RegionAwareAffinityFunction implements AffinityFunction {
        /** {@inheritDoc} */
        @Override public void reset() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public int partitions() {
            return PARTS_COUNT;
        }

        /** {@inheritDoc} */
        @Override public int partition(Object key) {
            Integer regionId;

            if (key instanceof RegionKey)
                regionId = ((RegionKey)key).regionId;
            else if (key instanceof BinaryObject) {
                BinaryObject bo = (BinaryObject)key;

                regionId = bo.field("regionId");
            }
            else
                throw new IgniteException("Unsupported key for region aware affinity");

            List<Integer> range = REGION_TO_PART_MAP.get(regionId);

            Integer cnt = range.get(1);

            return U.safeAbs(key.hashCode() % cnt) + range.get(0); // Assign partition in region's range.
        }

        /** {@inheritDoc} */
        @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
            List<ClusterNode> nodes = affCtx.currentTopologySnapshot();

            List<List<ClusterNode>> assignment = new ArrayList<>(PARTS_COUNT);

            for (int p = 0; p < PARTS_COUNT; p++) {
                // Get region for partition.
                int regionId = regionForPart(p);

                // Filter all nodes for region.
                AttributeNodeFilter f = new AttributeNodeFilter(REGION_ATTR_NAME, regionId);

                List<ClusterNode> regionNodes = new ArrayList<>();

                for (ClusterNode node : nodes)
                    if (f.apply(node))
                        regionNodes.add(node);

                final int cp = p;

                Collections.sort(regionNodes, new Comparator<ClusterNode>() {
                    @Override public int compare(ClusterNode o1, ClusterNode o2) {
                        return Long.compare(hash(cp, o1), hash(cp, o2));
                    }
                });

                assignment.add(regionNodes);
            }

            return assignment;
        }

        /** {@inheritDoc} */
        @Override public void removeNode(UUID nodeId) {
            // No-op.
        }

        /**
         * @param part Partition.
         */
        protected int regionForPart(int part) {
            for (Map.Entry<Integer, List<Integer>> entry : REGION_TO_PART_MAP.entrySet()) {
                List<Integer> range = entry.getValue();

                if (range.get(0) <= part && part < range.get(0) + range.get(1))
                    return entry.getKey();
            }

            throw new IgniteException("Failed to find zone for partition");
        }

        /**
         * @param part Partition.
         * @param obj Object.
         */
        private long hash(int part, Object obj) {
            long x = ((long)part << 32) | obj.hashCode();
            x ^= x >>> 12;
            x ^= x << 25;
            x ^= x >>> 27;
            return x * 2685821657736338717L;
        }
    }

    /**
     * Assigns a region to grid part.
     *
     * @param gridName Grid name.
     */
    protected Integer regionForGrid(String gridName) {
        char c = gridName.charAt(gridName.length() - 1);
        switch (c) {
            case '0':
                return 1;
            case '1':
            case '2':
                return 2;
            case '3':
            case '4':
            case '5':
                return 3;
            default:
                return 4;
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        int sum1 = 0;
        for (List<Integer> range : REGION_TO_PART_MAP.values())
            sum1 += range.get(1);

        assertEquals("Illegal partition per region distribution", PARTS_COUNT, sum1);

        startGridsMultiThreaded(GRIDS_COUNT);

        startGrid("client");

        // Fill caches.
        int clientId = 1;
        int depositId = 1;
        int regionId = 1;
        int p = 1; // Percents counter. Log message will be printed 10 times.

        try (IgniteDataStreamer<ClientKey, Client> clStr = grid(0).dataStreamer("cl");
             IgniteDataStreamer<DepositKey, Deposit> depStr = grid(0).dataStreamer("de")) {
            for (int cnt : PARTS_PER_REGION) {
                // Last region was left empty intentionally.
                if (regionId < PARTS_PER_REGION.length) {
                    for (int i = 0; i < cnt * CLIENTS_PER_PARTITION; i++) {
                        ClientKey ck = new ClientKey(clientId, regionId);

                        Client cl = new Client();
                        cl.firstName = "First_Name_" + clientId;
                        cl.lastName = "Last_Name_" + clientId;
                        cl.passport = clientId * 1_000;

                        clStr.addData(ck, cl);

                        for (int j = 0; j < DEPOSITS_PER_CLIENT; j++) {
                            DepositKey dk = new DepositKey(depositId++, new ClientKey(clientId, regionId));

                            Deposit depo = new Deposit();
                            depo.amount = ThreadLocalRandom.current().nextLong(1_000_001);
                            depStr.addData(dk, depo);
                        }

                        if (clientId / (float)TOTAL_CLIENTS >= p / 10f) {
                            log().info("Loaded " + clientId + " of " + TOTAL_CLIENTS);

                            p++;
                        }

                        clientId++;
                    }
                }

                Region region = new Region();
                region.name = "Region_" + regionId;
                region.code = regionId * 10;

                grid(0).cache("re").put(regionId, region);

                regionId++;
            }
        }
    }

    /**
     * @param orig Originator.
     */
    protected void doTestRegionQuery(Ignite orig) {
        IgniteCache<ClientKey, Client> cl = orig.cache("cl");

        for (int regionId = 1; regionId <= PARTS_PER_REGION.length; regionId++) {
            SqlQuery<ClientKey, Client> qry1 = new SqlQuery<>(Client.class, "regionId=?");
            qry1.setArgs(regionId);

            List<Cache.Entry<ClientKey, Client>> clients1 = cl.query(qry1).getAll();

            int expRegionCnt = regionId == 5 ? 0 : PARTS_PER_REGION[regionId - 1] * CLIENTS_PER_PARTITION;

            assertEquals("Region " + regionId + " count", expRegionCnt, clients1.size());

            validateClients(regionId, clients1);

            // Repeat the same query with partition set condition.
            List<Integer> range = REGION_TO_PART_MAP.get(regionId);

            SqlQuery<ClientKey, Client> qry2 = new SqlQuery<>(Client.class, "1=1");
            qry2.setPartitions(createRange(range.get(0), range.get(1)));

            try {
                List<Cache.Entry<ClientKey, Client>> clients2 = cl.query(qry2).getAll();

                assertEquals("Region " + regionId + " count with partition set", expRegionCnt, clients2.size());

                // Query must produce only results from single region.
                validateClients(regionId, clients2);

                if (regionId == UNMAPPED_REGION)
                    fail();
            } catch (CacheException ignored) {
                if (regionId != UNMAPPED_REGION)
                    fail();
            }
        }
    }

    /** */
    protected int[] createRange(int start, int cnt) {
        int[] vals = new int[cnt];

        for (int i = 0; i < cnt; i++)
            vals[i] = start + i;

        return vals;
    }

    /**
     * @param orig Originator.
     */
    protected void doTestPartitionsQuery(Ignite orig) {
        IgniteCache<ClientKey, Client> cl = orig.cache("cl");

        for (int regionId = 1; regionId <= PARTS_PER_REGION.length; regionId++) {
            log().info("Running test queries for region " + regionId);

            List<Integer> range = REGION_TO_PART_MAP.get(regionId);

            int[] parts = createRange(range.get(0), range.get(1));

            int off = rnd.nextInt(parts.length);

            int p1 = parts[off], p2 = parts[(off + (1 + rnd.nextInt(parts.length-1))) % parts.length];

            log().info("Parts: " + p1 + " " + p2);

            SqlQuery<ClientKey, Client> qry = new SqlQuery<>(Client.class, "1=1");

            qry.setPartitions(p1, p2);

            try {
                List<Cache.Entry<ClientKey, Client>> clients = cl.query(qry).getAll();

                // Query must produce only results from two partitions.
                for (Cache.Entry<ClientKey, Client> client : clients) {
                    int p = orig.affinity("cl").partition(client.getKey());

                    assertTrue("Incorrect partition for key", p == p1 || p == p2);
                }

                if (regionId == UNMAPPED_REGION)
                    fail();
            } catch (CacheException ignored) {
                if (regionId != UNMAPPED_REGION)
                    fail();
            }
        }
    }

    /**
     * @param orig Query originator.
     * @param regionIds Region ids.
     */
    protected void doTestJoinQuery(Ignite orig, int... regionIds) {
        IgniteCache<ClientKey, Client> cl = orig.cache("cl");

        if (regionIds == null) {
            regionIds = new int[PARTS_PER_REGION.length];

            for (int i = 0; i < regionIds.length; i++)
                regionIds[i] = i + 1;
        }

        for (int regionId : regionIds) {
            List<Integer> range = REGION_TO_PART_MAP.get(regionId);

            SqlFieldsQuery qry = new SqlFieldsQuery(JOIN_QRY);

            int[] pSet = createRange(range.get(0), 1 + rnd.nextInt(range.get(1) - 1));

            qry.setPartitions(pSet);

            try {
                List<List<?>> rows = cl.query(qry).getAll();

                for (List<?> row : rows) {
                    ClientKey key = (ClientKey)row.get(0);

                    int p = orig.affinity("cl").partition(key);

                    assertTrue(Arrays.binarySearch(pSet, p) >= 0);
                }

                // Query must produce only results from single region.
                for (List<?> row : rows)
                    assertEquals("Region id", regionId, ((Integer)row.get(2)).intValue());

                if (regionId == UNMAPPED_REGION)
                    fail();
            }
            catch (CacheException e) {
                if (X.hasCause(e, InterruptedException.class, IgniteInterruptedCheckedException.class))
                    return; // Allow interruptions.

                if (regionId != UNMAPPED_REGION) {
                    e.printStackTrace(System.err);

                    fail("Unexpected exception (see details above): " + e.getMessage());
                }
            }
        }
    }

    /**
     * @param regionId Region id.
     * @param clients Clients.
     */
    protected void validateClients(int regionId, List<Cache.Entry<ClientKey, Client>> clients) {
        for (Cache.Entry<ClientKey, Client> entry : clients) {
            List<Integer> range = REGION_TO_PART_MAP.get(regionId);

            int start = range.get(0) * CLIENTS_PER_PARTITION;
            int end = start + range.get(1) * CLIENTS_PER_PARTITION;

            int clientId = entry.getKey().clientId;

            assertTrue("Client id in range", start < clientId && start <= end);
        }
    }

    /** */
    protected static class ClientKey extends RegionKey {
        /** Client id. */
        @QuerySqlField(index = true)
        protected int clientId;

        /**
         * @param clientId Client id.
         * @param regionId Region id.
         */
        public ClientKey(int clientId, int regionId) {
            this.clientId = clientId;
            this.regionId = regionId;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            ClientKey clientKey = (ClientKey)o;

            return clientId == clientKey.clientId;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return clientId;
        }
    }

    /** */
    protected static class DepositKey extends RegionKey {
        @QuerySqlField(index = true)
        protected int depositId;

        @QuerySqlField(index = true)
        protected int clientId;

        /** Client id. */
        @AffinityKeyMapped
        protected ClientKey clientKey;

        /**
         * @param depositId Client id.
         * @param clientKey Client key.
         */
        public DepositKey(int depositId, ClientKey clientKey) {
            this.depositId = depositId;
            this.clientId = clientKey.clientId;
            this.regionId = clientKey.regionId;
            this.clientKey = clientKey;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            DepositKey that = (DepositKey)o;

            return depositId == that.depositId;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return depositId;
        }
    }

    /** */
    protected static class RegionKey implements Serializable {
        /** Region id. */
        @QuerySqlField(index = true)
        protected int regionId;
    }

    /** */
    protected static class Client {
        @QuerySqlField
        protected String firstName;

        @QuerySqlField
        protected String lastName;

        @QuerySqlField(index = true)
        protected int passport;
    }

    /** */
    protected static class Deposit {
        @QuerySqlField
        protected long amount;
    }

    /** */
    protected static class Region {
        @QuerySqlField
        protected String name;

        @QuerySqlField
        protected int code;
    }
}