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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.PartitionSet;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jsr166.ThreadLocalRandom8;

import javax.cache.Cache;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.locks.LockSupport;

/**
 * Tests distributed queries over partition set.
 *
 * The test assigns partition ranges to specific grid nodes using special affinity implementation.
 */
public class IgniteCacheDistributedQueryPartitionSetSelfTest extends GridCommonAbstractTest {
    /** Segment attribute name. */
    private static final String SEGMENT_ATTR_NAME = "seg";

    /** Grids count. */
    private static final int GRIDS_COUNT = 10;

    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Clients per region distribution. */
    private static final int[] CLIENTS_PER_REGION = new int[] {1_000, 2_000, 3_000, 4_000, 240};

    /** Clients per partition. */
    private static final int CLIENTS_PER_PARTITION = 10;

    /** Total clients */
    private static final int TOTAL_CLIENTS;

    /** Partitions count. */
    public static final int PARTS_COUNT;

    static {
        int total = 0;
        int parts = 0;
        for (int regCnt : CLIENTS_PER_REGION) {
            total += regCnt;

            parts += regCnt / CLIENTS_PER_PARTITION;
        }

        TOTAL_CLIENTS = total - CLIENTS_PER_REGION[CLIENTS_PER_REGION.length-1]; // Last region is empty.
        PARTS_COUNT = parts;
    }

    /** Deposits per client. */
    public static final int DEPOSITS_PER_CLIENT = 10;

    /** Regions to partitions mapping. */
    public static final NavigableMap<Object, List<Integer>> REGION_TO_PART_MAP = new TreeMap<Object, List<Integer>>() {{
        int p = 0, regId = 1;

        for (int r : CLIENTS_PER_REGION) {
            int cnt = r / CLIENTS_PER_PARTITION;

            put(regId++, Arrays.asList(p, cnt));

            p += cnt;
        }
    }};

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);
        TcpDiscoverySpi spi = (TcpDiscoverySpi)cfg.getDiscoverySpi();
        spi.setIpFinder(IP_FINDER);

        /** Clients cache */
        CacheConfiguration<ClientKey, Client> clientCfg = new CacheConfiguration<>();
        clientCfg.setName("cl");
        clientCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        clientCfg.setBackups(0);
        clientCfg.setIndexedTypes(ClientKey.class, Client.class);

        /** Deposits cache */
        CacheConfiguration<DepositKey, Deposit> depoCfg = new CacheConfiguration<>();
        depoCfg.setName("de");
        depoCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        depoCfg.setBackups(0);
        depoCfg.setIndexedTypes(DepositKey.class, Deposit.class);

        /** Regions cache */
        CacheConfiguration<Integer, String> regionCfg = new CacheConfiguration<>();
        regionCfg.setName("re");
        regionCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        regionCfg.setCacheMode(CacheMode.REPLICATED);
        regionCfg.setIndexedTypes(Integer.class, Region.class);

        cfg.setCacheConfiguration(clientCfg, depoCfg, regionCfg);

        if ("client".equals(gridName))
            cfg.setClientMode(true);
        else {
            Integer reg = regionForGrid(gridName);

            cfg.setUserAttributes(F.asMap(SEGMENT_ATTR_NAME, reg));

            log().info("Assigned region " + reg + " to grid " + gridName);

            // TODO set topology validator.
        }

        return cfg;
    }

    /**
     * @param gridName Grid name.
     */
    private Integer regionForGrid(String gridName) {
        char c = gridName.charAt(gridName.length() - 1);
        switch (c) {
            case '0':
                return 0;
            case '1':
            case '2':
                return 1;
            case '3':
            case '4':
            case '5':
                return 2;
            default:
                return 3;
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

        // Fill caches.
        int clientId = 1;
        int depositId = 1;
        int regId = 1;
        int p = 1; // Percents counter. Log message will be printed 10 times.
        for (int cnt : CLIENTS_PER_REGION) {
            // Do not put clients into last region because it will not get any grid nodes assigned.
            if (regId < CLIENTS_PER_REGION.length) {
                for (int i = 0; i < cnt; i++) {
                    ClientKey ck = new ClientKey();
                    ck.clientId = clientId;
                    ck.regionId = regId;

                    Client cl = new Client();
                    cl.firstName = "First_Name_" + clientId;
                    cl.lastName = "Last_Name_" + clientId;
                    cl.passport = clientId * 1_000;

                    grid(0).cache("cl").put(ck, cl);

//                    for (int j = 0; j < DEPOSITS_PER_CLIENT; j++) {
//                        DepositKey dk = new DepositKey();
//                        dk.depositId = depositId++;
//                        dk.clientId = clientId;
//
//                        Deposit depo = new Deposit();
//                        depo.amount = ThreadLocalRandom8.current().nextLong(1_000_001);
//                        grid(0).cache("de").put(dk, depo);
//                    }

                    if (clientId / (float) TOTAL_CLIENTS >= p / 10f) {
                        log().info("Loaded " + clientId + " of " + TOTAL_CLIENTS);

                        p++;
                    }

                    clientId++;
                }
            }

            Region region = new Region();
            region.name = "Region_" + regId;
            region.code = regId * 10;

            grid(0).cache("re").put(regId, region);

            regId++;
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** Basic test. */
    public void testBasicQuery() {
        IgniteCache<Object, Object> cl = grid(0).cache("cl");
//        assertEquals("Clients count", TOTAL_CLIENTS, cl.size());
//
        IgniteCache<Object, Object> de = grid(0).cache("de");
//        assertEquals("Deposits count", TOTAL_CLIENTS * DEPOSITS_PER_CLIENT, de.size());
//
        IgniteCache<Object, Object> re = grid(0).cache("re");
//        assertEquals("Regions count", CLIENTS_PER_REGION.length, re.size());

        int regId = 2;

        SqlQuery<ClientKey, Client> qry1 = new SqlQuery<>(Client.class, "regionId=?");
        qry1.setArgs(regId);

        List<Cache.Entry<ClientKey, Client>> clients1 = cl.query(qry1).getAll();

        assertEquals("Region count", CLIENTS_PER_REGION[regId - 1], clients1.size());

        for (Cache.Entry<ClientKey, Client> entry : clients1) {
            List<Integer> range = REGION_TO_PART_MAP.get(regId);

            int start = range.get(0) * CLIENTS_PER_PARTITION;
            int end = start + range.get(1) * CLIENTS_PER_PARTITION;

            int clientId = entry.getKey().clientId;

            assertTrue("Client id in range", start < clientId && start <= end);
        }

        // Limit partitions.
        List<Integer> range = REGION_TO_PART_MAP.get(regId);

        SqlQuery<ClientKey, Client> qry2 = new SqlQuery<>(Client.class, "1=1");
        qry2.setPartitionSet(new PartitionSet(range.get(0), range.get(1)));

        List<Cache.Entry<ClientKey, Client>> clients2 = cl.query(qry2).getAll();

        assertEquals("Region count", CLIENTS_PER_REGION[regId - 1], clients2.size());
    }

    /** */
    private static class ClientKey extends RegionKey {
        /** Client id. */
        @QuerySqlField(index = true)
        protected int clientId;

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ClientKey clientKey = (ClientKey) o;

            return clientId == clientKey.clientId;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return clientId;
        }
    }

    /** */
    private static class DepositKey extends RegionKey {
        @QuerySqlField(index = true)
        protected int depositId;

        /** Client id. */
        @AffinityKeyMapped
        @QuerySqlField(index = true)
        protected int clientId;

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            DepositKey that = (DepositKey) o;

            return depositId == that.depositId;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return depositId;
        }
    }

    /** */
    private static class RegionKey implements Serializable {
        /** Region id. */
        @QuerySqlField
        protected int regionId;
    }

    /** */
    private static class Client {
        @QuerySqlField
        protected String firstName;

        @QuerySqlField
        protected String lastName;

        @QuerySqlField
        protected int passport;
    }

    /** */
    private static class Deposit {
        @QuerySqlField
        protected long amount;
    }

    /** */
    private static class Region {
        @QuerySqlField
        protected String name;

        @QuerySqlField
        protected int code;
    }
}