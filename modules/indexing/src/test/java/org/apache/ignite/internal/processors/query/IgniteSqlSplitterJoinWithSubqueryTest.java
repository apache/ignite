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

package org.apache.ignite.internal.processors.query;

import java.sql.Timestamp;
import java.util.List;
import java.util.Random;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for correct distributed partitioned queries.
 */
@SuppressWarnings("unchecked")
public class IgniteSqlSplitterJoinWithSubqueryTest extends GridCommonAbstractTest {
    /** */
    private static final int CLIENT = 7;

    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(3, false);
        Ignition.setClientMode(true);
        try {
            startGrid(CLIENT);
        }
        finally {
            Ignition.setClientMode(false);
        }
    }

    /**
     * @param name Cache name.
     * @param partitioned Partition or replicated cache.
     * @param idxTypes Indexed types.
     * @return Cache configuration.
     */
    private static CacheConfiguration cacheConfig(String name, boolean partitioned, Class<?>... idxTypes) {
        return new CacheConfiguration(DEFAULT_CACHE_NAME)
            .setName(name)
            .setCacheMode(partitioned ? CacheMode.PARTITIONED : CacheMode.REPLICATED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setBackups(1)
            .setIndexedTypes(idxTypes);
    }

    /**
     *
     */
    public void testTwoJoinWithSubquery() {
        IgniteCache<Integer, Client> cli = ignite(0).createCache(
            cacheConfig("Client", true,
                Integer.class, Client.class));

        IgniteCache<Integer, Good> good = ignite(0).createCache(
            cacheConfig("Good", true,
                Integer.class, Good.class));

        IgniteCache<Integer, Order> order = ignite(0).createCache(
            cacheConfig("Order", true,
                Integer.class, Order.class));

        final int clients = 100;
        final int goods = 100;
        final int orders = 1000;

        for (int i = 0; i < clients; i++)
            cli.put( i, new Client(i));

        for (int i = 0; i < goods; i++)
            good.put( i, new Good(i));

        for (int i = 0; i < orders; i++)
            order.put( i, new Order(i, i % clients, (i * i) % orders));

        final List<List<?>> res = grid(0).context().query().querySqlFields(new SqlFieldsQuery(
            "SELECT cli.name, good.name \n" +
            "FROM (SELECT DISTINCT cliId, goodId FROM \"Order\".\"ORDER\") as ord \n" +
            "LEFT JOIN \"Client\".Client cli ON cli.id = ord.cliId \n" +
            "LEFT JOIN \"Good\".Good good ON good.id = ord.goodId"), false).getAll();
    }

    /** */
    public static class Client {
        /** */
        @QuerySqlField
        int id;

        /** */
        @QuerySqlField
        String name;

        /** */
        public Client(int id) {
            this.id = id;
            name = "Client " + id;
        }
    }

    /** */
    public static class Good {
        /** */
        @QuerySqlField
        int id;

        /** */
        @QuerySqlField
        String name;

        /** */
        public Good(int id) {
            this.id = id;
            name = "Client " + id;
        }
    }

    /** */
    public static class Order {
        /** */
        @QuerySqlField
        int id;

        /** */
        @QuerySqlField
        int cliId;

        /** */
        @QuerySqlField
        int goodId;

        /** */
        @QuerySqlField
        String comment;

        /** */
        @QuerySqlField
        Timestamp ts;

        /** */
        public Order(int id, int cliId, int goodId) {
            this.id = id;
            this.cliId = cliId;
            this.goodId = goodId;

            comment = "Comment " + id;
            ts = new Timestamp(new Timestamp(118, 0, 1, 12, 0, 0, 0).getTime() +
                id * 3600_1000);
        }
    }
}
