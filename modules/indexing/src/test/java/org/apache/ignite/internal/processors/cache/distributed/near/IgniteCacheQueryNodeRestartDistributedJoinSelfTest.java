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
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.GridRandom;
import org.apache.ignite.internal.util.typedef.CAX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Test for distributed queries with node restarts.
 */
public class IgniteCacheQueryNodeRestartDistributedJoinSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String QRY_0 = "select co._key, count(*) cnt\n" +
        "from \"pe\".Person pe, \"pr\".Product pr, \"co\".Company co, \"pu\".Purchase pu\n" +
        "where pe._key = pu.personId and pu.productId = pr._key and pr.companyId = co._key \n" +
        "group by co._key order by cnt desc, co._key";

    /** */
    private static final String QRY_0_BROADCAST = "select co._key, count(*) cnt\n" +
        "from \"co\".Company co, \"pr\".Product pr, \"pu\".Purchase pu, \"pe\".Person pe \n" +
        "where pe._key = pu.personId and pu.productId = pr._key and pr.companyId = co._key \n" +
        "group by co._key order by cnt desc, co._key";

    /** */
    private static final String QRY_1 = "select pr._key, co._key\n" +
        "from \"pr\".Product pr, \"co\".Company co\n" +
        "where pr.companyId = co._key\n" +
        "order by co._key, pr._key ";

    /** */
    private static final String QRY_1_BROADCAST = "select pr._key, co._key\n" +
        "from \"co\".Company co, \"pr\".Product pr \n" +
        "where pr.companyId = co._key\n" +
        "order by co._key, pr._key ";

    /** */
    private static final int GRID_CNT = 6;

    /** */
    private static final int PERS_CNT = 600;

    /** */
    private static final int PURCHASE_CNT = 6000;

    /** */
    private static final int COMPANY_CNT = 25;

    /** */
    private static final int PRODUCT_CNT = 100;

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        int i = 0;

        CacheConfiguration<?, ?>[] ccs = new CacheConfiguration[4];

        for (String name : F.asList("pe", "pu", "co", "pr")) {
            CacheConfiguration<?, ?> cc = defaultCacheConfiguration();

            cc.setName(name);
            cc.setCacheMode(PARTITIONED);
            cc.setBackups(2);
            cc.setWriteSynchronizationMode(FULL_SYNC);
            cc.setAtomicityMode(TRANSACTIONAL);
            cc.setRebalanceMode(SYNC);
            cc.setLongQueryWarningTimeout(15_000);
            cc.setAffinity(new RendezvousAffinityFunction(false, 60));

            switch (name) {
                case "pe":
                    cc.setIndexedTypes(
                        Integer.class, Person.class
                    );

                    break;

                case "pu":
                    cc.setIndexedTypes(
                        Integer.class, Purchase.class
                    );

                    break;

                case "co":
                    cc.setIndexedTypes(
                        Integer.class, Company.class
                    );

                    break;

                case "pr":
                    cc.setIndexedTypes(
                        Integer.class, Product.class
                    );

                    break;
            }

            ccs[i++] = cc;
        }

        c.setCacheConfiguration(ccs);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(GRID_CNT);

        fillCaches();
    }

    /**
     *
     */
    private void fillCaches() {
        IgniteCache<Integer, Company> co = grid(0).cache("co");

        for (int i = 0; i < COMPANY_CNT; i++)
            co.put(i, new Company(i));

        IgniteCache<Integer, Product> pr = grid(0).cache("pr");

        Random rnd = new GridRandom();

        for (int i = 0; i < PRODUCT_CNT; i++)
            pr.put(i, new Product(i, rnd.nextInt(COMPANY_CNT)));

        IgniteCache<Integer, Person> pe = grid(0).cache("pe");

        for (int i = 0; i < PERS_CNT; i++)
            pe.put(i, new Person(i));

        IgniteCache<Integer, Purchase> pu = grid(0).cache("pu");

        for (int i = 0; i < PURCHASE_CNT; i++) {
            int persId = rnd.nextInt(PERS_CNT);
            int prodId = rnd.nextInt(PRODUCT_CNT);

            pu.put(i, new Purchase(persId, prodId));
        }
    }
    /**
     * @throws Exception If failed.
     */
    public void testRestarts() throws Exception {
        restarts(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRestartsBroadcast() throws Exception {
        restarts(true);
    }

    /**
     * @param broadcastQry If {@code true} tests broadcast query.
     * @throws Exception If failed.
     */
    private void restarts(final boolean broadcastQry) throws Exception {
        int duration = 90 * 1000;
        int qryThreadNum = 4;
        int restartThreadsNum = 2; // 4 + 2 = 6 nodes
        final int nodeLifeTime = 4000;
        final int logFreq = 100;

        final AtomicIntegerArray locks = new AtomicIntegerArray(GRID_CNT);

        SqlFieldsQuery qry0 ;

        if (broadcastQry)
            qry0 = new SqlFieldsQuery(QRY_0_BROADCAST).setDistributedJoins(true).setEnforceJoinOrder(true);
        else
            qry0 = new SqlFieldsQuery(QRY_0).setDistributedJoins(true);

        String plan = queryPlan(grid(0).cache("pu"), qry0);

        X.println("Plan1: " + plan);

        assertEquals(broadcastQry, plan.contains("batched:broadcast"));

        final List<List<?>> pRes = grid(0).cache("pu").query(qry0).getAll();

        Thread.sleep(3000);

        assertEquals(pRes, grid(0).cache("pu").query(qry0).getAll());

        final SqlFieldsQuery qry1;

        if (broadcastQry)
            qry1 = new SqlFieldsQuery(QRY_1_BROADCAST).setDistributedJoins(true).setEnforceJoinOrder(true);
        else
            qry1 = new SqlFieldsQuery(QRY_1).setDistributedJoins(true);

        plan = queryPlan(grid(0).cache("co"), qry1);

        X.println("Plan2: " + plan);

        assertEquals(broadcastQry, plan.contains("batched:broadcast"));

        final List<List<?>> rRes = grid(0).cache("co").query(qry1).getAll();

        assertFalse(pRes.isEmpty());
        assertFalse(rRes.isEmpty());

        final AtomicInteger qryCnt = new AtomicInteger();
        final AtomicBoolean qrysDone = new AtomicBoolean();

        IgniteInternalFuture<?> fut1 = multithreadedAsync(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                GridRandom rnd = new GridRandom();

                while (!qrysDone.get()) {
                    int g;

                    do {
                        g = rnd.nextInt(locks.length());
                    }
                    while (!locks.compareAndSet(g, 0, 1));

                    if (rnd.nextBoolean()) {
                        IgniteCache<?, ?> cache = grid(g).cache("pu");

                        SqlFieldsQuery qry;

                        if (broadcastQry)
                            qry = new SqlFieldsQuery(QRY_0_BROADCAST).setDistributedJoins(true).setEnforceJoinOrder(true);
                        else
                            qry = new SqlFieldsQuery(QRY_0).setDistributedJoins(true);

                        boolean smallPageSize = rnd.nextBoolean();

                        qry.setPageSize(smallPageSize ? 30 : 1000);

                        try {
                            assertEquals(pRes, cache.query(qry).getAll());
                        }
                        catch (CacheException e) {
                            assertTrue("On large page size must retry.", smallPageSize);

                            boolean failedOnRemoteFetch = false;

                            for (Throwable th = e; th != null; th = th.getCause()) {
                                if (!(th instanceof CacheException))
                                    continue;

                                if (th.getMessage() != null &&
                                    th.getMessage().startsWith("Failed to fetch data from node:")) {
                                    failedOnRemoteFetch = true;

                                    break;
                                }
                            }

                            if (!failedOnRemoteFetch) {
                                e.printStackTrace();

                                fail("Must fail inside of GridResultPage.fetchNextPage or subclass.");
                            }
                        }
                    }
                    else {
                        IgniteCache<?, ?> cache = grid(g).cache("co");

                        SqlFieldsQuery qry;

                        if (broadcastQry)
                            qry = new SqlFieldsQuery(QRY_1_BROADCAST).setDistributedJoins(true).setEnforceJoinOrder(true);
                        else
                            qry = new SqlFieldsQuery(QRY_1).setDistributedJoins(true);

                        assertEquals(rRes, cache.query(qry1).getAll());
                    }

                    locks.set(g, 0);

                    int c = qryCnt.incrementAndGet();

                    if (c % logFreq == 0)
                        info("Executed queries: " + c);
                }
            }
        }, qryThreadNum, "query-thread");

        final AtomicInteger restartCnt = new AtomicInteger();

        final AtomicBoolean restartsDone = new AtomicBoolean();

        IgniteInternalFuture<?> fut2 = multithreadedAsync(new Callable<Object>() {
            @SuppressWarnings({"BusyWait"})
            @Override public Object call() throws Exception {
                GridRandom rnd = new GridRandom();

                while (!restartsDone.get()) {
                    int g;

                    do {
                        g = rnd.nextInt(locks.length());
                    }
                    while (!locks.compareAndSet(g, 0, -1));

                    log.info("Stop node: " + g);

                    stopGrid(g);

                    Thread.sleep(rnd.nextInt(nodeLifeTime));

                    log.info("Start node: " + g);

                    startGrid(g);

                    Thread.sleep(rnd.nextInt(nodeLifeTime));

                    locks.set(g, 0);

                    int c = restartCnt.incrementAndGet();

                    if (c % logFreq == 0)
                        info("Node restarts: " + c);
                }

                return true;
            }
        }, restartThreadsNum, "restart-thread");

        Thread.sleep(duration);

        info("Stopping..");

        restartsDone.set(true);
        qrysDone.set(true);

        fut2.get();
        fut1.get();

        info("Stopped.");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     *
     */
    private static class Person implements Serializable {
        /** */
        @QuerySqlField(index = true)
        int id;

        /**
         * @param id ID.
         */
        Person(int id) {
            this.id = id;
        }
    }

    /**
     *
     */
    private static class Purchase implements Serializable {
        /** */
        @QuerySqlField(index = true)
        int personId;

        /** */
        @QuerySqlField(index = true)
        int productId;

        /**
         * @param personId Person ID.
         * @param productId Product ID.
         */
        Purchase(int personId, int productId) {
            this.personId = personId;
            this.productId = productId;
        }
    }

    /**
     *
     */
    private static class Company implements Serializable {
        /** */
        @QuerySqlField(index = true)
        int id;

        /**
         * @param id ID.
         */
        Company(int id) {
            this.id = id;
        }
    }

    /**
     *
     */
    private static class Product implements Serializable {
        /** */
        @QuerySqlField(index = true)
        int id;

        /** */
        @QuerySqlField(index = true)
        int companyId;

        /**
         * @param id ID.
         * @param companyId Company ID.
         */
        Product(int id, int companyId) {
            this.id = id;
            this.companyId = companyId;
        }
    }
}