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
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.internal.util.GridRandom;
import org.apache.ignite.internal.util.typedef.CAX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Test for distributed queries with node restarts.
 */
public class IgniteCacheQueryNodeRestartSelfTest2 extends GridCommonAbstractTest {
    /** */
    private static final String PARTITIONED_QRY = "select co.id, count(*) cnt\n" +
        "from \"pe\".Person pe, \"pr\".Product pr, \"co\".Company co, \"pu\".Purchase pu\n" +
        "where pe.id = pu.personId and pu.productId = pr.id and pr.companyId = co.id \n" +
        "group by co.id order by cnt desc, co.id";

    /** */
    private static final String REPLICATED_QRY = "select pr.id, co.id\n" +
        "from \"pr\".Product pr, \"co\".Company co\n" +
        "where pr.companyId = co.id\n" +
        "order by co.id, pr.id ";

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

        for (String name : F.asList("pe", "pu")) {
            CacheConfiguration<?, ?> cc = defaultCacheConfiguration();

            cc.setName(name);
            cc.setCacheMode(PARTITIONED);
            cc.setBackups(2);
            cc.setWriteSynchronizationMode(FULL_SYNC);
            cc.setAtomicityMode(TRANSACTIONAL);
            cc.setRebalanceMode(SYNC);
            cc.setAffinity(new RendezvousAffinityFunction(false, 60));

            if (name.equals("pe")) {
                cc.setIndexedTypes(
                    Integer.class, Person.class
                );
            }
            else if (name.equals("pu")) {
                cc.setIndexedTypes(
                    AffinityKey.class, Purchase.class
                );
            }

            ccs[i++] = cc;
        }

        for (String name : F.asList("co", "pr")) {
            CacheConfiguration<?, ?> cc = defaultCacheConfiguration();

            cc.setName(name);
            cc.setCacheMode(REPLICATED);
            cc.setWriteSynchronizationMode(FULL_SYNC);
            cc.setAtomicityMode(TRANSACTIONAL);
            cc.setRebalanceMode(SYNC);
            cc.setAffinity(new RendezvousAffinityFunction(false, 50));

            if (name.equals("co")) {
                cc.setIndexedTypes(
                    Integer.class, Company.class
                );
            }
            else if (name.equals("pr")) {
                cc.setIndexedTypes(
                    Integer.class, Product.class
                );
            }

            ccs[i++] = cc;
        }

        c.setCacheConfiguration(ccs);

        return c;
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

        IgniteCache<AffinityKey<Integer>, Purchase> pu = grid(0).cache("pu");

        for (int i = 0; i < PURCHASE_CNT; i++) {
            int persId = rnd.nextInt(PERS_CNT);
            int prodId = rnd.nextInt(PRODUCT_CNT);

            pu.put(new AffinityKey<>(i, persId), new Purchase(persId, prodId));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRestarts() throws Exception {
        int duration = 90 * 1000;
        int qryThreadNum = 4;
        int restartThreadsNum = 2; // 4 + 2 = 6 nodes
        final int nodeLifeTime = 2 * 1000;
        final int logFreq = 10;

        startGridsMultiThreaded(GRID_CNT);

        final AtomicIntegerArray locks = new AtomicIntegerArray(GRID_CNT);

        fillCaches();

        final List<List<?>> pRes = grid(0).cache("pu").query(new SqlFieldsQuery(PARTITIONED_QRY)).getAll();

        Thread.sleep(3000);

        assertEquals(pRes, grid(0).cache("pu").query(new SqlFieldsQuery(PARTITIONED_QRY)).getAll());

        final List<List<?>> rRes = grid(0).cache("co").query(new SqlFieldsQuery(REPLICATED_QRY)).getAll();

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

                    try {
                        if (rnd.nextBoolean()) { // Partitioned query.
                            IgniteCache<?,?> cache = grid(g).cache("pu");

                            SqlFieldsQuery qry = new SqlFieldsQuery(PARTITIONED_QRY);

                            boolean smallPageSize = rnd.nextBoolean();

                            if (smallPageSize)
                                qry.setPageSize(3);

                            try {
                                assertEquals(pRes, cache.query(qry).getAll());
                            } catch (CacheException e) {
                                // Interruptions are expected here.
                                if (e.getCause() instanceof IgniteInterruptedCheckedException)
                                    continue;

                                if (e.getCause() instanceof QueryCancelledException)
                                    fail("Retry is expected");

                                if (!smallPageSize)
                                    e.printStackTrace();

                                assertTrue("On large page size must retry.", smallPageSize);

                                boolean failedOnRemoteFetch = false;
                                boolean failedOnInterruption = false;

                                for (Throwable th = e; th != null; th = th.getCause()) {
                                    if (th instanceof InterruptedException) {
                                        failedOnInterruption = true;

                                        break;
                                    }

                                    if (!(th instanceof CacheException))
                                        continue;

                                    if (th.getMessage() != null &&
                                        th.getMessage().startsWith("Failed to fetch data from node:")) {
                                        failedOnRemoteFetch = true;

                                        break;
                                    }
                                }

                                // Interruptions are expected here.
                                if (failedOnInterruption)
                                    continue;

                                if (!failedOnRemoteFetch) {
                                    e.printStackTrace();

                                    fail("Must fail inside of GridResultPage.fetchNextPage or subclass.");
                                }
                            }
                        } else { // Replicated query.
                            IgniteCache<?, ?> cache = grid(g).cache("co");

                            assertEquals(rRes, cache.query(new SqlFieldsQuery(REPLICATED_QRY)).getAll());
                        }
                    } finally {
                        // Clearing lock in final handler to avoid endless loop if exception is thrown.
                        locks.set(g, 0);

                        int c = qryCnt.incrementAndGet();

                        if (c % logFreq == 0)
                            info("Executed queries: " + c);
                    }
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

                    try {
                        log.info("Stop node: " + g);

                        stopGrid(g);

                        Thread.sleep(rnd.nextInt(nodeLifeTime));

                        log.info("Start node: " + g);

                        startGrid(g);

                        Thread.sleep(rnd.nextInt(nodeLifeTime));
                    } finally {
                        locks.set(g, 0);

                        int c = restartCnt.incrementAndGet();

                        if (c % logFreq == 0)
                            info("Node restarts: " + c);
                    }
                }

                return true;
            }
        }, restartThreadsNum, "restart-thread");

        Thread.sleep(duration);

        info("Stopping..");

        restartsDone.set(true);

        fut2.get();

        info("Restarts stopped.");

        qrysDone.set(true);

        // Query thread can stuck in next page waiting loop because all nodes are left.
        try {
            fut1.get(5_000);
        } catch (IgniteFutureTimeoutCheckedException ignored) {
            fut1.cancel();
        }

        info("Queries stopped.");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     *
     */
    private static class Person implements Serializable {
        @QuerySqlField(index = true)
        int id;

        Person(int id) {
            this.id = id;
        }
    }

    /**
     *
     */
    private static class Purchase implements Serializable {
        @QuerySqlField(index = true)
        int personId;

        @QuerySqlField(index = true)
        int productId;

        Purchase(int personId, int productId) {
            this.personId = personId;
            this.productId = productId;
        }
    }

    /**
     *
     */
    private static class Company implements Serializable {
        @QuerySqlField(index = true)
        int id;

        Company(int id) {
            this.id = id;
        }
    }

    /**
     *
     */
    private static class Product implements Serializable {
        @QuerySqlField(index = true)
        int id;

        @QuerySqlField(index = true)
        int companyId;

        Product(int id, int companyId) {
            this.id = id;
            this.companyId = companyId;
        }
    }
}
