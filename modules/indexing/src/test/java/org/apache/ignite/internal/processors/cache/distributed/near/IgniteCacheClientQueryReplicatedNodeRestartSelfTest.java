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
import java.util.LinkedList;
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
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.util.GridRandom;
import org.apache.ignite.internal.util.typedef.CAX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Test for distributed queries with replicated client cache and node restarts.
 */
public class IgniteCacheClientQueryReplicatedNodeRestartSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String QRY = "select co.id, count(*) cnt\n" +
        "from \"pe\".Person pe, \"pr\".Product pr, \"co\".Company co, \"pu\".Purchase pu\n" +
        "where pe.id = pu.personId and pu.productId = pr.id and pr.companyId = co.id \n" +
        "group by co.id order by cnt desc, co.id";

    /** */
    private static final P1<ClusterNode> DATA_NODES_FILTER = new P1<ClusterNode>() {
            @Override public boolean apply(ClusterNode clusterNode) {
                String gridName = clusterNode.attribute(IgniteNodeAttributes.ATTR_GRID_NAME);

                return !gridName.endsWith(String.valueOf(GRID_CNT - 1)); // The last one is client only.
            }
        };

    /** */
    private static final List<List<?>> FAKE = new LinkedList<>();

    /** */
    private static final int GRID_CNT = 5;

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
        X.println("grid name: " + gridName);

        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        int i = 0;

        CacheConfiguration<?, ?>[] ccs = new CacheConfiguration[4];

        for (String name : F.asList("co", "pr", "pe", "pu")) {
            CacheConfiguration<?, ?> cc = defaultCacheConfiguration();

            cc.setNodeFilter(DATA_NODES_FILTER);
            cc.setName(name);
            cc.setCacheMode(REPLICATED);
            cc.setWriteSynchronizationMode(FULL_SYNC);
            cc.setAtomicityMode(TRANSACTIONAL);
            cc.setRebalanceMode(SYNC);
            cc.setAffinity(new RendezvousAffinityFunction(false, 50));

            switch (name) {
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

                case "pe":
                    cc.setIndexedTypes(
                        Integer.class, Person.class
                    );

                    break;

                case "pu":
                    cc.setIndexedTypes(
                        AffinityKey.class, Purchase.class
                    );

                    break;
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
     * @param c Cache.
     * @param client If it must be a client cache.
     */
    private void assertClient(IgniteCache<?,?> c, boolean client) {
        assertTrue(((IgniteCacheProxy)c).context().affinityNode() == !client);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRestarts() throws Exception {
        int duration = 90 * 1000;
        int qryThreadNum = 5;
        int restartThreadsNum = 2; // 2 of 4 data nodes
        final int nodeLifeTime = 2 * 1000;
        final int logFreq = 10;

        startGridsMultiThreaded(GRID_CNT);

        final AtomicIntegerArray locks = new AtomicIntegerArray(GRID_CNT - 1); // The last is client only.

        fillCaches();

        final List<List<?>> pRes = grid(0).cache("pu").query(new SqlFieldsQuery(QRY)).getAll();

        Thread.sleep(3000);

        assertEquals(pRes, grid(0).cache("pu").query(new SqlFieldsQuery(QRY)).getAll());

        assertFalse(pRes.isEmpty());

        final AtomicInteger qryCnt = new AtomicInteger();
        final AtomicBoolean qrysDone = new AtomicBoolean();

        final List<Integer> cacheSize = new ArrayList<>(4);

        for (int i = 0; i < GRID_CNT - 1; i++) {
            int j = 0;

            for (String cacheName : F.asList("co", "pr", "pe", "pu")) {
                IgniteCache<?,?> cache = grid(i).cache(cacheName);

                assertClient(cache, false);

                if (i == 0)
                    cacheSize.add(cache.size());
                else
                    assertEquals(cacheSize.get(j++).intValue(), cache.size());
            }
        }

        int j = 0;

        for (String cacheName : F.asList("co", "pr", "pe", "pu")) {
            IgniteCache<?,?> cache = grid(GRID_CNT - 1).cache(cacheName);

            assertClient(cache, true);

            assertEquals(cacheSize.get(j++).intValue(), cache.size());
        }

        final IgniteCache<?,?> clientCache = grid(GRID_CNT - 1).cache("pu");

        IgniteInternalFuture<?> fut1 = multithreadedAsync(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                GridRandom rnd = new GridRandom();

                while (!qrysDone.get()) {
                    SqlFieldsQuery qry = new SqlFieldsQuery(QRY);

                    boolean smallPageSize = rnd.nextBoolean();

                    if (smallPageSize)
                        qry.setPageSize(3);

                    List<List<?>> res;

                    try {
                        res = clientCache.query(qry).getAll();
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

                        res = FAKE;
                    }

                    if (res != FAKE && !res.equals(pRes)) {
                        int j = 0;

                        // Check for data loss.
                        for (String cacheName : F.asList("co", "pr", "pe", "pu")) {
                            assertEquals(cacheName, cacheSize.get(j++).intValue(),
                                grid(GRID_CNT - 1).cache(cacheName).size());
                        }

                        assertEquals(pRes, res); // Fail with nice message.
                    }

                    int c = qryCnt.incrementAndGet();

                    if (c % logFreq == 0)
                        info("Executed queries: " + c);
                }
            }
        }, qryThreadNum);

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

                    stopGrid(g);

                    Thread.sleep(rnd.nextInt(nodeLifeTime));

                    startGrid(g);

                    Thread.sleep(rnd.nextInt(nodeLifeTime));

                    locks.set(g, 0);

                    int c = restartCnt.incrementAndGet();

                    if (c % logFreq == 0)
                        info("Node restarts: " + c);
                }

                return true;
            }
        }, restartThreadsNum);

        Thread.sleep(duration);

        info("Stopping..");

        restartsDone.set(true);

        fut2.get();

        info("Restarts stopped.");

        qrysDone.set(true);

        fut1.get();

        info("Queries stopped.");
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