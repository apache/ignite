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

package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.AffinityUuid;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.GridRandom;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 */
public class IgniteCacheCollocatedQuerySelfTest extends GridCommonAbstractTest {
    /** */
    private static final String QRY =
        "select productId, sum(price) s, count(1) c " +
        "from Purchase " +
        "group by productId " +
        "having c > ? " +
        "order by s desc, productId limit ? ";

    /** */
    private static final int PURCHASES = 1000;

    /** */
    private static final int PRODUCTS = 10;

    /** */
    private static final int MAX_PRICE = 5;

    /** */
    private static final long SEED = ThreadLocalRandom.current().nextLong();

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        CacheConfiguration<?,?> cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setSwapEnabled(false);
        cacheCfg.setBackups(1);
        cacheCfg.setIndexedTypes(
            AffinityUuid.class, Purchase.class
        );

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(3);

        X.println("--> seed: " + SEED);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        ignite(0).cache(null).removeAll();
    }

    /**
     * @param c Cache.
     * @param collocated Colocated.
     * @return Result.
     */
    private static List<List<?>> query(IgniteCache<AffinityUuid,Purchase> c, boolean collocated) {
        return c.query(new SqlFieldsQuery(QRY).setArgs(30, 5).setCollocated(collocated)).getAll();
    }

    /**
     * Correct affinity.
     */
    public void testColocatedQueryRight() {
        IgniteCache<AffinityUuid,Purchase> c = ignite(0).cache(null);

        Random rnd = new GridRandom(SEED);

        for (int i = 0; i < PURCHASES; i++) {
            Purchase p = new Purchase();

            p.productId = rnd.nextInt(PRODUCTS);
            p.price = rnd.nextInt(MAX_PRICE);

            c.put(new AffinityUuid(p.productId), p); // Correct affinity.
        }

        List<List<?>> res1 = query(c, false);
        List<List<?>> res2 = query(c, true);

        X.println("res1: " + res1);
        X.println("res2: " + res2);

        assertFalse(res1.isEmpty());
        assertEquals(res1.toString(), res2.toString()); // TODO fix type conversion issue
    }

    /**
     * Correct affinity.
     */
    public void testColocatedQueryWrong() {
        IgniteCache<AffinityUuid,Purchase> c = ignite(0).cache(null);

        Random rnd = new GridRandom(SEED);

        for (int i = 0; i < PURCHASES; i++) {
            Purchase p = new Purchase();

            p.productId = rnd.nextInt(PRODUCTS);
            p.price = rnd.nextInt(MAX_PRICE);

            c.put(new AffinityUuid(rnd.nextInt(PRODUCTS)), p); // Random affinity.
        }

        List<List<?>> res1 = query(c, false);
        List<List<?>> res2 = query(c, true);

        X.println("res1: " + res1);
        X.println("res2: " + res2);

        assertFalse(res1.isEmpty());
        assertFalse(res1.equals(res2));
    }

    /**
     *
     */
    private static class Purchase implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @QuerySqlField
        int productId;

        /** */
        @QuerySqlField
        int price;

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            Purchase purchase = (Purchase)o;

            return productId == purchase.productId && price == purchase.price;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = productId;

            result = 31 * result + price;

            return result;
        }
    }
}
