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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.annotations.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import javax.cache.*;
import java.util.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CacheRebalanceMode.*;

/**
 * Tests cross cache queries.
 */
public class GridCacheCrossCacheQuerySelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        c.setMarshaller(new OptimizedMarshaller(false));

        c.setCacheConfiguration(createCache("replicated", CacheMode.REPLICATED),
            createCache("partitioned", CacheMode.PARTITIONED));

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ignite = startGridsMultiThreaded(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        ignite = null;
    }

    /**
     * Creates new cache configuration.
     *
     * @param name Cache name.
     * @param mode Cache mode.
     * @return Cache configuration.
     */
    private static CacheConfiguration createCache(String name, CacheMode mode) {
        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setName(name);
        cc.setCacheMode(mode);
        cc.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cc.setRebalanceMode(SYNC);
        cc.setSwapEnabled(true);
        cc.setEvictNearSynchronized(false);
        cc.setAtomicityMode(TRANSACTIONAL);
        cc.setDistributionMode(NEAR_PARTITIONED);

        return cc;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTwoStep() throws Exception {
        fillCaches();

        String cache = "partitioned";

        GridCacheQueriesEx<Integer, FactPurchase> qx =
            (GridCacheQueriesEx<Integer, FactPurchase>)((IgniteKernal)ignite)
                .<Integer, FactPurchase>cache(cache).queries();

//        for (Map.Entry<Integer, FactPurchase> e : qx.createSqlQuery(FactPurchase.class, "1 = 1").execute().get())
//            X.println("___ "  + e);

        GridCacheTwoStepQuery q = new GridCacheTwoStepQuery("select cast(sum(x) as long) from _cnts_ where ? = ?", 1, 1);

        q.addMapQuery("_cnts_", "select count(*) x from \"partitioned\".FactPurchase where ? = ?", 2 ,2);

        Object cnt = qx.execute(cache, q).get().iterator().next().get(0);

        assertEquals(10L, cnt);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTwoStepGroupAndAggregates() throws Exception {
        fillCaches();

        GridCacheQueriesEx<Integer, FactPurchase> qx =
            (GridCacheQueriesEx<Integer, FactPurchase>)((IgniteKernal)ignite)
                .<Integer, FactPurchase>cache("partitioned").queries();

        Set<Integer> set1 = new HashSet<>();

        X.println("___ simple");

        for (List<?> o : qx.executeTwoStepQuery("partitioned", "select f.productId, p.name, f.price " +
            "from FactPurchase f, \"replicated\".DimProduct p where p.id = f.productId ").get()) {
            X.println("___ -> " + o);

            set1.add((Integer)o.get(0));
        }

        Set<Integer> set0 = new HashSet<>();

        X.println("___ GROUP BY");

        for (List<?> o : qx.executeTwoStepQuery("partitioned", "select productId from FactPurchase group by productId")
            .get()) {
            X.println("___ -> " + o);

            assertTrue(set0.add((Integer) o.get(0)));
        }

        assertFalse(set1.isEmpty());
        assertEquals(set0, set1);

        X.println("___ AVG MIN MAX SUM COUNT(*) COUNT(x)");

        for (List<?> o : qx.executeTwoStepQuery("partitioned",
            "select p.name, avg(f.price), min(f.price), max(f.price), sum(f.price), count(*), " +
                "count(nullif(f.price, 5)) " +
                "from FactPurchase f, \"replicated\".DimProduct p " +
                "where p.id = f.productId " +
                "group by f.productId, p.name").get()) {
            X.println("___ -> " + o);
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void fillCaches() throws IgniteCheckedException {
        int idGen = 0;

        GridCache<Integer, Object> dimCache = ((IgniteKernal)ignite).cache("replicated");

        for (int i = 0; i < 2; i++) {
            int id = idGen++;

            dimCache.put(id, new DimStore(id, "Store" + id));
        }

        for (int i = 0; i < 5; i++) {
            int id = idGen++;

            dimCache.put(id, new DimProduct(id, "Product" + id));
        }

        CacheProjection<Integer, DimStore> stores = dimCache.projection(Integer.class, DimStore.class);
        CacheProjection<Integer, DimProduct> prods = dimCache.projection(Integer.class, DimProduct.class);

        GridCache<Integer, FactPurchase> factCache = ((IgniteKernal)ignite).cache("partitioned");

        List<DimStore> dimStores = new ArrayList<>(stores.values());
        Collections.sort(dimStores, new Comparator<DimStore>() {
            @Override public int compare(DimStore o1, DimStore o2) {
                return o1.getId() > o2.getId() ? 1 : o1.getId() < o2.getId() ? -1 : 0;
            }
        });

        List<DimProduct> dimProds = new ArrayList<>(prods.values());
        Collections.sort(dimProds, new Comparator<DimProduct>() {
            @Override
            public int compare(DimProduct o1, DimProduct o2) {
                return o1.getId() > o2.getId() ? 1 : o1.getId() < o2.getId() ? -1 : 0;
            }
        });

        for (int i = 0; i < 10; i++) {
            int id = idGen++;

            DimStore store = dimStores.get(i % dimStores.size());
            DimProduct prod = dimProds.get(i % dimProds.size());

            factCache.put(id, new FactPurchase(id, prod.getId(), store.getId(), i + 5));
        }
    }

    /**
     * Fills the caches with data and executes the query.
     *
     * @param prj Cache projection.
     * @throws Exception If failed.
     * @return Result.
     */
    private List<Map.Entry<Integer, FactPurchase>> body(CacheProjection<Integer, FactPurchase> prj)
        throws Exception {
        CacheQuery<Map.Entry<Integer, FactPurchase>> qry = (prj == null ?
            ((IgniteKernal)ignite)
                .<Integer, FactPurchase>cache("partitioned") : prj).queries().createSqlQuery(FactPurchase.class,
            "from \"replicated\".DimStore, \"partitioned\".FactPurchase where DimStore.id = FactPurchase.storeId");

        List<Map.Entry<Integer, FactPurchase>> res = new ArrayList<>(qry.execute().get());
        Collections.sort(res, new Comparator<Map.Entry<Integer, FactPurchase>>() {
            @Override public int compare(Map.Entry<Integer, FactPurchase> o1, Map.Entry<Integer, FactPurchase> o2) {
                return o1.getKey() > o2.getKey() ? 1 : o1.getKey() < o2.getKey() ? -1 : 0;
            }
        });

        return res;
    }

    /**
     * Checks result.
     * @param res Result to check.
     */
    private static void check(List<Map.Entry<Integer, FactPurchase>> res) {
        assertEquals("Result size", 4, res.size());

        checkPurchase(res.get(0), 13, 3, 0);
        checkPurchase(res.get(1), 14, 4, 1);
        checkPurchase(res.get(2), 15, 5, 0);
        checkPurchase(res.get(3), 16, 6, 1);
    }

    /**
     * Checks purchase.
     * @param entry Entry to check.
     * @param id ID.
     * @param productId Product ID.
     * @param storeId Store ID.
     */
    private static void checkPurchase(Map.Entry<Integer, FactPurchase> entry, int id, int productId, int storeId) {
        FactPurchase purchase = entry.getValue();

        assertEquals("Id", id, entry.getKey().intValue());
        assertEquals("Id", id, purchase.getId());
        assertEquals("ProductId", productId, purchase.getProductId());
        assertEquals("StoreId", storeId, purchase.getStoreId());
    }

    /**
     * Represents a product available for purchase. In our {@code snowflake} schema a {@code product} is a {@code
     * 'dimension'} and will be cached in {@link org.apache.ignite.cache.CacheMode#REPLICATED} cache.
     */
    private static class DimProduct {
        /** Primary key. */
        @QuerySqlField
        private int id;

        /** Product name. */
        @QuerySqlField
        private String name;

        /**
         * Constructs a product instance.
         *
         * @param id Product ID.
         * @param name Product name.
         */
        DimProduct(int id, String name) {
            this.id = id;
            this.name = name;
        }

        /**
         * Gets product ID.
         *
         * @return Product ID.
         */
        public int getId() {
            return id;
        }

        /**
         * Gets product name.
         *
         * @return Product name.
         */
        public String getName() {
            return name;
        }
    }

    /**
     * Represents a physical store location. In our {@code snowflake} schema a {@code store} is a {@code 'dimension'}
     * and will be cached in {@link org.apache.ignite.cache.CacheMode#REPLICATED} cache.
     */
    private static class DimStore {
        /** Primary key. */
        @QuerySqlField
        private int id;

        /** Store name. */
        @QuerySqlField
        private String name;

        /**
         * Constructs a store instance.
         *
         * @param id Store ID.
         * @param name Store name.
         */
        DimStore(int id, String name) {
            this.id = id;
            this.name = name;
        }

        /**
         * Gets store ID.
         *
         * @return Store ID.
         */
        public int getId() {
            return id;
        }

        /**
         * Gets store name.
         *
         * @return Store name.
         */
        public String getName() {
            return name;
        }
    }

    /**
     * Represents a purchase record. In our {@code snowflake} schema purchase is a {@code 'fact'} and will be cached in
     * larger {@link org.apache.ignite.cache.CacheMode#PARTITIONED} cache.
     */
    private static class FactPurchase {
        /** Primary key. */
        @QuerySqlField
        private int id;

        /** Foreign key to store at which purchase occurred. */
        @QuerySqlField
        private int storeId;

        /** Foreign key to purchased product. */
        @QuerySqlField
        private int productId;

        @QuerySqlField
        private int price;

        /**
         * Constructs a purchase record.
         *
         * @param id Purchase ID.
         * @param productId Purchased product ID.
         * @param storeId Store ID.
         */
        FactPurchase(int id, int productId, int storeId, int price) {
            this.id = id;
            this.productId = productId;
            this.storeId = storeId;
            this.price = price;
        }

        /**
         * Gets purchase ID.
         *
         * @return Purchase ID.
         */
        public int getId() {
            return id;
        }

        /**
         * Gets purchased product ID.
         *
         * @return Product ID.
         */
        public int getProductId() {
            return productId;
        }

        /**
         * Gets ID of store at which purchase was made.
         *
         * @return Store ID.
         */
        public int getStoreId() {
            return storeId;
        }
    }
}
