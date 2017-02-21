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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;

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

        c.setCacheConfiguration(createCache("replicated", CacheMode.REPLICATED),
            createCache("partitioned", CacheMode.PARTITIONED));

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ignite = startGridsMultiThreaded(3);

        fillCaches();
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
        CacheConfiguration<?,?> cc = defaultCacheConfiguration();

        cc.setName(name);
        cc.setCacheMode(mode);
        cc.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cc.setRebalanceMode(SYNC);
        cc.setSwapEnabled(true);
        cc.setAtomicityMode(TRANSACTIONAL);

        if (mode == CacheMode.PARTITIONED)
            cc.setIndexedTypes(
                Integer.class, FactPurchase.class
            );
        else if (mode == CacheMode.REPLICATED)
            cc.setIndexedTypes(
                Integer.class, DimProduct.class,
                Integer.class, DimStore.class
            );
        else
            throw new IllegalStateException("mode: " + mode);

        return cc;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTwoStepGroupAndAggregates() throws Exception {
        IgniteInternalCache<Integer, FactPurchase> cache =
            ((IgniteKernal)ignite).getCache("partitioned");

        GridQueryProcessor qryProc = ((IgniteKernal) ignite).context().query();

        Set<Integer> set1 = new HashSet<>();

        X.println("___ simple");

        SqlFieldsQuery qry = new SqlFieldsQuery("select f.productId, p.name, f.price " +
            "from FactPurchase f, \"replicated\".DimProduct p where p.id = f.productId ");

        for (List<?> o : qryProc.queryTwoStep(cache.context(), qry).getAll()) {
            X.println("___ -> " + o);

            set1.add((Integer)o.get(0));
        }

        assertFalse(set1.isEmpty());

        Set<Integer> set0 = new HashSet<>();

        X.println("___ GROUP BY");

        qry = new SqlFieldsQuery("select productId from FactPurchase group by productId");

        for (List<?> o : qryProc.queryTwoStep(cache.context(), qry).getAll()) {
            X.println("___ -> " + o);

            assertTrue(set0.add((Integer) o.get(0)));
        }

        assertEquals(set0, set1);

        X.println("___ GROUP BY AVG MIN MAX SUM COUNT(*) COUNT(x) (MAX - MIN) * 2 as");

        Set<String> names = new HashSet<>();

        qry = new SqlFieldsQuery("select p.name, avg(f.price), min(f.price), max(f.price), sum(f.price), count(*), " +
            "count(nullif(f.price, 5)), (max(f.price) - min(f.price)) * 3 as nn " +
            ", CAST(max(f.price) + 7 AS VARCHAR) " +
            "from FactPurchase f, \"replicated\".DimProduct p " +
            "where p.id = f.productId " +
            "group by f.productId, p.name");

        for (List<?> o : qryProc.queryTwoStep(cache.context(), qry).getAll()) {
            X.println("___ -> " + o);

            assertTrue(names.add((String)o.get(0)));
            assertEquals(i(o, 4), i(o, 2) + i(o, 3));
            assertEquals(i(o, 7), (i(o, 3) - i(o, 2)) * 3);
            assertEquals(o.get(8), Integer.toString(i(o, 3) + 7));
        }

        X.println("___ SUM HAVING");

        qry = new SqlFieldsQuery("select p.name, sum(f.price) s " +
            "from FactPurchase f, \"replicated\".DimProduct p " +
            "where p.id = f.productId " +
            "group by f.productId, p.name " +
            "having s >= 15");

        for (List<?> o : qryProc.queryTwoStep(cache.context(), qry).getAll()) {
            X.println("___ -> " + o);

            assertTrue(i(o, 1) >= 15);
        }

        X.println("___ DISTINCT ORDER BY TOP");

        int top = 6;

        qry = new SqlFieldsQuery("select top 3 distinct productId " +
            "from FactPurchase f order by productId desc ");

        for (List<?> o : qryProc.queryTwoStep(cache.context(), qry).getAll()) {
            X.println("___ -> " + o);

            assertEquals(top--, o.get(0));
        }

        X.println("___ DISTINCT ORDER BY OFFSET LIMIT");

        top = 5;

        qry = new SqlFieldsQuery("select distinct productId " +
            "from FactPurchase f order by productId desc limit 2 offset 1");

        for (List<?> o : qryProc.queryTwoStep(cache.context(), qry).getAll()) {
            X.println("___ -> " + o);

            assertEquals(top--, o.get(0));
        }

        assertEquals(3, top);
    }

    /**
     * @throws Exception If failed.
     */
    public void testApiQueries() throws Exception {
        IgniteCache<Object,Object> c = ignite.cache("partitioned");

        c.query(new SqlFieldsQuery("select cast(? as varchar) from FactPurchase").setArgs("aaa")).getAll();

        List<List<?>> res = c.query(new SqlFieldsQuery("select cast(? as varchar), id " +
            "from FactPurchase order by id limit ? offset ?").setArgs("aaa", 1, 1)).getAll();

        assertEquals(1, res.size());
        assertEquals("aaa", res.get(0).get(0));
    }

//    @Override protected long getTestTimeout() {
//        return 10 * 60 * 1000;
//    }

    /**
     * @param l List.
     * @param idx Index.
     * @return Int.
     */
    private static int i(List<?> l, int idx){
        return ((Number)l.get(idx)).intValue();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void fillCaches() throws IgniteCheckedException {
        int idGen = 0;

        GridCacheAdapter<Integer, Object> dimCache = ((IgniteKernal)ignite).internalCache("replicated");

        List<DimStore> dimStores = new ArrayList<>();

        List<DimProduct> dimProds = new ArrayList<>();

        for (int i = 0; i < 2; i++) {
            int id = idGen++;

            DimStore v = new DimStore(id, "Store" + id);

            dimCache.getAndPut(id, v);

            dimStores.add(v);
        }

        for (int i = 0; i < 5; i++) {
            int id = idGen++;

            DimProduct v = new DimProduct(id, "Product" + id);

            dimCache.getAndPut(id, v);

            dimProds.add(v);
        }

        GridCacheAdapter<Integer, FactPurchase> factCache = ((IgniteKernal)ignite).internalCache("partitioned");

        Collections.sort(dimStores, new Comparator<DimStore>() {
            @Override public int compare(DimStore o1, DimStore o2) {
                return o1.getId() > o2.getId() ? 1 : o1.getId() < o2.getId() ? -1 : 0;
            }
        });

        Collections.sort(dimProds, new Comparator<DimProduct>() {
            @Override public int compare(DimProduct o1, DimProduct o2) {
                return o1.getId() > o2.getId() ? 1 : o1.getId() < o2.getId() ? -1 : 0;
            }
        });

        for (int i = 0; i < 10; i++) {
            int id = idGen++;

            DimStore store = dimStores.get(i % dimStores.size());
            DimProduct prod = dimProds.get(i % dimProds.size());

            factCache.getAndPut(id, new FactPurchase(id, prod.getId(), store.getId(), i + 5));
        }
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
