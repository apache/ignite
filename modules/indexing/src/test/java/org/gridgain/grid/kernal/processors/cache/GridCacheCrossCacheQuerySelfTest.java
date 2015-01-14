/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.processors.cache.query.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;

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

        c.setMarshaller(new IgniteOptimizedMarshaller(false));

        c.setCacheConfiguration(createCache("replicated", GridCacheMode.REPLICATED),
            createCache("partitioned", GridCacheMode.PARTITIONED));

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
    private static GridCacheConfiguration createCache(String name, GridCacheMode mode) {
        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setName(name);
        cc.setCacheMode(mode);
        cc.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cc.setPreloadMode(SYNC);
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
            (GridCacheQueriesEx<Integer, FactPurchase>)ignite.<Integer, FactPurchase>cache(cache).queries();

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
    public void testTwoStepGroup() throws Exception {
        fillCaches();

        GridCacheQueriesEx<Integer, FactPurchase> qx =
            (GridCacheQueriesEx<Integer, FactPurchase>)ignite.<Integer, FactPurchase>cache("partitioned").queries();

        Set<Integer> set0 = new HashSet<>();

        for (List<?> o : qx.executeTwoStepQuery("partitioned", "select productId from FactPurchase group by productId")
            .get()) {
            X.println("___ -> " + o);

            assertTrue(set0.add((Integer) o.get(0)));
        }

        X.println("___ ");

        Set<Integer> set1 = new HashSet<>();

        for (List<?> o : qx.executeTwoStepQuery("partitioned", "select productId from FactPurchase")
            .get()) {
            X.println("___ -> " + o);

            set1.add((Integer)o.get(0));
        }

        assertFalse(set1.isEmpty());
        assertEquals(set0, set1);
    }

    /** @throws Exception If failed. */
    public void testOnProjection() throws Exception {
        fillCaches();

        GridCacheProjection<Integer, FactPurchase> prj = ignite.<Integer, FactPurchase>cache("partitioned").projection(
            new IgnitePredicate<GridCacheEntry<Integer, FactPurchase>>() {
                @Override
                public boolean apply(GridCacheEntry<Integer, FactPurchase> e) {
                    return e.getKey() > 12;
                }
            });

        List<Map.Entry<Integer, FactPurchase>> res = body(prj);

        check(res);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void fillCaches() throws IgniteCheckedException, InterruptedException {
        int idGen = 0;

        GridCache<Integer, Object> dimCache = ignite.cache("replicated");

        for (int i = 0; i < 2; i++) {
            int id = idGen++;

            dimCache.put(id, new DimStore(id, "Store" + id));
        }

        for (int i = 0; i < 5; i++) {
            int id = idGen++;

            dimCache.put(id, new DimProduct(id, "Product" + id));
        }

        GridCacheProjection<Integer, DimStore> stores = dimCache.projection(Integer.class, DimStore.class);
        GridCacheProjection<Integer, DimProduct> prods = dimCache.projection(Integer.class, DimProduct.class);

        GridCache<Integer, FactPurchase> factCache = ignite.cache("partitioned");

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

            factCache.put(id, new FactPurchase(id, prod.getId(), store.getId()));
        }
    }

    /**
     * Fills the caches with data and executes the query.
     *
     * @param prj Cache projection.
     * @throws Exception If failed.
     * @return Result.
     */
    private List<Map.Entry<Integer, FactPurchase>> body(GridCacheProjection<Integer, FactPurchase> prj)
        throws Exception {
        GridCacheQuery<Map.Entry<Integer, FactPurchase>> qry = (prj == null ?
            ignite.<Integer, FactPurchase>cache("partitioned") : prj).queries().createSqlQuery(FactPurchase.class,
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
     * 'dimension'} and will be cached in {@link GridCacheMode#REPLICATED} cache.
     */
    private static class DimProduct {
        /** Primary key. */
        @GridCacheQuerySqlField(unique = true)
        private int id;

        /** Product name. */
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
     * and will be cached in {@link GridCacheMode#REPLICATED} cache.
     */
    private static class DimStore {
        /** Primary key. */
        @GridCacheQuerySqlField(unique = true)
        private int id;

        /** Store name. */
        @GridCacheQuerySqlField
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
     * larger {@link GridCacheMode#PARTITIONED} cache.
     */
    private static class FactPurchase {
        /** Primary key. */
        @GridCacheQuerySqlField(unique = true)
        private int id;

        /** Foreign key to store at which purchase occurred. */
        @GridCacheQuerySqlField
        private int storeId;

        /** Foreign key to purchased product. */
        @GridCacheQuerySqlField
        private int productId;

        /**
         * Constructs a purchase record.
         *
         * @param id Purchase ID.
         * @param productId Purchased product ID.
         * @param storeId Store ID.
         */
        FactPurchase(int id, int productId, int storeId) {
            this.id = id;
            this.productId = productId;
            this.storeId = storeId;
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
