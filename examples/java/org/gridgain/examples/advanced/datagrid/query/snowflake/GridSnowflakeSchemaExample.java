// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.advanced.datagrid.query.snowflake;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.query.GridCacheQueryType.*;

/**
 * <a href="http://en.wikipedia.org/wiki/Snowflake_schema">Snowflake Schema</a> is a logical
 * arrangement of data in which data is split into {@code dimensions} and {@code facts}.
 * <i>Dimensions</i> can be referenced or joined by other <i>dimensions</i> or <i>facts</i>,
 * however, <i>facts</i> are generally not referenced by other facts. You can view <i>dimensions</i>
 * as your master or reference data, while <i>facts</i> are usually large data sets of events or
 * other objects that continuously come into the system and may change frequently. In GridGain
 * such architecture is supported via cross-cache queries. By storing <i>dimensions</i> in
 * {@link GridCacheMode#REPLICATED REPLICATED} caches and <i>facts</i> in much larger
 * {@link GridCacheMode#PARTITIONED PARTITIONED} caches you can freely execute distributed joins across
 * your whole in-memory data grid, thus querying your in memory data without any limitations.
 * <p>
 * In this example we have two <i>dimensions</i>, {@link DimProduct} and {@link DimStore} and
 * one <i>fact</i> - {@link FactPurchase}. Queries are executed by joining dimensions and facts
 * in various ways.
 * <p>
 * Remote nodes should always be started with configuration file which includes
 * cache: {@code 'ggstart.sh examples/config/example-cache.xml'}.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridSnowflakeSchemaExample {
    /** ID generator. */
    private static int idGen = (int)System.currentTimeMillis();

    /**
     * Main method.
     *
     * @param args Parameters (none for this example).
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        GridGain.start("examples/config/example-cache.xml");

        try {
            populateDimensions();
            populateFacts();

            queryStorePurchases();
            queryProductPurchases();
        }
        finally {
            GridGain.stop(false);
        }
    }

    /**
     * Populate cache with {@code 'dimensions'} which in our case are
     * {@link DimStore} and {@link DimProduct} instances.
     *
     * @throws GridException If failed.
     */
    private static void populateDimensions() throws GridException {
        GridCache<Integer, Object> cache = GridGain.grid().cache("replicated");

        DimStore store1 = new DimStore(idGen++, "Store1", "12345", "321 Chilly Dr, NY");
        DimStore store2 = new DimStore(idGen++, "Store2", "54321", "123 Windy Dr, San Francisco");

        // Populate stores.
        cache.put(store1.getId(), store1);
        cache.put(store2.getId(), store2);

        // Populate products
        for (int i = 0; i < 20; i++) {
            int id = idGen++;

            cache.put(id, new DimProduct(id, "Product" + i, i + 1, (i + 1) * 10));
        }
    }

    /**
     * Populate cache with {@code 'facts'}, which in our case are {@link FactPurchase} objects.
     *
     * @throws GridException If failed.
     */
    private static void populateFacts() throws GridException {
        GridCache<Integer, Object> dimCache = GridGain.grid().cache("replicated");
        GridCache<Integer, Object> factCache = GridGain.grid().cache("partitioned");

        GridCacheProjection<Integer, DimStore> stores = dimCache.projection(Integer.class, DimStore.class);
        GridCacheProjection<Integer, DimProduct> prods = dimCache.projection(Integer.class, DimProduct.class);

        for (int i = 0; i < 100; i++) {
            int id = idGen++;

            DimStore store = rand(stores.values());
            DimProduct prod = rand(prods.values());

            factCache.put(id, new FactPurchase(id, prod.getId(), store.getId(), (i + 1)));
        }
    }

    /**
     * Query all purchases made at a specific store. This query uses cross-cache joins
     * between {@link DimStore} objects stored in {@code 'replicated'} cache and
     * {@link FactPurchase} objects stored in {@code 'partitioned'} cache.
     *
     * @throws GridException If failed.
     */
    private static void queryStorePurchases() throws GridException {
        GridCache<Integer, FactPurchase> factCache = GridGain.grid().cache("partitioned");

        // All purchases for store1.
        // ========================

        // Create cross cache query to get all purchases made at store1.
        GridCacheQuery<Integer, FactPurchase> storePurchases = factCache.queries().createQuery(
            SQL,
            FactPurchase.class,
            "from \"replicated\".DimStore, \"partitioned\".FactPurchase " +
                "where DimStore.id=FactPurchase.storeId and DimStore.name=?");

        printQueryResults("All purchases made at store1:",
            storePurchases.queryArguments("Store1").execute().get());
    }

    /**
     * Query all purchases made at a specific store for 3 specific products.
     * This query uses cross-cache joins between {@link DimStore}, {@link DimProduct}
     * objects stored in {@code 'replicated'} cache and {@link FactPurchase} objects
     * stored in {@code 'partitioned'} cache.
     *
     * @throws GridException If failed.
     */
    private static void queryProductPurchases() throws GridException {
        GridCache<Integer, Object> dimCache = GridGain.grid().cache("replicated");
        GridCache<Integer, FactPurchase> factCache = GridGain.grid().cache("partitioned");

        GridCacheProjection<Integer, DimProduct> prods = dimCache.projection(Integer.class, DimProduct.class);

        // All purchases for certain product made at store2.
        // =================================================

        DimProduct p1 = rand(prods.values());
        DimProduct p2 = rand(prods.values());
        DimProduct p3 = rand(prods.values());

        System.out.println("IDs of products [p1=" + p1.getId() + ", p2=" + p2.getId() + ", p3=" + p3.getId() + ']');

        // Create cross cache query to get all purchases made at store2
        // for specified products.
        GridCacheQuery<Integer, FactPurchase> prodPurchases = factCache.queries().createQuery(
            SQL,
            FactPurchase.class,
            "from \"replicated\".DimStore, \"replicated\".DimProduct, \"partitioned\".FactPurchase " +
                "where DimStore.id=FactPurchase.storeId and DimProduct.id=FactPurchase.productId " +
                "and DimStore.name=? and DimProduct.id in(?, ?, ?)");

        printQueryResults("All purchases made at store2 for 3 specific products:",
            prodPurchases.queryArguments("Store2", p1.getId(), p2.getId(), p3.getId()).execute().get());
    }

    /**
     * Print query results.
     *
     * @param msg Initial message.
     * @param res Results to print.
     */
    private static <V> void printQueryResults(String msg, Iterable<Map.Entry<Integer, V>> res) {
        System.out.println(msg);

        for (Map.Entry<?, ?> e : res)
            System.out.println("    " + e.getValue().toString());
    }

    /**
     * Gets random value from given collection.
     *
     * @param c Input collection (no {@code null} and not emtpy).
     * @return Random value from the input collection.
     */
    @SuppressWarnings("UnusedDeclaration")
    private static <T> T rand(Collection<? extends T> c) {
        A.notNull(c, "c");

        int n = ThreadLocalRandom.current().nextInt(c.size());

        int i = 0;

        for (T t : c) {
            if (i++ == n) {
                return t;
            }
        }

        throw new ConcurrentModificationException();
    }
}
