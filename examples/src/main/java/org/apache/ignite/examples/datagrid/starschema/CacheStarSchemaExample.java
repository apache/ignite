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

package org.apache.ignite.examples.datagrid.starschema;

import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.ExampleNodeStartup;

/**
 * <a href="http://en.wikipedia.org/wiki/Snowflake_schema">Snowflake Schema</a> is a logical
 * arrangement of data in which data is split into {@code dimensions} and {@code facts}.
 * <i>Dimensions</i> can be referenced or joined by other <i>dimensions</i> or <i>facts</i>,
 * however, <i>facts</i> are generally not referenced by other facts. You can view <i>dimensions</i>
 * as your master or reference data, while <i>facts</i> are usually large data sets of events or
 * other objects that continuously come into the system and may change frequently. In Ignite
 * such architecture is supported via cross-cache queries. By storing <i>dimensions</i> in
 * {@link CacheMode#REPLICATED REPLICATED} caches and <i>facts</i> in much larger
 * {@link CacheMode#PARTITIONED PARTITIONED} caches you can freely execute distributed joins across
 * your whole in-memory data ignite cluster, thus querying your in memory data without any limitations.
 * <p>
 * In this example we have two <i>dimensions</i>, {@link DimProduct} and {@link DimStore} and
 * one <i>fact</i> - {@link FactPurchase}. Queries are executed by joining dimensions and facts
 * in various ways.
 * <p>
 * Remote nodes can be started with {@link ExampleNodeStartup} in another JVM which will
 * start node with {@code examples/config/example-ignite.xml} configuration.
 */
public class CacheStarSchemaExample {
    /** Partitioned cache name. */
    private static final String FACT_CACHE_NAME = CacheStarSchemaExample.class.getSimpleName() + "Fact";

    /** Replicated cache name. */
    private static final String DIM_STORE_CACHE_NAME = CacheStarSchemaExample.class.getSimpleName() + "DimStore";

    /** Replicated cache name. */
    private static final String DIM_PROD_CACHE_NAME = CacheStarSchemaExample.class.getSimpleName() + "DimProd";

    /** ID generator. */
    private static int idGen;

    /** DimStore data. */
    private static Map<Integer, DimStore> dataStore = new HashMap<>();

    /** DimProduct data. */
    private static Map<Integer, DimProduct> dataProduct = new HashMap<>();

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Cache star schema example started.");

            CacheConfiguration<Integer, FactPurchase> factCacheCfg = new CacheConfiguration<>(FACT_CACHE_NAME);
            factCacheCfg.setCacheMode(CacheMode.PARTITIONED);
            factCacheCfg.setIndexedTypes(Integer.class, FactPurchase.class);

            CacheConfiguration<Integer, DimStore> dimStoreCacheCfg = new CacheConfiguration<>(DIM_STORE_CACHE_NAME);
            dimStoreCacheCfg.setCacheMode(CacheMode.REPLICATED);
            dimStoreCacheCfg.setIndexedTypes(Integer.class, DimStore.class);

            CacheConfiguration<Integer, DimProduct> dimProdCacheCfg = new CacheConfiguration<>(DIM_PROD_CACHE_NAME);
            dimProdCacheCfg.setCacheMode(CacheMode.REPLICATED);
            dimProdCacheCfg.setIndexedTypes(Integer.class, DimProduct.class);

            // Auto-close cache at the end of the example.
            try (IgniteCache<Integer, FactPurchase> factCache = ignite.getOrCreateCache(factCacheCfg);
                 IgniteCache<Integer, DimStore> dimStoreCache = ignite.getOrCreateCache(dimStoreCacheCfg);
                 IgniteCache<Integer, DimProduct> dimProdCache = ignite.getOrCreateCache(dimProdCacheCfg)) {

                populateDimensions(dimStoreCache, dimProdCache);
                populateFacts(factCache);

                queryStorePurchases();
                queryProductPurchases();
            }
            finally {
                // Distributed cache could be removed from cluster only by #destroyCache() call.
                ignite.destroyCache(FACT_CACHE_NAME);
                ignite.destroyCache(DIM_STORE_CACHE_NAME);
                ignite.destroyCache(DIM_PROD_CACHE_NAME);
            }
        }
    }

    /**
     * Populate cache with {@code 'dimensions'} which in our case are
     * {@link DimStore} and {@link DimProduct} instances.
     * @param dimStoreCache Cache of the DimStores to populate.
     * @param dimProdCache Cache of the DimProducts to populate.
     *
     * @throws IgniteException If failed.
     */
    private static void populateDimensions(Cache<Integer, DimStore> dimStoreCache,
        Cache<Integer, DimProduct> dimProdCache) throws IgniteException {
        DimStore store1 = new DimStore(idGen++, "Store1", "12345", "321 Chilly Dr, NY");
        DimStore store2 = new DimStore(idGen++, "Store2", "54321", "123 Windy Dr, San Francisco");

        // Populate stores.
        dimStoreCache.put(store1.getId(), store1);
        dimStoreCache.put(store2.getId(), store2);

        dataStore.put(store1.getId(), store1);
        dataStore.put(store2.getId(), store2);

        // Populate products
        for (int i = 0; i < 20; i++) {
            int id = idGen++;

            DimProduct product = new DimProduct(id, "Product" + i, i + 1, (i + 1) * 10);

            dimProdCache.put(id, product);

            dataProduct.put(id, product);
        }
    }

    /**
     * Populate cache with {@code 'facts'}, which in our case are {@link FactPurchase} objects.
     * @param factCache Cache to populate.
     *
     * @throws IgniteException If failed.
     */
    private static void populateFacts(Cache<Integer, FactPurchase> factCache) throws IgniteException {
        for (int i = 0; i < 100; i++) {
            int id = idGen++;

            DimStore store = rand(dataStore.values());
            DimProduct prod = rand(dataProduct.values());

            factCache.put(id, new FactPurchase(id, prod.getId(), store.getId(), (i + 1)));
        }
    }

    /**
     * Query all purchases made at a specific store. This query uses cross-cache joins
     * between {@link DimStore} objects stored in {@code 'replicated'} cache and
     * {@link FactPurchase} objects stored in {@code 'partitioned'} cache.
     *
     * @throws IgniteException If failed.
     */
    private static void queryStorePurchases() {
        IgniteCache<Integer, FactPurchase> factCache = Ignition.ignite().cache(FACT_CACHE_NAME);

        // All purchases for store1.
        // ========================

        // Create cross cache query to get all purchases made at store1.
        QueryCursor<List<?>> storePurchases = factCache.query(new SqlFieldsQuery(
            "select fp.* from \"" + DIM_STORE_CACHE_NAME + "\".DimStore, \"" + FACT_CACHE_NAME + "\".FactPurchase as fp "
                + "where DimStore.id=fp.storeId and DimStore.name=?").setArgs("Store1"));

        printQueryResults("All purchases made at store1:", storePurchases.getAll());
    }

    /**
     * Query all purchases made at a specific store for 3 specific products.
     * This query uses cross-cache joins between {@link DimStore}, {@link DimProduct}
     * objects stored in {@code 'replicated'} cache and {@link FactPurchase} objects
     * stored in {@code 'partitioned'} cache.
     *
     * @throws IgniteException If failed.
     */
    private static void queryProductPurchases() {
        IgniteCache<Integer, FactPurchase> factCache = Ignition.ignite().cache(FACT_CACHE_NAME);

        // All purchases for certain product made at store2.
        // =================================================

        DimProduct p1 = rand(dataProduct.values());
        DimProduct p2 = rand(dataProduct.values());
        DimProduct p3 = rand(dataProduct.values());

        System.out.println("IDs of products [p1=" + p1.getId() + ", p2=" + p2.getId() + ", p3=" + p3.getId() + ']');

        // Create cross cache query to get all purchases made at store2
        // for specified products.
        QueryCursor<List<?>> prodPurchases = factCache.query(new SqlFieldsQuery(
            "select fp.* from \"" + DIM_STORE_CACHE_NAME + "\".DimStore, \"" + DIM_PROD_CACHE_NAME + "\".DimProduct, " +
                "\"" + FACT_CACHE_NAME + "\".FactPurchase as fp "
                + "where DimStore.id=fp.storeId and DimProduct.id=fp.productId "
                + "and DimStore.name=? and DimProduct.id in(?, ?, ?)")
            .setArgs("Store2", p1.getId(), p2.getId(), p3.getId()));

        printQueryResults("All purchases made at store2 for 3 specific products:", prodPurchases.getAll());
    }

    /**
     * Print query results.
     *
     * @param msg Initial message.
     * @param res Results to print.
     */
    private static void printQueryResults(String msg, Iterable<List<?>> res) {
        System.out.println(msg);

        for (List<?> row : res)
            System.out.println("    " + row.toString());
    }

    /**
     * Gets random value from given collection.
     *
     * @param c Input collection (no {@code null} and not emtpy).
     * @return Random value from the input collection.
     */
    @SuppressWarnings("UnusedDeclaration")
    private static <T> T rand(Collection<? extends T> c) {
        if (c == null)
            throw new IllegalArgumentException();

        int n = ThreadLocalRandom.current().nextInt(c.size());

        int i = 0;

        for (T t : c) {
            if (i++ == n)
                return t;
        }

        throw new ConcurrentModificationException();
    }
}
