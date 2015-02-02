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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.examples.datagrid.*;
import org.apache.ignite.internal.processors.cache.query.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * <a href="http://en.wikipedia.org/wiki/Snowflake_schema">Snowflake Schema</a> is a logical
 * arrangement of data in which data is split into {@code dimensions} and {@code facts}.
 * <i>Dimensions</i> can be referenced or joined by other <i>dimensions</i> or <i>facts</i>,
 * however, <i>facts</i> are generally not referenced by other facts. You can view <i>dimensions</i>
 * as your master or reference data, while <i>facts</i> are usually large data sets of events or
 * other objects that continuously come into the system and may change frequently. In GridGain
 * such architecture is supported via cross-cache queries. By storing <i>dimensions</i> in
 * {@link org.apache.ignite.cache.CacheMode#REPLICATED REPLICATED} caches and <i>facts</i> in much larger
 * {@link org.apache.ignite.cache.CacheMode#PARTITIONED PARTITIONED} caches you can freely execute distributed joins across
 * your whole in-memory data grid, thus querying your in memory data without any limitations.
 * <p>
 * In this example we have two <i>dimensions</i>, {@link DimProduct} and {@link DimStore} and
 * one <i>fact</i> - {@link FactPurchase}. Queries are executed by joining dimensions and facts
 * in various ways.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-cache.xml'}.
 * <p>
 * Alternatively you can run {@link CacheNodeStartup} in another JVM which will
 * start GridGain node with {@code examples/config/example-cache.xml} configuration.
 */
public class CacheStarSchemaExample {
    /** Partitioned cache name. */
    private static final String PARTITIONED_CACHE_NAME = "partitioned";

    /** Replicated cache name. */
    private static final String REPLICATED_CACHE_NAME = "replicated";

    /** ID generator. */
    private static int idGen = (int)System.currentTimeMillis();

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteCheckedException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        Ignite g = Ignition.start("examples/config/example-cache.xml");

        System.out.println();
        System.out.println(">>> Cache star schema example started.");

        // Clean up caches on all nodes before run.
        g.cache(PARTITIONED_CACHE_NAME).globalClearAll(0);
        g.cache(REPLICATED_CACHE_NAME).globalClearAll(0);

        try {
            populateDimensions();
            populateFacts();

            queryStorePurchases();
            queryProductPurchases();
        }
        finally {
            Ignition.stop(false);
        }
    }

    /**
     * Populate cache with {@code 'dimensions'} which in our case are
     * {@link DimStore} and {@link DimProduct} instances.
     *
     * @throws IgniteCheckedException If failed.
     */
    private static void populateDimensions() throws IgniteCheckedException {
        GridCache<Integer, Object> cache = Ignition.ignite().cache(REPLICATED_CACHE_NAME);

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
     * @throws IgniteCheckedException If failed.
     */
    private static void populateFacts() throws IgniteCheckedException {
        GridCache<Integer, Object> dimCache = Ignition.ignite().cache(REPLICATED_CACHE_NAME);
        GridCache<Integer, Object> factCache = Ignition.ignite().cache(PARTITIONED_CACHE_NAME);

        CacheProjection<Integer, DimStore> stores = dimCache.projection(Integer.class, DimStore.class);
        CacheProjection<Integer, DimProduct> prods = dimCache.projection(Integer.class, DimProduct.class);

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
     * @throws IgniteCheckedException If failed.
     */
    private static void queryStorePurchases() throws IgniteCheckedException {
        GridCache<Integer, FactPurchase> factCache = Ignition.ignite().cache(PARTITIONED_CACHE_NAME);

        // All purchases for store1.
        // ========================

        // Create cross cache query to get all purchases made at store1.
        CacheQuery<Map.Entry<Integer, FactPurchase>> storePurchases = factCache.queries().createSqlQuery(
            FactPurchase.class,
            "from \"replicated\".DimStore, \"partitioned\".FactPurchase " +
                "where DimStore.id=FactPurchase.storeId and DimStore.name=?");

        printQueryResults("All purchases made at store1:",
            storePurchases.execute("Store1").get());
    }

    /**
     * Query all purchases made at a specific store for 3 specific products.
     * This query uses cross-cache joins between {@link DimStore}, {@link DimProduct}
     * objects stored in {@code 'replicated'} cache and {@link FactPurchase} objects
     * stored in {@code 'partitioned'} cache.
     *
     * @throws IgniteCheckedException If failed.
     */
    private static void queryProductPurchases() throws IgniteCheckedException {
        GridCache<Integer, Object> dimCache = Ignition.ignite().cache(REPLICATED_CACHE_NAME);
        GridCache<Integer, FactPurchase> factCache = Ignition.ignite().cache(PARTITIONED_CACHE_NAME);

        CacheProjection<Integer, DimProduct> prods = dimCache.projection(Integer.class, DimProduct.class);

        // All purchases for certain product made at store2.
        // =================================================

        DimProduct p1 = rand(prods.values());
        DimProduct p2 = rand(prods.values());
        DimProduct p3 = rand(prods.values());

        System.out.println("IDs of products [p1=" + p1.getId() + ", p2=" + p2.getId() + ", p3=" + p3.getId() + ']');

        // Create cross cache query to get all purchases made at store2
        // for specified products.
        CacheQuery<Map.Entry<Integer, FactPurchase>> prodPurchases = factCache.queries().createSqlQuery(
            FactPurchase.class,
            "from \"replicated\".DimStore, \"replicated\".DimProduct, \"partitioned\".FactPurchase " +
                "where DimStore.id=FactPurchase.storeId and DimProduct.id=FactPurchase.productId " +
                "and DimStore.name=? and DimProduct.id in(?, ?, ?)");

        printQueryResults("All purchases made at store2 for 3 specific products:",
            prodPurchases.execute("Store2", p1.getId(), p2.getId(), p3.getId()).get());
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
