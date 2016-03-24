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

package org.apache.ignite.examples.datagrid.store;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Scanner;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.integration.CacheLoaderException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.store.CacheLoadOnlyStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.examples.model.Person;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Example of how to load data from CSV file using {@link CacheLoadOnlyStoreAdapter}.
 * <p>
 * The adapter is intended to be used in cases when you need to pre-load a cache from text or file of any other format.
 * <p>
 * Remote nodes can be started with {@link ExampleNodeStartup} in another JVM which will
 * start node with {@code examples/config/example-ignite.xml} configuration.
 */
public class CacheLoadOnlyStoreExample {
    /** Cache name. */
    private static final String CACHE_NAME = CacheLoadOnlyStoreExample.class.getSimpleName();

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> CacheLoadOnlyStoreExample started.");

            ProductLoader productLoader = new ProductLoader("examples/src/main/resources/person.csv");

            productLoader.setThreadsCount(2);
            productLoader.setBatchSize(10);
            productLoader.setBatchQueueSize(1);

            try (IgniteCache<Long, Person> cache = ignite.getOrCreateCache(cacheConfiguration(productLoader))) {
                // load data.
                cache.loadCache(null);

                System.out.println(">>> Loaded number of items: " + cache.size(CachePeekMode.PRIMARY));

                System.out.println(">>> Data for the person by id1: " + cache.get(1L));
            }
            finally {
                // Distributed cache could be removed from cluster only by #destroyCache() call.
                ignite.destroyCache(CACHE_NAME);
            }
        }
    }

    /**
     * Creates cache configurations for the loader.
     *
     * @return {@link CacheConfiguration}.
     */
    private static CacheConfiguration cacheConfiguration(ProductLoader productLoader) {
        CacheConfiguration cacheCfg = new CacheConfiguration();

        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setName(CACHE_NAME);

        // provide the loader.
        cacheCfg.setCacheStoreFactory(new FactoryBuilder.SingletonFactory(productLoader));

        return cacheCfg;
    }

    /**
     * Csv data loader for product data.
     */
    private static class ProductLoader extends CacheLoadOnlyStoreAdapter<Long, Person, String> implements Serializable {
        /** Csv file name. */
        final String csvFileName;

        /** Constructor. */
        ProductLoader(String csvFileName) {
            this.csvFileName = csvFileName;
        }

        /** {@inheritDoc} */
        @Override protected Iterator<String> inputIterator(@Nullable Object... args) throws CacheLoaderException {
            final Scanner scanner;

            try {
                File path = IgniteUtils.resolveIgnitePath(csvFileName);

                if (path == null)
                    throw new CacheLoaderException("Failed to open the source file: " + csvFileName);

                scanner = new Scanner(path);

                scanner.useDelimiter("\\n");
            }
            catch (FileNotFoundException e) {
                throw new CacheLoaderException("Failed to open the source file " + csvFileName, e);
            }

            /**
             * Iterator for text input. The scanner is implicitly closed when there's nothing to scan.
             */
            return new Iterator<String>() {
                /** {@inheritDoc} */
                @Override public boolean hasNext() {
                    if (!scanner.hasNext()) {
                        scanner.close();

                        return false;
                    }

                    return true;
                }

                /** {@inheritDoc} */
                @Override public String next() {
                    if (!hasNext())
                        throw new NoSuchElementException();

                    return scanner.next();
                }

                /** {@inheritDoc} */
                @Override public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        /** {@inheritDoc} */
        @Nullable @Override protected IgniteBiTuple<Long, Person> parse(String rec, @Nullable Object... args) {
            String[] p = rec.split("\\s*,\\s*");
            return new T2<>(Long.valueOf(p[0]), new Person(Long.valueOf(p[0]), Long.valueOf(p[1]),
                p[2], p[3], Double.valueOf(p[4]), p[5].trim()));
        }
    }
}
