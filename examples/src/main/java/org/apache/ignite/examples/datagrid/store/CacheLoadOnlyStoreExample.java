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
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Scanner;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.integration.CacheLoaderException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.store.CacheLoadOnlyStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Example of how to load data from CSV file.
 *
 * Note that it aims to illustrate the use of {@link CacheLoadOnlyStoreAdapter}
 * and does not contains thorough error checking and validation.
 */
public class CacheLoadOnlyStoreExample {
    /** Cache name. */
    private static final String CACHE_NAME = CacheLoadOnlyStoreExample.class.getSimpleName();

    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> CacheLoadOnlyStoreExample started.");

            ProductLoader productLoader = new ProductLoader("product.csv");

            productLoader.setThreadsCount(2);
            productLoader.setBatchSize(10);
            productLoader.setBatchQueueSize(1);

            try (IgniteCache<Integer, String> cache = ignite.getOrCreateCache(cacheConfiguration(productLoader))) {
                // load data.
                cache.loadCache(null);

                System.out.println(">>> Loaded number of items: " + cache.size(CachePeekMode.PRIMARY));
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

        cacheCfg.setReadThrough(true);
        cacheCfg.setWriteThrough(true);
        cacheCfg.setLoadPreviousValue(true);

        // provide the loader.
        cacheCfg.setCacheStoreFactory(new FactoryBuilder.SingletonFactory(productLoader));

        return cacheCfg;
    }

    /**
     * Csv data loader for product data.
     */
    private static class ProductLoader extends CacheLoadOnlyStoreAdapter<String, Product, String> implements Serializable {
        /** Csv file name. */
        final String csvFileName;

        /** Constructor. */
        ProductLoader(String csvFileName) {
            this.csvFileName = csvFileName;
        }

        /** {@inheritDoc} */
        @Override protected Iterator<String> inputIterator(@Nullable Object... args) throws CacheLoaderException {
            Scanner scanner;

            try {
                scanner = new Scanner(new File(this.getClass().getClassLoader().getResource(csvFileName).getFile()));

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
            };
        }

        /** {@inheritDoc} */
        @Nullable @Override protected IgniteBiTuple<String, Product> parse(String rec, @Nullable Object... args) {
            String[] p = rec.split("\\s*,\\s*");
            return new T2<>(p[0], new Product(p[0], p[1], p[2], p[3], new BigDecimal(p[4])));
        }
    }

    /**
     * Sample product class.
     */
    private static class Product {
        /** Product id. */
        private String id;

        /** Title. */
        private String title;

        /** Description. */
        private String desc;

        /** Vendor. */
        private String vendor;

        /** Price. */
        private BigDecimal price;

        public Product(String id, String title, String desc, String vendor, BigDecimal price) {
            this.id = id;
            this.title = title;
            this.desc = desc;
            this.vendor = vendor;
            this.price = price;
        }

        public String getId() {
            return id;
        }

        public String getTitle() {
            return title;
        }

        public String getDesc() {
            return desc;
        }

        public String getVendor() {
            return vendor;
        }

        public BigDecimal getPrice() {
            return price;
        }
    }
}
