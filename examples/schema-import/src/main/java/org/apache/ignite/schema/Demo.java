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

package org.apache.ignite.schema;

import org.apache.ignite.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.cache.store.jdbc.*;
import org.apache.ignite.configuration.*;

import javax.cache.*;
import javax.cache.configuration.*;

/**
 * Demo for CacheJdbcPojoStore.
 *
 * This example demonstrates the use of cache with {@link CacheJdbcPojoStore}.
 *
 * Custom SQL will be executed to populate cache with data from database.
 */
public class Demo {
    /**
     * Executes demo.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        System.out.println(">>> Start demo...");

        IgniteConfiguration cfg = new IgniteConfiguration();

        CacheConfiguration ccfg = new CacheConfiguration<>();

        // Configure cache store.
        ccfg.setCacheStoreFactory(new Factory<CacheStore>() {
            @Override public CacheStore create() {
                return CacheConfig.store();
            }
        });

        ccfg.setReadThrough(true);
        ccfg.setWriteThrough(true);

        // Enable database batching.
        ccfg.setWriteBehindEnabled(true);

        // Configure cache types metadata.
        ccfg.setTypeMetadata(CacheConfig.typeMetadata());

        cfg.setCacheConfiguration(ccfg);

        // Start Ignite node.
        try (Ignite ignite = Ignition.start(cfg)) {
            IgniteCache<PersonKey, Person> cache = ignite.jcache(null);

            // Demo for load cache with custom SQL.
            cache.loadCache(null, "org.apache.ignite.schema.PersonKey",
                "select * from PERSON where ID <= 3");

            for (Cache.Entry<PersonKey, Person> person : cache)
                System.out.println(">>> Loaded Person: " + person);
        }
    }
}
