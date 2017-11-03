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

package org.apache.ignite.examples.persistentstore;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.datagrid.CacheQueryExample;
import org.apache.ignite.examples.model.Organization;

/**
 * This example demonstrates the usage of Apache Ignite Persistent Store.
 * <p>
 * To execute this example you should start an instance of {@link PersistentStoreExampleNodeStartup}
 * class which will start up an Apache Ignite remote server node with a proper configuration.
 * <p>
 * When {@code UPDATE} parameter of this example is set to {@code true}, the example will populate
 * the cache with some data and will then run a sample SQL query to fetch some results.
 * <p>
 * When {@code UPDATE} parameter of this example is set to {@code false}, the example will run
 * the SQL query against the cache without the initial data pre-loading from the store.
 * <p>
 * You can populate the cache first with {@code UPDATE} set to {@code true}, then restart the nodes and
 * run the example with {@code UPDATE} set to {@code false} to verify that Apache Ignite can work with the
 * data that is in the persistence only.
 */
public class PersistentStoreExample {
    /** Organizations cache name. */
    private static final String ORG_CACHE = CacheQueryExample.class.getSimpleName() + "Organizations";

    /** */
    private static final boolean UPDATE = true;

    /**
     * @param args Program arguments, ignored.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        Ignition.setClientMode(true);

        try (Ignite ignite = Ignition.start("examples/config/persistentstore/example-persistent-store.xml")) {
            // Activate the cluster. Required to do if the persistent store is enabled because you might need
            // to wait while all the nodes, that store a subset of data on disk, join the cluster.
            ignite.active(true);

            CacheConfiguration<Long, Organization> cacheCfg = new CacheConfiguration<>(ORG_CACHE);

            cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            cacheCfg.setBackups(1);
            cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
            cacheCfg.setIndexedTypes(Long.class, Organization.class);

            IgniteCache<Long, Organization> cache = ignite.getOrCreateCache(cacheCfg);

            if (UPDATE) {
                System.out.println("Populating the cache...");

                try (IgniteDataStreamer<Long, Organization> streamer = ignite.dataStreamer(ORG_CACHE)) {
                    streamer.allowOverwrite(true);

                    for (long i = 0; i < 100_000; i++) {
                        streamer.addData(i, new Organization(i, "organization-" + i));

                        if (i > 0 && i % 10_000 == 0)
                            System.out.println("Done: " + i);
                    }
                }
            }

            // Run SQL without explicitly calling to loadCache().
            QueryCursor<List<?>> cur = cache.query(
                new SqlFieldsQuery("select id, name from Organization where name like ?")
                    .setArgs("organization-54321"));

            System.out.println("SQL Result: " + cur.getAll());

            // Run get() without explicitly calling to loadCache().
            Organization org = cache.get(54321l);

            System.out.println("GET Result: " + org);
        }
    }
}
