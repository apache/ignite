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

package org.apache.ignite.examples.aep;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.datagrid.CacheQueryExample;
import org.apache.ignite.examples.model.Organization;
import org.apache.ignite.examples.model.Person;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;

import javax.cache.Cache;
import java.util.Iterator;
import java.util.List;

/**
 * This example demonstrates the usage of Apache Ignite Persistent Store.
 * <p>
 * To execute this example you should start an instance of {@link CacheDataStoreExample}
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
public class CacheDataStoreExample {
    /** Organizations cache name. */
    private static final String ORG_CACHE = CacheQueryExample.class.getSimpleName() + "Organizations";

    /**
     * @param args Program arguments, ignored.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {

        Ignition.setAepStore("/mnt/mem/" + CacheDataStoreExample.class.getSimpleName());

        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {

            CacheConfiguration<Long, Organization> cacheCfg = new CacheConfiguration<>(ORG_CACHE);

            cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
            cacheCfg.setIndexedTypes(Long.class, Organization.class);

            IgniteCache<Long, Organization> cache = ignite.getOrCreateCache(cacheCfg);

            if (true) {
                Ignition.print(">>> Populating the cache...");
                for (long i = 0; i < 10; i++)
                    cache.put(i, new Organization(i, "organization-" + i));
            }

            QueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery("select id,name from Organization"));

            Ignition.print(">>> SQL result:");
            Iterator<List<?>> itr = cur.iterator();
            while (itr.hasNext())
                Ignition.print(itr.next().toString());
        }
    }
}
