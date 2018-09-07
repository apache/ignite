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

import org.apache.ignite.*;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.examples.datagrid.CacheQueryExample;
import org.apache.ignite.examples.model.Organization;

/**
 * This example demonstrates some of the cache rich API capabilities.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will
 * start node with {@code examples/config/example-ignite.xml} configuration.
 */
public class AepStoreClientExample {
    /** Organizations cache name. */
    private static final String ORG_CACHE = CacheQueryExample.class.getSimpleName() + "Organizations";

    /** */
    private static final boolean UPDATE = true;

    /**
     * @param args Program arguments, ignored.
     * @throws Exception If failed.
     */
    public static void main(String[] args) {
        Ignition.setClientMode(true);
        Ignition.setAepStore(args[0], true);

        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            // Activate the cluster. Required to do if the persistent store is enabled because you might need
            // to wait while all the nodes, that store a subset of data on disk, join the cluster.
            ignite.active(true);

            CacheConfiguration<Long, Organization> cacheCfg = new CacheConfiguration<>(ORG_CACHE);
            cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
            IgniteCache<Long, Organization> cache = ignite.getOrCreateCache(cacheCfg);

            if (UPDATE) {
                System.out.println("Populating the cache...");

                try (IgniteDataStreamer<Long, Organization> streamer = ignite.dataStreamer(ORG_CACHE)) {
                    streamer.allowOverwrite(true);

                    for (long i = 0; i < Long.parseLong(args[1]); i++) {
                        streamer.addData(i, new Organization(i, "organization-" + i));

                        if (i > 0 && i % 10_000 == 0)
                            System.out.println("Done: " + i);
                    }
                }
            }

            // Run get() without explicitly calling to loadCache().
            Organization org = cache.get(54321l);

            System.out.println("GET Result: " + org);
        }
    }
}
