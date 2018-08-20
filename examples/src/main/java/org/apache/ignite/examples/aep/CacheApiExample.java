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
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.examples.model.Person;
import javax.cache.Cache;
import java.util.Iterator;


/**
 * This example demonstrates some of the cache rich API capabilities.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will
 * start node with {@code examples/config/example-ignite.xml} configuration.
 */
public class CacheApiExample {
    /** Cache name. */
    private static final String CACHE_NAME = CacheApiExample.class.getSimpleName();

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {

        Ignition.setAepStore("/mnt/mem/" + CacheApiExample.class.getSimpleName());

        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Cache API example started.");

            CacheConfiguration<Integer, Person> cacheCfg = new CacheConfiguration<>(CACHE_NAME);

            // Auto-close cache at the end of the example.
            try (IgniteCache<Integer, Person> cache = ignite.getOrCreateCache(cacheCfg)) {

                if (true) {
                    cache.put(1, new Person(1L, "Alan", "Turing"));
                    cache.put(2, new Person(2L, "John", "Backus"));
                    cache.put(3, new Person(3L, "Donald", "Knuth"));
                }

                Iterator<Cache.Entry<Integer, Person>> itr = cache.iterator();
                while (itr.hasNext())
                    Ignition.print(itr.next().getValue().toString());
            }
            finally {
                // Distributed cache could be removed from cluster only by #destroyCache() call.
                ignite.destroyCache(CACHE_NAME);
            }
        }
    }
}

