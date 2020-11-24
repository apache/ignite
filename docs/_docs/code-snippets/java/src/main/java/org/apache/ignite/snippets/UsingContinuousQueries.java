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
package org.apache.ignite.snippets;

import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteClosure;

public class UsingContinuousQueries {

    public static void runAll() {
        initialQueryExample();
        localListenerExample();
        remoteFilterExample();
        remoteTransformerExample();
    }

    public static void initialQueryExample() {
        try (Ignite ignite = Ignition.start()) {

            //tag::initialQry[]
            IgniteCache<Integer, String> cache = ignite.getOrCreateCache("myCache");

            //end::initialQry[]
            cache.put(100, "100");

            //tag::initialQry[]
            ContinuousQuery<Integer, String> query = new ContinuousQuery<>();

            // Setting an optional initial query.
            // The query will return entries for the keys greater than 10.
            query.setInitialQuery(new ScanQuery<>((k, v) -> k > 10));

            //mandatory local listener
            query.setLocalListener(events -> {
            });

            try (QueryCursor<Cache.Entry<Integer, String>> cursor = cache.query(query)) {
                // Iterating over the entries returned by the initial query 
                for (Cache.Entry<Integer, String> e : cursor)
                    System.out.println("key=" + e.getKey() + ", val=" + e.getValue());
            }
            //end::initialQry[]
        }
    }

    public static void localListenerExample() {
        try (Ignite ignite = Ignition.start()) {

            //tag::localListener[]
            IgniteCache<Integer, String> cache = ignite.getOrCreateCache("myCache");

            ContinuousQuery<Integer, String> query = new ContinuousQuery<>();

            query.setLocalListener(new CacheEntryUpdatedListener<Integer, String>() {

                @Override
                public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends String>> events)
                    throws CacheEntryListenerException {
                    // react to the update events here
                }
            });

            cache.query(query);

            //end::localListener[]
        }
    }

    public static void remoteFilterExample() {
        try (Ignite ignite = Ignition.start()) {

            IgniteCache<Integer, String> cache = ignite.getOrCreateCache("myCache");

            //tag::remoteFilter[]
            ContinuousQuery<Integer, String> qry = new ContinuousQuery<>();

            qry.setLocalListener(events ->
                events.forEach(event -> System.out.format("Entry: key=[%s] value=[%s]\n", event.getKey(), event.getValue()))
            );

            qry.setRemoteFilterFactory(new Factory<CacheEntryEventFilter<Integer, String>>() {
                @Override
                public CacheEntryEventFilter<Integer, String> create() {
                    return new CacheEntryEventFilter<Integer, String>() {
                        @Override
                        public boolean evaluate(CacheEntryEvent<? extends Integer, ? extends String> e) {
                            System.out.format("the value for key [%s] was updated from [%s] to [%s]\n", e.getKey(), e.getOldValue(), e.getValue());
                            return true;
                        }
                    };
                }
            });

            //end::remoteFilter[]
            cache.query(qry);
            cache.put(1, "1");

        }
    }

    public static void remoteTransformerExample() {
        try (Ignite ignite = Ignition.start()) {

            //tag::transformer[]
            IgniteCache<Integer, Person> cache = ignite.getOrCreateCache("myCache");

            // Create a new continuous query with a transformer.
            ContinuousQueryWithTransformer<Integer, Person, String> qry = new ContinuousQueryWithTransformer<>();

            // Factory to create transformers.
            Factory factory = FactoryBuilder.factoryOf(
                // Return one field of a complex object.
                // Only this field will be sent over to the local listener.
                (IgniteClosure<CacheEntryEvent, String>)
                    event -> ((Person)event.getValue()).getName()
            );

            qry.setRemoteTransformerFactory(factory);

            // Listener that will receive transformed data.
            qry.setLocalListener(names -> {
                for (String name : names)
                    System.out.println("New person name: " + name);
            });
            //end::transformer[]

            cache.query(qry);
            cache.put(1, new Person(1, "Vasya"));
        }
    }
}
