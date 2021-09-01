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

package org.apache.ignite.loadtests.h2indexing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;

/**
 * SQL query stress test.
 */
public class FetchingQueryCursorStressTest {
    /** Node count. */
    private static final int NODE_CNT = 4; // Switch to 4 to see better throughput.

    /** Number of entries. */
    private static final int ENTRIES_CNT = 10_000;

    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** Thread count. */
    private static final int THREAD_CNT = 16;

    /** Execution counter. */
    private static final AtomicLong CNT = new AtomicLong();

    /** Verbose mode. */
    private static final boolean VERBOSE = false;

    /** */
    private static final long TIMEOUT = TimeUnit.SECONDS.toMillis(30);

    public static final AtomicReference<Exception> error = new AtomicReference<>();

    /**
     * Entry point.
     */
    public static void main(String[] args) throws Exception {
        List<Thread> threads = new ArrayList<>(THREAD_CNT + 1);

        try (Ignite ignite = start()) {

            IgniteCache<Integer, Person> cache = ignite.cache(CACHE_NAME);

            loadData(ignite, cache);

            System.out.println("Loaded data: " + cache.size());

            for (int i = 0; i < THREAD_CNT; i++)
                threads.add(startDaemon("qry-exec-" + i, new QueryExecutor(cache, "Select * from Person")));

            threads.add(startDaemon("printer", new ThroughputPrinter()));

            Thread.sleep(TIMEOUT);

            for (Thread t : threads)
                t.join();

            if (error.get() != null)
                throw error.get();
        }
        finally {
            Ignition.stopAll(false);
        }
    }

    /**
     * Start daemon thread.
     *
     * @param name Name.
     * @param r Runnable.
     */
    private static Thread startDaemon(String name, Runnable r) {
        Thread t = new Thread(r);

        t.setName(name);
        t.setDaemon(true);

        t.start();

        return t;
    }

    /**
     * Load data into Ignite.
     *
     * @param ignite Ignite.
     * @param cache Cache.
     */
    private static void loadData(Ignite ignite, IgniteCache<Integer, Person> cache) throws Exception {
        try (IgniteDataStreamer<Object, Object> str = ignite.dataStreamer(cache.getName())) {

            for (int id = 0; id < ENTRIES_CNT; id++)
                str.addData(id, new Person(id, "John" + id, "Doe"));
        }
    }

    /**
     * Start topology.
     *
     * @return Client node.
     */
    private static Ignite start() {
        int i = 0;

        for (; i < NODE_CNT; i++)
            Ignition.start(config(i, false));

        return Ignition.start(config(i, true));
    }

    /**
     * Create configuration.
     *
     * @param idx Index.
     * @param client Client flag.
     * @return Configuration.
     */
    @SuppressWarnings("unchecked")
    private static IgniteConfiguration config(int idx, boolean client) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName("grid-" + idx);
        cfg.setClientMode(client);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(CACHE_NAME);
        ccfg.setIndexedTypes(Integer.class, Person.class);
        cfg.setMarshaller(new BinaryMarshaller());

        cfg.setCacheConfiguration(ccfg);

        cfg.setLocalHost("127.0.0.1");

        return cfg;
    }

    /**
     *
     */
    private static class Person implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @QuerySqlField
        private int id;

        /** */
        @QuerySqlField
        private String firstName;

        /** */
        @QuerySqlField
        private String lastName;

        public Person(int id, String firstName, String lastName) {
            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
        }
    }

    /**
     * Query runner.
     */
    private static class QueryExecutor implements Runnable {
        /** Cache. */
        private final IgniteCache<Integer, Person> cache;

        /** */
        private final String query;

        /**
         * Constructor.
         *
         * @param cache Cache.
         */
        public QueryExecutor(IgniteCache<Integer, Person> cache, String query) {
            this.cache = cache;
            this.query = query;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            System.out.println("Executor started: " + Thread.currentThread().getName());

            try {
                while (error.get() == null && !Thread.currentThread().isInterrupted()) {
                    long start = System.nanoTime();

                    SqlFieldsQuery qry = new SqlFieldsQuery(query);

//                qry.setArgs((Object[]) argumentForQuery());

                    Set<Integer> extIds = new HashSet<>();

                    for (List<?> next : cache.query(qry))
                        extIds.add((Integer)next.get(0));

                    long dur = (System.nanoTime() - start) / 1_000_000;

                    CNT.incrementAndGet();

                    if (VERBOSE)
                        System.out.println("[extIds=" + extIds.size() + ", dur=" + dur + ']');
                }
            }
            catch (CacheException ex) {
                error.compareAndSet(null, ex);
            }
        }
    }

    /**
     * Throughput printer.
     */
    private static class ThroughputPrinter implements Runnable {
        /** {@inheritDoc} */
        @Override public void run() {
            while (error.get() == null) {
                long before = CNT.get();
                long beforeTime = System.currentTimeMillis();

                try {
                    Thread.sleep(2000L);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    return;
                }

                long after = CNT.get();
                long afterTime = System.currentTimeMillis();

                double res = 1000 * ((double)(after - before)) / (afterTime - beforeTime);

                System.out.println((long)res + " ops/sec");
            }
        }
    }
}
