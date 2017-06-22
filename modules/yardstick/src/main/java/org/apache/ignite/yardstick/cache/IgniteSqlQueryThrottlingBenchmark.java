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

package org.apache.ignite.yardstick.cache;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.yardstick.cache.model.Person;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Ignite benchmark that performs identical query operations with occasional puts to measure effect of query throttling.
 */
public class IgniteSqlQueryThrottlingBenchmark extends IgniteCacheAbstractBenchmark<Integer, Person> {
    /** */
    private final AtomicInteger putCnt = new AtomicInteger();

    /** */
    private final AtomicInteger qryCnt = new AtomicInteger();

    /** Min salary to query. */
    private int minSalary;

    /** Max salary to query. */
    private int maxSalary;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        println(cfg, "Populating query data...");

        long start = System.nanoTime();

        try (IgniteDataStreamer<Integer, Person> dataLdr = ignite().dataStreamer(cache.getName())) {
            for (int i = 0; i < args.range() && !Thread.currentThread().isInterrupted(); i++) {
                dataLdr.addData(i, person(i));

                if (i % 100000 == 0)
                    println(cfg, "Populated persons: " + i);
            }
        }

        println(cfg, "Finished populating query data in " + ((System.nanoTime() - start) / 1_000_000) + " ms.");

        maxSalary = args.range() * 1000;

        minSalary = (int)(maxSalary * 0.15);

        maxSalary = (int)(maxSalary * 0.85);
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        // Let's make puts 20% of all operations.
        boolean put = (ThreadLocalRandom.current().nextInt(5) == 0);

        if (put) {
            cache().put(ThreadLocalRandom.current().nextInt(args.range),
                person(ThreadLocalRandom.current().nextInt(args.range)));

            putCnt.incrementAndGet();

            return true;
        }

        Collection<Cache.Entry<Integer, Person>> entries = executeQuery(minSalary, maxSalary);

        for (Cache.Entry<Integer, Person> entry : entries) {
            Person p = entry.getValue();

            if (p.getSalary() < minSalary || p.getSalary() > maxSalary)
                throw new Exception("Invalid person retrieved [min=" + minSalary + ", max=" + maxSalary +
                    ", person=" + p + ']');
        }

        qryCnt.incrementAndGet();

        return true;
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        println(cfg, "SQL query throttling benchmark has finished [putCnt=" + putCnt.get() + ", qryCnt=" + qryCnt.get() +
            ']');

        super.tearDown();
    }

    /**
     * @param minSalary Min salary.
     * @param maxSalary Max salary.
     * @return Query result.
     * @throws Exception If failed.
     */
    private Collection<Cache.Entry<Integer, Person>> executeQuery(double minSalary, double maxSalary) throws Exception {
        SqlQuery<Integer, Person> qry = new SqlQuery<>(Person.class, "salary >= ? and salary <= ?");

        qry.setArgs(minSalary, maxSalary);

        return cache.query(qry).getAll();
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Person> cache() {
        return ignite().cache("query");
    }

    /**
     * A {@link Person} with field values derived from param value.
     */
    private static Person person(int p) {
        return new Person(p, "firstName" + p, "lastName" + p, p * 1000);
    }
}
