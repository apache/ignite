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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.yardstick.cache.model.Person;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Ignite benchmark that performs identical query operations with occasional puts to measure effect
 * of query results reuse.
 */
public class IgniteSqlQueryResultsReuseBenchmark extends IgniteCacheAbstractBenchmark<Integer, Person> {
    /** Timeout for this client to use to make cache puts. */
    private int putTimeoutMs = DFLT_PUT_TIMEOUT_MS;

    /** Time after which one of this client's threads is allowed to do a cache put. */
    private volatile long nextPutTime;

    /** */
    private final AtomicInteger putCnt = new AtomicInteger();

    /** */
    private final AtomicInteger qryCnt = new AtomicInteger();

    /** Min salary to query. */
    private int minSalary;

    /** Max salary to query. */
    private int maxSalary;

    /** Flag to synchronize puts (one thread gets to make an actual put within a timeout). */
    private final AtomicBoolean putFlag = new AtomicBoolean();

    /** Default put timeout. */
    private final static int DFLT_PUT_TIMEOUT_MS = 500;

    /** Property name for put timeout. */
    private final static String PUT_TIMEOUT_PROP_NAME = "PUT_TIMEOUT";

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        String timeoutProp = cfg.customProperties().get(PUT_TIMEOUT_PROP_NAME);

        if (!F.isEmpty(timeoutProp)) {
            putTimeoutMs = Integer.parseInt(timeoutProp);

            A.ensure(putTimeoutMs > 0, PUT_TIMEOUT_PROP_NAME);

            println(cfg, "Using custom PUT timeout: " + putTimeoutMs);
        }

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
        // Let's avoid locking by using CAS here.
        boolean put = putFlag.compareAndSet(false, true);

        if (put) {
            long curTime = System.currentTimeMillis();

            if (curTime > nextPutTime) {
                cache().put(nextRandom(args.range), person(nextRandom(args.range)));

                putCnt.incrementAndGet();

                nextPutTime = curTime + putTimeoutMs;

                putFlag.set(false);

                return true;
            }
            else // It's not the time to put yet, let's release the flag for others to try.
                putFlag.set(false);
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
