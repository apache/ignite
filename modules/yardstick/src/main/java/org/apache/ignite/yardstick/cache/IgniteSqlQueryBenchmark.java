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

import org.apache.ignite.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.yardstick.cache.model.*;
import org.yardstickframework.*;

import java.util.*;
import java.util.concurrent.*;

import static org.yardstickframework.BenchmarkUtils.*;

/**
 * Ignite benchmark that performs query operations.
 */
public class IgniteSqlQueryBenchmark extends IgniteCacheAbstractBenchmark {
    /** */
    private CacheQuery qry;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        println(cfg, "Populating query data...");

        long start = System.nanoTime();

        try (IgniteDataLoader<Integer, Person> dataLdr = ignite().dataLoader(cache.getName())) {
            for (int i = 0; i < args.range() && !Thread.currentThread().isInterrupted(); i++) {
                dataLdr.addData(i, new Person(i, "firstName" + i, "lastName" + i, i * 1000));

                if (i % 100000 == 0)
                    println(cfg, "Populated persons: " + i);
            }
        }

        println(cfg, "Finished populating query data in " + ((System.nanoTime() - start) / 1_000_000) + " ms.");

        qry = null; // TODO: should be fixed after IGNITE-2 cache.queries().createSqlQuery(Person.class, "salary >= ? and salary <= ?");
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        double salary = ThreadLocalRandom.current().nextDouble() * args.range() * 1000;

        double maxSalary = salary + 1000;

        Collection<Map.Entry<Integer, Person>> entries = executeQuery(salary, maxSalary);

        for (Map.Entry<Integer, Person> entry : entries) {
            Person p = entry.getValue();

            if (p.getSalary() < salary || p.getSalary() > maxSalary)
                throw new Exception("Invalid person retrieved [min=" + salary + ", max=" + maxSalary +
                        ", person=" + p + ']');
        }

        return true;
    }

    /**
     * @param minSalary Min salary.
     * @param maxSalary Max salary.
     * @return Query result.
     * @throws Exception If failed.
     */
    private Collection<Map.Entry<Integer, Person>> executeQuery(double minSalary, double maxSalary) throws Exception {
        CacheQuery<Map.Entry<Integer, Person>> q = (CacheQuery<Map.Entry<Integer, Person>>)qry;

        return q.execute(minSalary, maxSalary).get();
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().jcache("query");
    }
}
