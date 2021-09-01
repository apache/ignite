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

package org.apache.ignite.yardstick.cache.dml;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.yardstick.cache.IgniteCacheAbstractBenchmark;
import org.apache.ignite.yardstick.cache.model.Person;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Ignite benchmark that performs SQL MERGE and query operations.
 */
public class IgniteSqlMergeQueryBenchmark extends IgniteCacheAbstractBenchmark<Integer, Object> {
    /** */
    private AtomicInteger putCnt = new AtomicInteger();

    /** */
    private AtomicInteger qryCnt = new AtomicInteger();

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        if (rnd.nextBoolean()) {
            double salary = rnd.nextDouble() * args.range() * 1000;

            double maxSalary = salary + 1000;

            Collection<Cache.Entry<Integer, Object>> entries = executeQuery(salary, maxSalary);

            for (Cache.Entry<Integer, Object> entry : entries) {
                Object o = entry.getValue();

                double s = o instanceof Person ? ((Person) o).getSalary() : ((BinaryObject) o).<Double>field("salary");

                if (s < salary || s > maxSalary)
                    throw new Exception("Invalid person retrieved [min=" + salary + ", max=" + maxSalary +
                            ", person=" + o + ']');
            }

            qryCnt.getAndIncrement();
        }
        else {
            int i = rnd.nextInt(args.range());

            cache.query(new SqlFieldsQuery("merge into Person(_key, id, firstName, lastName, salary) " +
                "values (?, ?, ?, ?, ?)").setArgs(i, i, "firstName" + i, "lastName" + i, (double) i * 1000));

            putCnt.getAndIncrement();
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public void onWarmupFinished() {
        super.onWarmupFinished();
    }

    /**
     * @param minSalary Min salary.
     * @param maxSalary Max salary.
     * @return Query result.
     * @throws Exception If failed.
     */
    private Collection<Cache.Entry<Integer, Object>> executeQuery(double minSalary, double maxSalary) throws Exception {
        SqlQuery qry = new SqlQuery(Person.class, "salary >= ? and salary <= ?");

        qry.setArgs(minSalary, maxSalary);

        return cache.query(qry).getAll();
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("query").withKeepBinary();
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        println(cfg, "Finished sql query put benchmark [putCnt=" + putCnt.get() + ", qryCnt=" + qryCnt.get() + ']');

        super.tearDown();
    }
}

