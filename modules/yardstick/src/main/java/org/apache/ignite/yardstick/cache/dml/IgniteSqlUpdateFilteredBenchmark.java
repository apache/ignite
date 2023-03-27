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

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.yardstick.cache.IgniteCacheAbstractBenchmark;
import org.apache.ignite.yardstick.cache.model.Person;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Ignite benchmark that performs put and SQL UPDATE operations.
 */
public class IgniteSqlUpdateFilteredBenchmark extends IgniteCacheAbstractBenchmark<Integer, Object> {
    /** */
    private AtomicInteger putCnt = new AtomicInteger();

    /** */
    private AtomicInteger updCnt = new AtomicInteger();

    /** */
    private AtomicLong updItemsCnt = new AtomicLong();

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

            Long res = (Long)cache().query(new SqlFieldsQuery("update Person set salary = (salary - ?1 + ?2) / 2 " +
                    "where salary >= ?1 and salary <= ?2").setArgs(salary, maxSalary)).getAll().get(0).get(0);

            updItemsCnt.getAndAdd(res);

            updCnt.getAndIncrement();
        }
        else {
            int i = rnd.nextInt(args.range());

            cache.put(i, new Person(i, "firstName" + i, "lastName" + i, i * 1000));

            putCnt.getAndIncrement();
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("query");
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        println(cfg, "Finished SQL UPDATE query benchmark [putCnt=" + putCnt.get() + ", updCnt=" + updCnt.get() +
                ", updItemsCnt=" + updItemsCnt.get() + ']');

        super.tearDown();
    }
}
