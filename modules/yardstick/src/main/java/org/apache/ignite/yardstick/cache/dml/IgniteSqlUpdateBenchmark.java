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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.yardstick.cache.IgniteCacheAbstractBenchmark;
import org.apache.ignite.yardstick.cache.model.Person1;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

/**
 * Ignite benchmark that performs SQL UPDATE operations.
 */
public class IgniteSqlUpdateBenchmark extends IgniteCacheAbstractBenchmark<Integer, Object> {
    /** {@inheritDoc} */
    @Override public void setUp(final BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        final AtomicInteger i = new AtomicInteger();

        Collection<Thread> setupThreads = new ArrayList<>(cfg.threads());

        for (int j = 0; j < cfg.threads(); j++) {
            Thread t = new Thread() {
                /** {@inheritDoc} */
                @Override public void run() {
                    int k;

                    while ((k = i.getAndIncrement()) < args.range()) {
                        cache().put(k, new Person1(k));
                        if (++k % 100000 == 0)
                            BenchmarkUtils.println(cfg, "UPDATE setUp: have successfully put " + k + " items");
                    }
                }
            };

            setupThreads.add(t);

            t.start();
        }

        for (Thread t : setupThreads)
            t.join();
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        cache().query(new SqlFieldsQuery("update Person1 set _val = ? where _key = ?")
            .setArgs(new Person1(rnd.nextInt(args.range())), rnd.nextInt(args.range())));

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("atomic-index");
    }
}

