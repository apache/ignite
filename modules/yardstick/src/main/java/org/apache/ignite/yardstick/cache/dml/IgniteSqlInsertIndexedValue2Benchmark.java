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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.yardstick.cache.IgniteCacheAbstractBenchmark;

/**
 * Ignite benchmark that performs SQL INSERT operations for entity with 2 indexed fields.
 */
public class IgniteSqlInsertIndexedValue2Benchmark extends IgniteCacheAbstractBenchmark<Integer, Object> {
    /** */
    private final AtomicInteger insCnt = new AtomicInteger();

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        int key = insCnt.getAndIncrement();

        cache.query(new SqlFieldsQuery("insert into Person2(_key, val1, val2) values (?, ?, ?)")
                .setArgs(key, key, key + 1));

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("atomic-index-with-eviction");
    }
}
