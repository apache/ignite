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

package org.apache.ignite.internal.benchmarks.jmh.thin;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.openjdk.jmh.annotations.Benchmark;

/** Cache operations and an SQL select through the thin client; their arguments are created in setup. */
@SuppressWarnings("unchecked")
public class JmhThinClientAllocationBenchmark extends JmhThinClientAllocationAbstractBenchmark {
    /** Keys in a batch. */
    private static final int BATCH = 10;

    /** Thin client cache. */
    private ClientCache<Integer, Object> thinCache;

    /** Consecutive keys of each batch. */
    private Set<Integer>[] batchKeys;

    /** Entries of each batch. */
    private Map<Integer, Object>[] batches;

    /** Select of each key. */
    private SqlFieldsQuery[] selects;

    /** {@inheritDoc} */
    @Override public void setup() throws Exception {
        super.setup();

        batchKeys = new Set[CNT / BATCH];
        batches = new Map[CNT / BATCH];

        for (int b = 0; b < batches.length; b++) {
            batchKeys[b] = new LinkedHashSet<>();
            batches[b] = new LinkedHashMap<>();

            for (int i = b * BATCH; i < (b + 1) * BATCH; i++) {
                batchKeys[b].add(keys[i]);
                batches[b].put(keys[i], vals[i]);
            }
        }

        String selectSql = "SELECT * FROM " + valType.valueClass().getSimpleName() + " WHERE _key = ?";

        selects = new SqlFieldsQuery[CNT];

        for (int i = 0; i < CNT; i++)
            selects[i] = new SqlFieldsQuery(selectSql).setArgs(keys[i]);
    }

    /** {@inheritDoc} */
    @Override protected AutoCloseable connect() {
        IgniteClient client = Ignition.startClient(new ClientConfiguration()
            .setAddresses(addresses())
            .setPartitionAwarenessEnabled(partitionAwareness));

        thinCache = client.cache(cache.getName());

        return client;
    }

    /** Replaces the value of a key with the same value. */
    @Benchmark
    public void put() {
        int i = ThreadLocalRandom.current().nextInt(CNT);

        thinCache.put(keys[i], vals[i]);
    }

    /** @return Value of a key. */
    @Benchmark
    public Object get() {
        return thinCache.get(keys[ThreadLocalRandom.current().nextInt(CNT)]);
    }

    /** Replaces the values of {@link #BATCH} consecutive keys with the same values. */
    @Benchmark
    public void putAll() {
        thinCache.putAll(batches[ThreadLocalRandom.current().nextInt(batches.length)]);
    }

    /** @return Values of {@link #BATCH} consecutive keys. */
    @Benchmark
    public Map<Integer, Object> getAll() {
        return thinCache.getAll(batchKeys[ThreadLocalRandom.current().nextInt(batchKeys.length)]);
    }

    /** @return Row of a key, selected with SQL. */
    @Benchmark
    public List<List<?>> sqlSelect() {
        return thinCache.query(selects[ThreadLocalRandom.current().nextInt(CNT)]).getAll();
    }
}
