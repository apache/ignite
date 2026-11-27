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

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientTransaction;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.openjdk.jmh.annotations.Benchmark;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/** Puts of two keys in a thin client transaction. */
public class JmhThinClientTxAllocationBenchmark extends JmhThinClientAllocationAbstractBenchmark {
    /** Thin client. */
    private IgniteClient client;

    /** Thin client cache. */
    private ClientCache<Integer, Object> thinCache;

    /** {@inheritDoc} */
    @Override protected AutoCloseable connect() {
        client = Ignition.startClient(new ClientConfiguration()
            .setAddresses(addresses())
            .setPartitionAwarenessEnabled(partitionAwareness));

        thinCache = client.cache(cache.getName());

        return client;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration() {
        return super.cacheConfiguration().setAtomicityMode(TRANSACTIONAL);
    }

    /** Puts the values two consecutive keys already have, in a pessimistic repeatable read transaction. */
    @Benchmark
    public void txPut() {
        int i = ThreadLocalRandom.current().nextInt(CNT - 1);

        try (ClientTransaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            thinCache.put(keys[i], vals[i]);
            thinCache.put(keys[i + 1], vals[i + 1]);

            tx.commit();
        }
    }
}
