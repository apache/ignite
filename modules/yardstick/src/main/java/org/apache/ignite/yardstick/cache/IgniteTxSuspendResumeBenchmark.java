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

import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionState;
import org.apache.ignite.yardstick.cache.model.SampleValue;
import org.yardstickframework.BenchmarkConfiguration;

/**
 * Ignite benchmark that performs suspend\resume operations.
 * Concurrent threads must operate different keys, so there will be no waiting on key locks.
 * So, all keys are separated on different groups, defined by first key
 * and last key (first key plus keys number per thread).
 * Groups have no intersections.
 */
public class IgniteTxSuspendResumeBenchmark extends IgniteCacheAbstractBenchmark<Integer, Object> {
    /** Transactions. */
    private IgniteTransactions txs;

    /** ExecutorService. */
    private ExecutorService ex;

    /**
     * First keyCounter that can be used by transaction in certain thread.
     * The last one can be calculated based on keys per thread
     */
    private ThreadLocal<Integer> firstKey;

    /** Number of keys, allocated to every thread. */
    private int keysNumbPerThread = 0;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        txs = ignite().transactions();

        ex = Executors.newFixedThreadPool(cfg.threads());

        keysNumbPerThread = (args.range() > cfg.threads() ? args.range() / cfg.threads() : args.range());

        firstKey = new ThreadLocal<Integer>() {
            /** Helps to calculate first key for every thread. */
            final private AtomicInteger keyCntr = new AtomicInteger(0);

            /** {@inheritDoc} */
            @Override protected Integer initialValue() {
                return keyCntr.getAndAdd(keysNumbPerThread);
            }
        };
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        super.tearDown();

        ex.shutdown();
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        Integer origin = firstKey.get();

        int key = ThreadLocalRandom.current().nextInt(origin, origin + keysNumbPerThread);

        try (Transaction tx = txs.txStart(args.txConcurrency(), args.txIsolation())) {
            IgniteCache<Integer, Object> cache = cacheForOperation();

            cache.put(key, new SampleValue(key));

            tx.suspend();

            ex.submit(new Callable<Void>() {
                @Override public Void call() {
                    tx.resume();

                    tx.commit();

                    return null;
                }
            }).get();
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("tx");
    }
}
