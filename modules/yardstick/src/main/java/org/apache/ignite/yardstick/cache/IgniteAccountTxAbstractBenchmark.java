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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.yardstick.cache.model.Account;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 *
 */
public abstract class IgniteAccountTxAbstractBenchmark extends IgniteCacheAbstractBenchmark {
    /** */
    protected IgniteTransactions txs;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        txs = ignite().transactions();

        println(cfg, "Populating data...");

        long start = System.nanoTime();

        try (IgniteDataStreamer<Integer, Account> dataLdr = ignite().dataStreamer(cache.getName())) {
            for (int i = 0; i < args.range() && !Thread.currentThread().isInterrupted(); i++) {
                dataLdr.addData(i, new Account(100_000));

                if (i % 100000 == 0)
                    println(cfg, "Populated accounts: " + i);
            }
        }

        println(cfg, "Finished populating data in " + ((System.nanoTime() - start) / 1_000_000) + " ms.");
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("tx");
    }
}
