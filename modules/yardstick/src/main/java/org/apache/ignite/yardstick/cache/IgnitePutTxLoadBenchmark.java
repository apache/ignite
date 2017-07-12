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

import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionMetrics;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

/**
 * Ignite benchmark that performs transactional load put operations.
 */
public class IgnitePutTxLoadBenchmark extends IgniteCacheAbstractBenchmark<Integer, Object> {
    /** */
    private ArrayList<IgniteCache<Object, Object>> cacheList;

    /** */
    private String val;

    /** */
    private Random random;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        if (!IgniteSystemProperties.getBoolean("SKIP_MAP_CHECK"))
            ignite().compute().broadcast(new WaitMapExchangeFinishCallable());

        cacheList = new ArrayList<>(args.cachesCount());

        for (int i = 0; i < args.cachesCount(); i++)
            cacheList.add(ignite().cache("tx-" + i));

        val = createVal(args.getStringLength());

        random = new Random();
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        IgniteTransactions transactions = ignite().transactions();

        long startTime;

        try (Transaction tx = transactions.txStart(args.txConcurrency(), args.txIsolation())) {
            for (int i = 0; i < args.scaleFactor(); i++){
                IgniteCache<Object, Object> curCache = cacheList.get(random.nextInt(cacheList.size()));
                curCache.put(random.nextLong(), val);
            }

            startTime = System.currentTimeMillis();

            tx.commit();
        }

        TransactionMetrics tm = transactions.metrics();

        if (tm.commitTime() - startTime > args.getWarningTime())
            BenchmarkUtils.println("Transaction commit time = " + (tm.commitTime() - startTime));

        if (tm.txRollbacks() > 0 && args.printRollBacks())
            BenchmarkUtils.println("Transaction rollbacks = " + tm.txRollbacks());

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("tx");
    }

    /**
     * Creates String val
     * @param lgth String length
     * @return String for inserting in cache
     */
    private String createVal(int lgth){
        StringBuilder sb = new StringBuilder(lgth);

        for(int i = 0; i < lgth; i++)
            sb.append('x');

        return sb.toString();
    }
}
