/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.yardstick.cache;

import java.util.ArrayList;
import java.util.Collections;
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
        long endTime;

        try (Transaction tx = transactions.txStart(args.txConcurrency(), args.txIsolation())) {
            ArrayList<Long> keyList = new ArrayList<>(args.scaleFactor());

            for (int i = 0; i < args.scaleFactor(); i++)
                keyList.add(random.nextLong());

            Collections.sort(keyList);

            for (int i = 0; i < args.scaleFactor(); i++){
                IgniteCache<Object, Object> curCache = cacheList.get(random.nextInt(cacheList.size()));
                curCache.put(keyList.get(i), val);
            }

            startTime = System.currentTimeMillis();

            tx.commit();

            endTime = System.currentTimeMillis();
        }

        TransactionMetrics tm = transactions.metrics();

        if (endTime - startTime > args.getWarningTime())
            BenchmarkUtils.println("Transaction commit time = " + (endTime - startTime));

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
