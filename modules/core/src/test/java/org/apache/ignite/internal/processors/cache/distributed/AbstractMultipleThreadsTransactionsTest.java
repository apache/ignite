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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 * Abstract class for tests of multiple threads transactions.
 */
public abstract class AbstractMultipleThreadsTransactionsTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return CacheAtomicityMode.TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /**
     * @return Id of node, started transaction.
     */
    protected int txInitiatorNodeId() {
        return 0;
    }

    /**
     * Starts test scenario in mix mode of all transaction concurrency and all isolation levels.
     *
     * @param test Test's scenario.
     * @throws Exception In case of error.
     */
    protected void runMixIsolationsAndConcurrencies(Testable test) throws Exception {
        for (TransactionConcurrency txConcurrency : TransactionConcurrency.values())
            for (TransactionIsolation txIsolation : TransactionIsolation.values())
                test.test(txConcurrency, txIsolation);
    }

    /** */
    protected interface Testable {
        /**
         * @param txConcurrency Transaction Concurrency.
         * @param txIsolation Transaction txIsolation level.
         * @throws Exception In case of error.
         */
        void test(TransactionConcurrency txConcurrency, TransactionIsolation txIsolation) throws Exception;
    }
}
