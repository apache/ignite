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
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 *
 */
public abstract class AbstractTransactionsInMultipleThreadsTest extends GridCacheAbstractSelfTest {
    /** Transaction concurrency control. */
    protected TransactionConcurrency transactionConcurrency;

    /** Transaction isolation level. */
    protected TransactionIsolation transactionIsolation;

    /** Id of node, started transaction. */
    protected int txInitiatorNodeId = 0;

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
     * Starts test scenario for all transaction controls, and isolation levels.
     *
     * @param consumer Scenario.
     */
    protected void withAllIsolationsAndConcurrencies(IgniteClosure<Object, Void> consumer) {
        withAllIsolationsAndConcurrencies(consumer, null);
    }

    /**
     * Starts test scenario for all transaction controls, and isolation levels.
     *
     * @param consumer Scenario.
     * @param arg Argument.
     */
    protected void withAllIsolationsAndConcurrencies(IgniteClosure<Object, Void> consumer, Object arg) {
        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            this.transactionConcurrency = concurrency;

            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                this.transactionIsolation = isolation;

                consumer.apply(arg);
            }
        }
    }
}
