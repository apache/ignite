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

package org.apache.ignite.internal.processors.cache.consistency;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 *
 */
public abstract class CacheConsistencyCheckExplicitTransactionalAbstractTest extends CacheConsistencyCheckAbstractTest {
    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return CacheAtomicityMode.TRANSACTIONAL;
    }

    /**
     *
     */
    public abstract void test() throws Exception;

    /**
     *
     */
    protected void test(TransactionConcurrency concurrency,
        TransactionIsolation isolation) throws Exception {
        test(concurrency, isolation, true);
        test(concurrency, isolation, false);
    }

    /**
     *
     */
    private void test(TransactionConcurrency concurrency,
        TransactionIsolation isolation, boolean raw /*getEntry() or just get()*/) throws Exception {
        for (Ignite node : G.allGrids()) {
            testGet(node, concurrency, isolation, raw);
            testGetAllVariations(node, concurrency, isolation, raw);
        }
    }

    /**
     *
     */
    protected abstract void testGet(Ignite initiator, TransactionConcurrency concurrency,
        TransactionIsolation isolation, boolean raw) throws Exception;

    /**
     *
     */
    private void testGetAllVariations(Ignite initiator, TransactionConcurrency concurrency,
        TransactionIsolation isolation, boolean raw) throws Exception {
        testGetAll(initiator, concurrency, isolation, 1, raw); // 1 (all keys available at primary)
        testGetAll(initiator, concurrency, isolation, 2, raw); // less than backups
        testGetAll(initiator, concurrency, isolation, 3, raw); // equals to backups
        testGetAll(initiator, concurrency, isolation, 4, raw); // equals to backups + primary
        testGetAll(initiator, concurrency, isolation, 10, raw); // more than backups
    }

    /**
     *
     */
    protected abstract void testGetAll(Ignite initiator, TransactionConcurrency concurrency,
        TransactionIsolation isolation, Integer cnt, boolean raw) throws Exception;
}
