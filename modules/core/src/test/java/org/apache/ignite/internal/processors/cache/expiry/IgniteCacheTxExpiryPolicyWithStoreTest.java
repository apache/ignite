/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.expiry;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 *
 */
public class IgniteCacheTxExpiryPolicyWithStoreTest extends IgniteCacheExpiryPolicyWithStoreAbstractTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testGetReadThrough() throws Exception {
        super.testGetReadThrough();

        getReadThrough(false, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);
        getReadThrough(true, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);
        getReadThrough(false, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ);
        getReadThrough(true, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ);
        getReadThrough(false, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE);
        getReadThrough(true, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE);

        getReadThrough(false, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED);
        getReadThrough(true, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED);
        getReadThrough(false, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);
        getReadThrough(true, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);
        getReadThrough(false, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.SERIALIZABLE);
        getReadThrough(true, TransactionConcurrency.PESSIMISTIC, TransactionIsolation.SERIALIZABLE);
    }
}
