/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 * Topology validator test
 */
public abstract class IgniteTopologyValidatorAbstractTxCacheTest extends IgniteTopologyValidatorAbstractCacheTest {

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void testTopologyValidator() throws Exception {

        try (Transaction tx = grid(0).transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            putValid(CACHE_NAME_1);
            commitFailed(tx);
        }

        try (Transaction tx = grid(0).transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            putInvalid(CACHE_NAME_1);
        }

        try (Transaction tx = grid(0).transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            putValid(CACHE_NAME_1);
            putValid(null);
            putValid(CACHE_NAME_2);
            commitFailed(tx);
        }

        assertEmpty(null); // rolled back
        assertEmpty(CACHE_NAME_1); // rolled back
        assertEmpty(CACHE_NAME_2); // rolled back

        try (Transaction tx = grid(0).transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            putValid(null);
            putInvalid(CACHE_NAME_1);
        }

        assertEmpty(null); // rolled back
        assertEmpty(CACHE_NAME_1); // rolled back

        startGrid(1);

        try (Transaction tx = grid(0).transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            putValid(CACHE_NAME_1);
            tx.commit();
        }

        remove(CACHE_NAME_1);

        try (Transaction tx = grid(0).transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            putValid(CACHE_NAME_1);
            tx.commit();
        }

        remove(CACHE_NAME_1);

        startGrid(2);

        try (Transaction tx = grid(0).transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            putValid(null);
            putInvalid(CACHE_NAME_1);
        }

        assertEmpty(null); // rolled back
        assertEmpty(CACHE_NAME_1); // rolled back

        try (Transaction tx = grid(0).transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            putValid(CACHE_NAME_1);
            commitFailed(tx);
        }

        try (Transaction tx = grid(0).transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            putInvalid(CACHE_NAME_1);
        }

        try (Transaction tx = grid(0).transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            putValid(null);
            putValid(CACHE_NAME_2);
            tx.commit();
        }

        remove(null);
        remove(CACHE_NAME_2);

        try (Transaction tx = grid(0).transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            putValid(null);
            putValid(CACHE_NAME_2);
            tx.commit();
        }

        remove(null);
        remove(CACHE_NAME_2);
    }
}