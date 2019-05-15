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
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Topology validator test
 */
public abstract class IgniteTopologyValidatorAbstractTxCacheGroupsTest
    extends IgniteTopologyValidatorCacheGroupsAbstractTest {
    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testTopologyValidator() throws Exception {
        try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            putInvalid(CACHE_NAME_1);
        }

        try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            putInvalid(CACHE_NAME_3);
        }

        if (!MvccFeatureChecker.forcedMvcc()) {
            try (Transaction tx = grid(0).transactions().txStart(OPTIMISTIC, REPEATABLE_READ)) {
                putValid(CACHE_NAME_1);
                putValid(CACHE_NAME_3);
                commitFailed(tx);
            }
        }

        assertEmpty(CACHE_NAME_1); // Rolled back.
        assertEmpty(CACHE_NAME_3); // Rolled back.

        try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            putInvalid(CACHE_NAME_1);
        }

        try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            putInvalid(CACHE_NAME_3);
        }

        try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            putValid(DEFAULT_CACHE_NAME);
            putInvalid(CACHE_NAME_1);
        }

        assertEmpty(DEFAULT_CACHE_NAME); // Rolled back.

        startGrid(1);

        if (!MvccFeatureChecker.forcedMvcc()) {
            try (Transaction tx = grid(0).transactions().txStart(OPTIMISTIC, REPEATABLE_READ)) {
                putValid(CACHE_NAME_1);
                putValid(CACHE_NAME_3);
                tx.commit();
            }
        }
        else {
            try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                putValid(CACHE_NAME_1);
                putValid(CACHE_NAME_3);
                tx.commit();
            }
        }

        remove(CACHE_NAME_1);
        remove(CACHE_NAME_3);

        try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            putValid(CACHE_NAME_1);
            putValid(CACHE_NAME_3);
            tx.commit();
        }

        remove(CACHE_NAME_1);
        remove(CACHE_NAME_3);

        startGrid(2);

        try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            putValid(CACHE_NAME_3);
            putInvalid(CACHE_NAME_1);
        }

        assertEmpty(CACHE_NAME_3); // Rolled back.

        if (!MvccFeatureChecker.forcedMvcc()) {
            try (Transaction tx = grid(0).transactions().txStart(OPTIMISTIC, REPEATABLE_READ)) {
                putValid(CACHE_NAME_1);
                putValid(CACHE_NAME_3);
                commitFailed(tx);
            }
        }

        assertEmpty(CACHE_NAME_1); // Rolled back.
        assertEmpty(CACHE_NAME_3); // Rolled back.

        if (!MvccFeatureChecker.forcedMvcc()) {
            try (Transaction tx = grid(0).transactions().txStart(OPTIMISTIC, REPEATABLE_READ)) {
                putValid(DEFAULT_CACHE_NAME);
                putValid(CACHE_NAME_3);
                tx.commit();
            }
        }
        else {
            try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                putValid(DEFAULT_CACHE_NAME);
                putValid(CACHE_NAME_3);
                tx.commit();
            }
        }

        remove(DEFAULT_CACHE_NAME);
        remove(CACHE_NAME_3);

        try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            putValid(DEFAULT_CACHE_NAME);
            putValid(CACHE_NAME_3);
            tx.commit();
        }

        remove(DEFAULT_CACHE_NAME);
        remove(CACHE_NAME_3);
    }
}
