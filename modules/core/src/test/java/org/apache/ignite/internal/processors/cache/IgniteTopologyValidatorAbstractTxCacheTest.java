/*
 *                    GridGain Community Edition Licensing
 *                    Copyright 2019 GridGain Systems, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 *  Restriction; you may not use this file except in compliance with the License. You may obtain a
 *  copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 *  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the specific language governing permissions
 *  and limitations under the License.
 *
 *  Commons Clause Restriction
 *
 *  The Software is provided to you by the Licensor under the License, as defined below, subject to
 *  the following condition.
 *
 *  Without limiting other conditions in the License, the grant of rights under the License will not
 *  include, and the License does not grant to you, the right to Sell the Software.
 *  For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 *  under the License to provide to third parties, for a fee or other consideration (including without
 *  limitation fees for hosting or consulting/ support services related to the Software), a product or
 *  service whose value derives, entirely or substantially, from the functionality of the Software.
 *  Any license notice or attribution required by the License must also include this Commons Clause
 *  License Condition notice.
 *
 *  For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 *  the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 *  Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Topology validator test
 */
@RunWith(JUnit4.class)
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
    @Test
    @Override public void testTopologyValidator() throws Exception {
        try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            putInvalid(CACHE_NAME_1);
        }

        if (!MvccFeatureChecker.forcedMvcc()) {
            try (Transaction tx = grid(0).transactions().txStart(OPTIMISTIC, REPEATABLE_READ)) {
                putValid(CACHE_NAME_1);
                commitFailed(tx);
            }

            try (Transaction tx = grid(0).transactions().txStart(OPTIMISTIC, REPEATABLE_READ)) {
                putValid(CACHE_NAME_1);
                putValid(DEFAULT_CACHE_NAME);
                putValid(CACHE_NAME_2);
                commitFailed(tx);
            }
        }

        assertEmpty(DEFAULT_CACHE_NAME); // rolled back
        assertEmpty(CACHE_NAME_1); // rolled back
        assertEmpty(CACHE_NAME_2); // rolled back

        try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            putValid(DEFAULT_CACHE_NAME);
            putInvalid(CACHE_NAME_1);
        }

        assertEmpty(DEFAULT_CACHE_NAME); // rolled back
        assertEmpty(CACHE_NAME_1); // rolled back

        startGrid(1);

        try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            putValid(CACHE_NAME_1);
            tx.commit();
        }

        remove(CACHE_NAME_1);

        try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            putValid(CACHE_NAME_1);
            tx.commit();
        }

        remove(CACHE_NAME_1);

        startGrid(2);

        try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            putValid(DEFAULT_CACHE_NAME);
            putInvalid(CACHE_NAME_1);
        }

        assertEmpty(DEFAULT_CACHE_NAME); // rolled back
        assertEmpty(CACHE_NAME_1); // rolled back

        if (!MvccFeatureChecker.forcedMvcc()) {
            try (Transaction tx = grid(0).transactions().txStart(OPTIMISTIC, REPEATABLE_READ)) {
                putValid(CACHE_NAME_1);
                commitFailed(tx);
            }
        }


        try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            putInvalid(CACHE_NAME_1);
        }

        try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            putValid(DEFAULT_CACHE_NAME);
            putValid(CACHE_NAME_2);
            tx.commit();
        }

        remove(DEFAULT_CACHE_NAME);
        remove(CACHE_NAME_2);

        try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            putValid(DEFAULT_CACHE_NAME);
            putValid(CACHE_NAME_2);
            tx.commit();
        }

        remove(DEFAULT_CACHE_NAME);
        remove(CACHE_NAME_2);
    }
}
