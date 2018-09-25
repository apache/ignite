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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.util.typedef.CIX2;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class TxSavepointsSelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /**
     *
     */
    public void testNullNameSavepoint() {
        GridTestUtils.assertThrows(log, () -> {
                try (Transaction tx = ignite(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    tx.savepoint(null);
                }

                return null;
            },
            IllegalArgumentException.class,
            "Savepoint name can't be null."
        );
    }

    /**
     *
     */
    public void testNullNameRollbackToSavepoint() {
        GridTestUtils.assertThrows(log, () -> {
                try (Transaction tx = ignite(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    tx.rollbackToSavepoint(null);
                }

                return null;
            },
            IllegalArgumentException.class,
            "Savepoint name can't be null."
        );
    }

    /**
     *
     */
    public void testNullNameReleaseSavepoint() {
        GridTestUtils.assertThrows(log, () -> {
                try (Transaction tx = ignite(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    tx.releaseSavepoint(null);
                }

                return null;
            },
            IllegalArgumentException.class,
            "Savepoint name can't be null."
        );
    }

    /**
     *
     */
    public void testSetTimeoutAfterSavepoint() {
        GridTestUtils.assertThrows(log, () -> {
                try (Transaction tx = ignite(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    tx.savepoint("name");

                    tx.timeout(1_000);
                }

                return null;
            },
            IllegalStateException.class,
            "Cannot change timeout after transaction has started:"
        );
    }
}
