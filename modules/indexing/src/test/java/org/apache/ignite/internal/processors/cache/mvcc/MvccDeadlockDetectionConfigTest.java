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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.concurrent.CyclicBarrier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.After;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/** */
public class MvccDeadlockDetectionConfigTest extends GridCommonAbstractTest {
    /** */
    private boolean deadlockDetectionEnabled;

    /** */
    @After
    public void stopCluster() {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        int timeout = deadlockDetectionEnabled ? 1 : 0;

        return super.getConfiguration(igniteInstanceName)
            .setTransactionConfiguration(new TransactionConfiguration().setDeadlockTimeout(timeout));
    }

    /** */
    @Test
    public void deadlockDetectionDisabled() throws Exception {
        deadlockDetectionEnabled = false;

        Ignite ign = startGrid();

        IgniteCache<Object, Object> cache = ign.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT));

        CyclicBarrier b = new CyclicBarrier(2);

        int txTimeout = 3_000;

        IgniteInternalFuture<?> futA = GridTestUtils.runAsync(() -> {
            try (Transaction tx = ign.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, txTimeout, 0)) {
                cache.put(1, 'a');
                b.await();
                cache.put(2, 'a');
            }

            return null;
        });

        IgniteInternalFuture<?> futB = GridTestUtils.runAsync(() -> {
            try (Transaction tx = ign.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, txTimeout, 0)) {
                cache.put(2, 'b');
                b.await();
                cache.put(1, 'b');
            }

            return null;
        });

        IgniteCheckedException e = awaitCompletion(futA, futB);

        assertTrue(e.toString(), e.hasCause(IgniteTxTimeoutCheckedException.class));
    }

    /** */
    @Test
    public void deadlockDetectionEnabled() throws Exception {
        deadlockDetectionEnabled = true;

        Ignite ign = startGrid();

        IgniteCache<Object, Object> cache = ign.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT));

        CyclicBarrier b = new CyclicBarrier(2);

        IgniteInternalFuture<?> futA = GridTestUtils.runAsync(() -> {
            try (Transaction tx = ign.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.put(1, 'a');
                b.await();
                cache.put(2, 'a');
            }

            return null;
        });

        IgniteInternalFuture<?> futB = GridTestUtils.runAsync(() -> {
            try (Transaction tx = ign.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.put(2, 'b');
                b.await();
                cache.put(1, 'b');
            }

            return null;
        });

        IgniteCheckedException e = awaitCompletion(futA, futB);

        assertTrue(e.toString(), X.hasCause(e, "Deadlock", IgniteTxRollbackCheckedException.class));
    }

    /** */
    private IgniteCheckedException awaitCompletion(IgniteInternalFuture<?> fut1, IgniteInternalFuture<?> fut2) {
        IgniteCheckedException e = null;

        try {
            fut1.get(10_000);
        }
        catch (IgniteCheckedException e1) {
            e = e1;
        }

        try {
            fut2.get(10_000);
        }
        catch (IgniteCheckedException e1) {
            e = e1;
        }

        return e;
    }
}
