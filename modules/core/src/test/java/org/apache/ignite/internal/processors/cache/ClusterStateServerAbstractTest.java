/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE_READ_ONLY;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.internal.processors.cache.ClusterStateTestUtils.ENTRY_CNT;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;

/**
 * Tests that cluster state change works correctly in different situations.
 */
public abstract class ClusterStateServerAbstractTest extends ClusterStateAbstractTest {
    /** */
    private static final String FAILED_DEACTIVATE_MSG =
        "Failed to deactivate cluster (must invoke the method outside of an active transaction).";

    /** */
    private static final String FAILED_ACTIVATE_MSG =
        "Failed to activate cluster (must invoke the method outside of an active transaction).";

    /** */
    private static final String FAILED_READ_ONLY_MSG =
        "Failed to activate cluster in read-only mode (must invoke the method outside of an active transaction).";

    /**
     * Tests that deactivation is prohibited if explicit lock is held in current thread.
     */
    @Test
    public void testDeactivationWithPendingLock() {
        changeClusterStateWithPendingLock(INACTIVE, FAILED_DEACTIVATE_MSG);
    }

    /**
     * Tests that enabling read-only mode is prohibited if explicit lock is held in current thread.
     */
    @Test
    public void testReadOnlyWithPendingLock() {
        changeClusterStateWithPendingLock(ACTIVE_READ_ONLY, FAILED_READ_ONLY_MSG);
    }

    /**
     * Tests that change cluster mode from {@link ClusterState#ACTIVE} to {@link ClusterState#INACTIVE} is prohibited if
     * transaction is active in current thread.
     */
    @Test
    public void testDeactivationWithPendingTransaction() {
        grid(0).cluster().state(ACTIVE);

        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values())
                changeStateWithPendingTransaction(INACTIVE, concurrency, isolation, FAILED_DEACTIVATE_MSG);
        }
    }

    /**
     * Tests that change cluster mode from {@link ClusterState#ACTIVE_READ_ONLY} to {@link ClusterState#INACTIVE} is prohibited
     * if transaction is active in current thread.
     */
    @Test
    public void testDeactivateFromReadonlyWithPendingTransaction() {
        grid(0).cluster().state(ACTIVE_READ_ONLY);

        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values())
                changeStateWithPendingTransaction(INACTIVE, concurrency, isolation, FAILED_DEACTIVATE_MSG);
        }
    }

    /**
     * Tests that change cluster mode from {@link ClusterState#ACTIVE} to {@link ClusterState#ACTIVE_READ_ONLY} is prohibited
     * if transaction is active in current thread.
     */
    @Test
    public void testReadOnlyWithPendingTransaction() {
        grid(0).cluster().state(ACTIVE);

        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values())
                changeStateWithPendingTransaction(ACTIVE_READ_ONLY, concurrency, isolation, FAILED_READ_ONLY_MSG);
        }
    }

    /**
     * Tests that change cluster mode from {@link ClusterState#ACTIVE_READ_ONLY} to {@link ClusterState#ACTIVE} is prohibited
     * if transaction is active in current thread.
     */
    @Test
    public void testDisableReadonlyWithPendingTransaction() {
        grid(0).cluster().state(ACTIVE_READ_ONLY);

        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values())
                changeStateWithPendingTransaction(ACTIVE, concurrency, isolation, FAILED_ACTIVATE_MSG);
        }
    }

    /** */
    @Test
    public void testDynamicCacheStart() {
        grid(0).cluster().state(ACTIVE);

        try {
            IgniteCache<Object, Object> cache2 = grid(0).createCache(new CacheConfiguration<>("cache2"));

            for (int k = 0; k < ENTRY_CNT; k++)
                cache2.put(k, k);
        }
        finally {
            grid(0).destroyCache("cache2");
        }
    }

    /** {@inheritDoc} */
    @Override protected void changeState(ClusterState state) {
        grid(0).cluster().state(state);
    }

    /** */
    private void changeStateWithPendingTransaction(
        ClusterState state,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation,
        String exceptionMsg
    ) {
        final IgniteCache<Object, Object> cache0 = grid(0).cache(CACHE_NAME);

        assertNotSame(state, grid(0).cluster().state());

        try (Transaction ignore = grid(0).transactions().txStart(concurrency, isolation)) {
            if (grid(0).cluster().state() != ACTIVE_READ_ONLY)
                cache0.put(1, "1");

            //noinspection ThrowableNotThrown
            assertThrowsAnyCause(log, changeStateClo(state), IgniteException.class, exceptionMsg);
        }

        assertNotSame(state, grid(0).cluster().state());

        if (grid(0).cluster().state() != ACTIVE_READ_ONLY)
            assertNull(cache0.get(1));

        assertNull(grid(0).transactions().tx());
    }

    /**
     * Tests that cluster state change to {@code newState} is prohibited if explicit lock is held in current thread.
     *
     * @param newState New cluster state.
     * @param exceptionMsg Exception message.
     */
    private void changeClusterStateWithPendingLock(ClusterState newState, String exceptionMsg) {
        grid(0).cluster().state(ACTIVE);

        Lock lock = grid(0).cache(CACHE_NAME).lock(1);

        lock.lock();

        try {
            //noinspection ThrowableNotThrown
            assertThrowsAnyCause(log, changeStateClo(newState), IgniteException.class, exceptionMsg);
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * @param state New cluster state.
     * @return Callable which tries to change cluster state to {@code state} from {@code ignite} node.
     */
    Callable<Object> changeStateClo(ClusterState state) {
        return () -> {
            grid(0).cluster().state(state);

            return null;
        };
    }
}
