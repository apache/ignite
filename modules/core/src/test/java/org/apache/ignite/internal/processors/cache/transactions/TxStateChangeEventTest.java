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

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.events.TransactionStateChangedEvent;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVTS_TX;
import static org.apache.ignite.events.EventType.EVT_TX_COMMITTED;
import static org.apache.ignite.events.EventType.EVT_TX_RESUMED;
import static org.apache.ignite.events.EventType.EVT_TX_ROLLED_BACK;
import static org.apache.ignite.events.EventType.EVT_TX_STARTED;
import static org.apache.ignite.events.EventType.EVT_TX_SUSPENDED;

/**
 * Tests transaction state change event.
 */
public class TxStateChangeEventTest extends GridCommonAbstractTest {
    /** Label. */
    private final String lb = "testLabel";

    /** Timeout. */
    private final long timeout = 404;

    /** Creation. */
    private static AtomicBoolean creation = new AtomicBoolean();

    /** Commit. */
    private static AtomicBoolean commit = new AtomicBoolean();

    /** Rollback. */
    private static AtomicBoolean rollback = new AtomicBoolean();

    /** Suspend. */
    private static AtomicBoolean suspend = new AtomicBoolean();

    /** Resume. */
    private static AtomicBoolean resume = new AtomicBoolean();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setIncludeEventTypes(EventType.EVTS_ALL);
    }

    /**
     *
     */
    @Test
    public void testLocal() throws Exception {
        check(true);
    }

    /**
     *
     */
    @Test
    public void testRemote() throws Exception {
        check(false);
    }

    /**
     *
     */
    private void check(boolean loc) throws Exception {
        Ignite ignite = startGrids(5);

        final IgniteEvents evts = loc ? ignite.events() : grid(3).events();

        if (loc)
            evts.localListen((IgnitePredicate<Event>)e -> {
                assert e instanceof TransactionStateChangedEvent;

                checkEvent((TransactionStateChangedEvent)e);

                return true;
            }, EVTS_TX);
        else
            evts.remoteListen(null,
                (IgnitePredicate<Event>)e -> {
                    assert e instanceof TransactionStateChangedEvent;

                    checkEvent((TransactionStateChangedEvent)e);

                    return false;
                },
                EVTS_TX);

        IgniteTransactions txs = ignite.transactions();

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(getCacheConfig());

        checkCommit(txs, cache);

        if (!MvccFeatureChecker.forcedMvcc())
            checkSuspendResume(txs, cache);

        checkRollback(txs, cache);
    }

    /** */
    @SuppressWarnings("unchecked")
    private CacheConfiguration<Integer, Integer> getCacheConfig() {
        return defaultCacheConfiguration().setBackups(2);
    }

    /**
     * @param txs Transaction manager.
     * @param cache Ignite cache.
     */
    private void checkRollback(IgniteTransactions txs, IgniteCache<Integer, Integer> cache) {
        // create & rollback (pessimistic)
        try (Transaction tx = txs.withLabel(lb).txStart(
            TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ, timeout, 3)) {
            cache.put(4, 5);
        }

        assertTrue(
            creation.get() &&
                !commit.get() &&
                rollback.get() &&
                !suspend.get() &&
                !resume.get());
    }

    /**
     * @param txs Transaction manager.
     * @param cache Ignite cache.
     */
    private void checkSuspendResume(IgniteTransactions txs,
        IgniteCache<Integer, Integer> cache) throws IgniteInterruptedCheckedException {
        // create & suspend & resume & commit
        try (Transaction tx = txs.withLabel(lb).txStart(
            TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE, timeout, 3)) {
            cache.put(2, 7);

            tx.suspend();

            U.sleep(100);

            tx.resume();

            tx.commit();
        }

        assertTrue(
            creation.get() &&
                commit.get() &&
                !rollback.get() &&
                suspend.get() &&
                resume.get());

        clear();
    }

    /**
     * @param txs Transaction manager.
     * @param cache Ignite cache.
     */
    private void checkCommit(IgniteTransactions txs, IgniteCache<Integer, Integer> cache) {
        // create & commit
        try (Transaction tx = txs.withLabel(lb).txStart(
            TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ, timeout, 3)) {
            cache.put(1, 1);

            tx.commit();
        }

        assertTrue(
            creation.get() &&
                commit.get() &&
                !rollback.get() &&
                !suspend.get() &&
                !resume.get());

        clear();
    }

    /**
     *
     */
    private void clear() {
        creation.set(false);
        commit.set(false);
        rollback.set(false);
        suspend.set(false);
        resume.set(false);
    }

    /**
     * @param evt Event.
     */
    private void checkEvent(TransactionStateChangedEvent evt) {
        Transaction tx = evt.tx();

        assertEquals(timeout, tx.timeout());
        assertEquals(lb, tx.label());

        switch (evt.type()) {
            case EVT_TX_STARTED: {
                assertEquals(tx.state(), TransactionState.ACTIVE);

                assertFalse(creation.getAndSet(true));

                break;
            }

            case EVT_TX_COMMITTED: {
                assertEquals(tx.state(), TransactionState.COMMITTED);

                assertFalse(commit.getAndSet(true));

                break;
            }

            case EVT_TX_ROLLED_BACK: {
                assertEquals(tx.state(), TransactionState.ROLLED_BACK);

                assertFalse(rollback.getAndSet(true));

                break;
            }

            case EVT_TX_SUSPENDED: {
                assertEquals(tx.state(), TransactionState.SUSPENDED);

                assertFalse(suspend.getAndSet(true));

                break;
            }

            case EVT_TX_RESUMED: {
                assertEquals(tx.state(), TransactionState.ACTIVE);

                assertFalse(resume.getAndSet(true));

                break;
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        clear();
    }
}
