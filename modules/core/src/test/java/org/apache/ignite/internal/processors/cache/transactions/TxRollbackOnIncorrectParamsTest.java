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

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.TransactionStateChangedEvent;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.events.EventType.EVT_TX_STARTED;

/**
 * Tests transaction rollback on incorrect tx params.
 */
@RunWith(JUnit4.class)
public class TxRollbackOnIncorrectParamsTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        Assume.assumeFalse("https://issues.apache.org/jira/browse/IGNITE-10415", MvccFeatureChecker.forcedMvcc());
    }

    /**
     *
     */
    @Test
    public void testTimeoutSetLocalGuarantee() throws Exception {
        Ignite ignite = startGrid(0);

        ignite.events().localListen((IgnitePredicate<Event>)e -> {
            assert e instanceof TransactionStateChangedEvent;

            TransactionStateChangedEvent evt = (TransactionStateChangedEvent)e;

            Transaction tx = evt.tx();

            if (tx.timeout() < 200)
                tx.setRollbackOnly();

            return true;
        }, EVT_TX_STARTED);

        IgniteCache cache = ignite.getOrCreateCache(defaultCacheConfiguration());

        try (Transaction tx = ignite.transactions().txStart(
            TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ, 200, 2)) {
            cache.put(1, 1);

            tx.commit();
        }

        try (Transaction tx = ignite.transactions().txStart(
            TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ, 100, 2)) {
            cache.put(1, 2);

            tx.commit();

            fail("Should fail prior this line.");
        }
        catch (CacheException ignored) {
            // No-op.
        }

        try (Transaction tx = ignite.transactions().txStart()) {
            cache.put(1, 3);

            tx.commit();

            fail("Should fail prior this line.");
        }
        catch (CacheException ignored) {
            // No-op.
        }
    }

    /**
     *
     */
    @Test
    public void testLabelFilledLocalGuarantee() throws Exception {
        Ignite ignite = startGrid(0);

        ignite.events().localListen((IgnitePredicate<Event>)e -> {
            assert e instanceof TransactionStateChangedEvent;

            TransactionStateChangedEvent evt = (TransactionStateChangedEvent)e;

            Transaction tx = evt.tx();

            if (tx.label() == null)
                tx.setRollbackOnly();

            return true;
        }, EVT_TX_STARTED);

        IgniteCache cache = ignite.getOrCreateCache(defaultCacheConfiguration());

        try (Transaction tx = ignite.transactions().withLabel("test").txStart()) {
            cache.put(1, 1);

            tx.commit();
        }

        try (Transaction tx = ignite.transactions().txStart()) {
            cache.put(1, 2);

            tx.commit();

            fail("Should fail prior this line.");
        }
        catch (CacheException ignored) {
            // No-op.
        }
    }

    /**
     *
     */
    @Test
    public void testLabelFilledRemoteGuarantee() throws Exception {
        Ignite ignite = startGrid(0);
        Ignite remote = startGrid(1);

        IgniteCache cacheLocal = ignite.getOrCreateCache(defaultCacheConfiguration());
        IgniteCache cacheRemote = remote.getOrCreateCache(defaultCacheConfiguration());

        ignite.events().remoteListen(null,
            (IgnitePredicate<Event>)e -> {
                assert e instanceof TransactionStateChangedEvent;

                TransactionStateChangedEvent evt = (TransactionStateChangedEvent)e;

                Transaction tx = evt.tx();

                if (tx.label() == null)
                    tx.setRollbackOnly();

                return true;
            },
            EVT_TX_STARTED);

        try (Transaction tx = ignite.transactions().withLabel("test").txStart()) {
            cacheLocal.put(1, 1);

            tx.commit();
        }

        try (Transaction tx = remote.transactions().withLabel("test").txStart()) {
            cacheRemote.put(1, 2);

            tx.commit();
        }

        try (Transaction tx = ignite.transactions().txStart()) {
            cacheLocal.put(1, 3);

            tx.commit();

            fail("Should fail prior this line.");
        }
        catch (CacheException ignored) {
            // No-op.
        }

        try (Transaction tx = remote.transactions().txStart()) {
            cacheRemote.put(1, 4);

            tx.commit();

            fail("Should fail prior this line.");
        }
        catch (CacheException ignored) {
            // No-op.
        }
    }

    /**
     *
     */
    @Test
    public void testTimeoutSetRemoteGuarantee() throws Exception {
        Ignite ignite = startGrid(0);
        Ignite remote = startGrid(1);

        IgniteCache cacheLocal = ignite.getOrCreateCache(defaultCacheConfiguration());
        IgniteCache cacheRemote = remote.getOrCreateCache(defaultCacheConfiguration());

        ignite.events().remoteListen(null,
            (IgnitePredicate<Event>)e -> {
                assert e instanceof TransactionStateChangedEvent;

                TransactionStateChangedEvent evt = (TransactionStateChangedEvent)e;

                Transaction tx = evt.tx();

                if (tx.timeout() == 0)
                    tx.setRollbackOnly();

                return true;
            },
            EVT_TX_STARTED);

        try (Transaction tx = ignite.transactions().txStart(
            TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ, 100, 2)) {
            cacheLocal.put(1, 1);

            tx.commit();
        }

        try (Transaction tx = remote.transactions().txStart(
            TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ, 100, 2)) {
            cacheRemote.put(1, 2);

            tx.commit();
        }

        try (Transaction tx = ignite.transactions().txStart()) {
            cacheLocal.put(1, 3);

            tx.commit();

            fail("Should fail prior this line.");
        }
        catch (CacheException ignored) {
            // No-op.
        }

        try (Transaction tx = remote.transactions().txStart()) {
            cacheRemote.put(1, 4);

            tx.commit();

            fail("Should fail prior this line.");
        }
        catch (CacheException ignored) {
            // No-op.
        }
    }

    /**
     *
     */
    @Test
    public void testRollbackInsideLocalListenerAfterRemoteFilter() throws Exception {
        Ignite ignite = startGrid(0);
        Ignite remote = startGrid(1);

        IgniteCache cacheLocal = ignite.getOrCreateCache(defaultCacheConfiguration());
        IgniteCache cacheRemote = remote.getOrCreateCache(defaultCacheConfiguration());

        AtomicBoolean rollbackFailed = new AtomicBoolean();
        AtomicBoolean alreadyRolledBack = new AtomicBoolean();

        ignite.events().remoteListen(
            (IgniteBiPredicate<UUID, Event>)(uuid, e) -> {
                assert e instanceof TransactionStateChangedEvent;

                TransactionStateChangedEvent evt = (TransactionStateChangedEvent)e;

                Transaction tx = evt.tx();

                try {
                    tx.setRollbackOnly();
                }
                catch (IgniteException ignored) {
                    alreadyRolledBack.set(rollbackFailed.getAndSet(true));
                }

                return true;
            },
            (IgnitePredicate<Event>)e -> {
                assert e instanceof TransactionStateChangedEvent;

                return true;
            },
            EVT_TX_STARTED);

        assertFalse(rollbackFailed.get());
        assertFalse(alreadyRolledBack.get());

        try (Transaction tx = ignite.transactions().txStart()) {
            cacheLocal.put(1, 1);

            tx.commit();

            fail("Should fail prior this line.");
        }
        catch (CacheException ignored) {
            // No-op.
        }

        assertFalse(rollbackFailed.get());
        assertFalse(alreadyRolledBack.get());

        try (Transaction tx = remote.transactions().txStart()) {
            cacheRemote.put(1, 2);

            tx.commit();
        }

        assertTrue(GridTestUtils.waitForCondition(rollbackFailed::get, 5_000));

        assertFalse(alreadyRolledBack.get());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }
}
