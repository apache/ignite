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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.TransactionStateChangedEvent;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionRollbackException;

import static org.apache.ignite.events.EventType.EVTS_TX;
import static org.apache.ignite.events.EventType.EVT_TX_STARTED;

/**
 * Tests transaction rollback on incorrect tx params.
 */
public class TxRollbackOnIncorrectParamsTest extends GridCommonAbstractTest {
    /**
     *
     */
    public void testTimeoutSetLocalGuarantee() throws Exception {
        Ignite ignite = startGrid(0);

        final IgniteEvents evts = ignite.events();

        evts.enableLocal(EVTS_TX);

        evts.localListen((IgnitePredicate<Event>)e -> {
            assert e instanceof TransactionStateChangedEvent;

            TransactionStateChangedEvent evt = (TransactionStateChangedEvent)e;

            Transaction tx = evt.tx();

            if (tx.timeout() < 200)
                tx.rollback();

            return true;
        }, EVT_TX_STARTED);

        IgniteCache cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        try (Transaction tx = ignite.transactions().txStart(
            TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ, 200, 2)) {
            cache.put(1,1);

            tx.commit();
        }

        try (Transaction tx = ignite.transactions().txStart(
            TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ, 100, 2)) {
            cache.put(1,2);

            tx.commit();

            fail("Should fail prior this line.");
        }
        catch (TransactionRollbackException ignored) {
            // No-op.
        }

        try (Transaction tx = ignite.transactions().txStart()) {
            cache.put(1,3);

            tx.commit();

            fail("Should fail prior this line.");
        }
        catch (TransactionRollbackException ignored) {
            // No-op.
        }
    }

    /**
     *
     */
    public void testLabelFilledLocalGuarantee() throws Exception {
        Ignite ignite = startGrid(0);

        final IgniteEvents evts = ignite.events();

        evts.enableLocal(EVTS_TX);

        evts.localListen((IgnitePredicate<Event>)e -> {
            assert e instanceof TransactionStateChangedEvent;

            TransactionStateChangedEvent evt = (TransactionStateChangedEvent)e;

            Transaction tx = evt.tx();

            if (tx.label() == null)
                tx.rollback();

            return true;
        }, EVT_TX_STARTED);

        IgniteCache cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        try (Transaction tx = ignite.transactions().withLabel("test").txStart()) {
            cache.put(1,1);

            tx.commit();
        }

        try (Transaction tx = ignite.transactions().txStart()) {
            cache.put(1,2);

            tx.commit();

            fail("Should fail prior this line.");
        }
        catch (TransactionRollbackException ignored) {
            // No-op.
        }
    }

    /**
     *
     */
    public void testLabelFilledRemoteGuarantee() throws Exception {
        Ignite ignite = startGrid(0);
        Ignite remote = startGrid(1);

        final IgniteEvents evts = ignite.events();

        evts.enableLocal(EVTS_TX);

        evts.remoteListen(null,
            (IgnitePredicate<Event>)e -> {
                assert e instanceof TransactionStateChangedEvent;

                TransactionStateChangedEvent evt = (TransactionStateChangedEvent)e;

                Transaction tx = evt.tx();

                if (tx.label() == null)
                    tx.rollback();

                return true;
            },
            EVT_TX_STARTED);

        IgniteCache cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        try (Transaction tx = ignite.transactions().withLabel("test").txStart()) {
            cache.put(1,1);

            tx.commit();
        }

        try (Transaction tx = remote.transactions().withLabel("test").txStart()) {
            cache.put(1,2);

            tx.commit();
        }

        try (Transaction tx = ignite.transactions().txStart()) {
            cache.put(1,3);

            tx.commit();

            fail("Should fail prior this line.");
        }
        catch (TransactionRollbackException ignored) {
            // No-op.
        }

        try (Transaction tx = remote.transactions().txStart()) {
            cache.put(1,4);

            tx.commit();

            fail("Should fail prior this line.");
        }
        catch (TransactionRollbackException ignored) {
            // No-op.
        }
    }

    /**
     *
     */
    public void testTimeoutSetRemoteGuarantee() throws Exception {
        Ignite ignite = startGrid(0);
        Ignite remote = startGrid(1);

        final IgniteEvents evts = ignite.events();

        evts.enableLocal(EVTS_TX);

        evts.remoteListen(null,
            (IgnitePredicate<Event>)e -> {
                assert e instanceof TransactionStateChangedEvent;

                TransactionStateChangedEvent evt = (TransactionStateChangedEvent)e;

                Transaction tx = evt.tx();

                if (tx.timeout() == 0)
                    tx.rollback();

                return true;
            },
            EVT_TX_STARTED);

        IgniteCache cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        try (Transaction tx = ignite.transactions().txStart(
            TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ, 100, 2)) {
            cache.put(1,1);

            tx.commit();
        }

        try (Transaction tx = remote.transactions().txStart(
            TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ, 100, 2)) {
            cache.put(1,2);

            tx.commit();
        }

        try (Transaction tx = ignite.transactions().txStart()) {
            cache.put(1,3);

            tx.commit();

            fail("Should fail prior this line.");
        }
        catch (TransactionRollbackException ignored) {
            // No-op.
        }

        try (Transaction tx = remote.transactions().txStart()) {
            cache.put(1,4);

            tx.commit();

            fail("Should fail prior this line.");
        }
        catch (TransactionRollbackException ignored) {
            // No-op.
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }
}
