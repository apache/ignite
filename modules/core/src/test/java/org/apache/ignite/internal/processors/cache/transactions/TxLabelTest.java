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

import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.TransactionEvent;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.events.EventType.EVTS_TX;
import static org.apache.ignite.events.EventType.EVT_TX_STARTED;

/**
 * Tests transaction labels.
 */
public class TxLabelTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /**
     * Tests transaction labels.
     */
    public void testLabel() {
        testLabel0(grid(0), "lbl0");
        testLabel0(grid(0), "lbl1");

        try {
            testLabel0(grid(0), null);

            fail();
        }
        catch (Exception e) {
            // Expected.
        }
    }

    /**
     * @param ignite Ignite.
     * @param lbl Label.
     */
    private void testLabel0(Ignite ignite, String lbl) {
        try (Transaction tx = ignite.transactions().withLabel(lbl).txStart()) {
            assertEquals(lbl, tx.label());

            ignite.cache(DEFAULT_CACHE_NAME).put(0, 0);

            tx.commit();
        }
    }

    /**
     *
     */
    public void testLabelFilledCheck() {
        Ignite ignite = grid(0);

        final IgniteEvents evts = ignite.events();

        evts.enableLocal(EVTS_TX);

        AtomicReference<Throwable> ref = new AtomicReference<>();

        evts.localListen((IgnitePredicate<Event>)e -> {
            assert e instanceof TransactionEvent;

            TransactionEvent evt = (TransactionEvent)e;

            if (((GridNearTxLocal)(evt.tx())).label() == null)
                ref.set(new IllegalStateException("Label should be filled!"));

            return true;
        }, EVT_TX_STARTED);

        assertNull(ref.get());

        try (Transaction tx = ignite.transactions().withLabel("test").txStart()) {
            // No-op.
        }

        assertNull(ref.get());

        try (Transaction tx = ignite.transactions().txStart()) {
            // No-op.
        }

        assertNotNull(ref.get());
    }

    /**
     *
     */
    public void testLabelFilledGuarantee() {
        Ignite ignite = grid(0);

        final IgniteEvents evts = ignite.events();

        evts.enableLocal(EVTS_TX);

        class LabelNotFilledError extends Error {
            private LabelNotFilledError(String msg) {
                super(msg);
            }
        }

        evts.localListen((IgnitePredicate<Event>)e -> {
            assert e instanceof TransactionEvent;

            TransactionEvent evt = (TransactionEvent)e;

            if (((GridNearTxLocal)(evt.tx())).label() == null)
                throw new LabelNotFilledError("Label should be filled!");

            return true;
        }, EVT_TX_STARTED);

        try (Transaction tx = ignite.transactions().withLabel("test").txStart()) {
            // No-op.
        }

        try (Transaction tx = ignite.transactions().txStart()) {
            fail("Should fail prior this line.");
        }
        catch (LabelNotFilledError e) {
            // No-op.
        }
    }
}
