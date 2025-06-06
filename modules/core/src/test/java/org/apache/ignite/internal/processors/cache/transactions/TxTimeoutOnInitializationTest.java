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

import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientTransaction;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests transaction timeout during initialization.
 */
public class TxTimeoutOnInitializationTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxTimeoutOnInitialization() throws Exception {
        long txTimeout = 500L;

        IgniteEx ignite0 = startGrid(0);

        GridCacheSharedContext<?, ?> sharedCtx = ignite0.context().cache().context();

        IgniteTxManager tm = Mockito.spy(sharedCtx.tm());
        sharedCtx.setTxManager(tm);

        Mockito.doAnswer(m -> {
            doSleep(txTimeout * 2);

            return m.callRealMethod();
        }).when(tm).onCreated(Mockito.any(), Mockito.any());

        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, txTimeout, 1)) {
            // No-op.
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("timed out"));
        }

        assertTrue(GridTestUtils.waitForCondition(() -> tm.activeTransactions().isEmpty(), 1_000L));

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER))) {
            try (ClientTransaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, txTimeout)) {
                // No-op.
            }
            catch (Exception e) {
                assertTrue(e.getMessage().contains("timed out"));
            }
        }

        assertTrue(GridTestUtils.waitForCondition(() -> tm.activeTransactions().isEmpty(), 1_000L));
    }
}
