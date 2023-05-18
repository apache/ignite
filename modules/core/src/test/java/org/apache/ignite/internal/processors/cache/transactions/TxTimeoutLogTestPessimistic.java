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

import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockResponse;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;

/**
 * Tests timeout log content of pessimistic transaction.
 */
@RunWith(Parameterized.class)
public class TxTimeoutLogTestPessimistic extends AbstractTxTimeoutLogTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        txConcurrency = PESSIMISTIC;

        super.beforeTest();
    }

    /**
     * Test primary node not responding on lock request.
     */
    @Test
    public void testPrimaryDelaysOnLock() throws Exception {
        doTest(true, GridNearLockResponse.class);
    }

    /** Test no 'not responded nodes' message is when primary just leaves on locks phase before prepare phase. */
    @Test
    public void testPrimaryLeftOnLocks() throws Exception {
        doTestPrimaryLeft(GridNearLockResponse.class);
    }
}

