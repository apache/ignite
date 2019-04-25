/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCheckedException;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests for local transactions.
 */
public abstract class IgniteMvccTxSingleThreadedAbstractTest extends IgniteTxAbstractTest {
    /**
     * @throws IgniteCheckedException If test failed.
     */
    @Test
    public void testPessimisticRepeatableReadCommit() throws Exception {
        checkCommit(PESSIMISTIC, REPEATABLE_READ);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    @Test
    public void testPessimisticRepeatableReadRollback() throws Exception {
        checkRollback(PESSIMISTIC, REPEATABLE_READ);

        finalChecks();
    }
}
