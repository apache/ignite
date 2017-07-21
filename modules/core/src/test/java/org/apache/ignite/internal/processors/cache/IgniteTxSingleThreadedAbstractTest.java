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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCheckedException;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Tests for local transactions.
 */
@SuppressWarnings( {"BusyWait"})
public abstract class IgniteTxSingleThreadedAbstractTest extends IgniteTxAbstractTest {
    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testPessimisticReadCommittedCommit() throws Exception {
        checkCommit(PESSIMISTIC, READ_COMMITTED);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testPessimisticRepeatableReadCommit() throws Exception {
        checkCommit(PESSIMISTIC, REPEATABLE_READ);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testPessimisticSerializableCommit() throws Exception {
        checkCommit(PESSIMISTIC, SERIALIZABLE);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testOptimisticReadCommittedCommit() throws Exception {
        checkCommit(OPTIMISTIC, READ_COMMITTED);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testOptimisticRepeatableReadCommit() throws Exception {
        checkCommit(OPTIMISTIC, REPEATABLE_READ);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testOptimisticSerializableCommit() throws Exception {
        checkCommit(OPTIMISTIC, SERIALIZABLE);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testPessimisticReadCommittedRollback() throws Exception {
        checkRollback(PESSIMISTIC, READ_COMMITTED);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testPessimisticRepeatableReadRollback() throws Exception {
        checkRollback(PESSIMISTIC, REPEATABLE_READ);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testPessimisticSerializableRollback() throws Exception {
        checkRollback(PESSIMISTIC, SERIALIZABLE);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testOptimisticReadCommittedRollback() throws Exception {
        checkRollback(OPTIMISTIC, READ_COMMITTED);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testOptimisticRepeatableReadRollback() throws Exception {
        checkRollback(OPTIMISTIC, REPEATABLE_READ);

        finalChecks();
    }

    /**
     * @throws IgniteCheckedException If test failed.
     */
    public void testOptimisticSerializableRollback() throws Exception {
        checkRollback(OPTIMISTIC, SERIALIZABLE);

        finalChecks();
    }
}