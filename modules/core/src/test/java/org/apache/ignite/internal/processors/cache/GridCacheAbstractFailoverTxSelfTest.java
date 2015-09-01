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

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public abstract class GridCacheAbstractFailoverTxSelfTest extends GridCacheAbstractFailoverSelfTest {
    /**
     * @throws Exception If failed.
     */
    public void testOptimisticReadCommittedTxConstantTopologyChange() throws Exception {
        testConstantTopologyChange(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticRepeatableReadTxConstantTopologyChange() throws Exception {
        testConstantTopologyChange(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticSerializableTxConstantTopologyChange() throws Exception {
        testConstantTopologyChange(OPTIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticReadCommittedTxConstantTopologyChange() throws Exception {
        testConstantTopologyChange(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticRepeatableReadTxConstantTopologyChange() throws Exception {
        testConstantTopologyChange(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticSerializableTxConstantTopologyChange() throws Exception {
        testConstantTopologyChange(PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticReadCommittedTxTopologyChange() throws Exception {
        testTopologyChange(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticRepeatableReadTxTopologyChange() throws Exception {
        testTopologyChange(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticSerializableTxTopologyChange() throws Exception {
        testTopologyChange(OPTIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticReadCommittedTxTopologyChange() throws Exception {
        testTopologyChange(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticRepeatableReadTxTopologyChange() throws Exception {
        testTopologyChange(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticSerializableTxTopologyChange() throws Exception {
        testTopologyChange(PESSIMISTIC, SERIALIZABLE);
    }
}