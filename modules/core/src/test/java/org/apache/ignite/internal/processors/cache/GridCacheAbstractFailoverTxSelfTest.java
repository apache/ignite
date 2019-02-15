/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache;

import org.junit.Test;

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
    @Test
    public void testOptimisticReadCommittedTxConstantTopologyChange() throws Exception {
        testConstantTopologyChange(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticRepeatableReadTxConstantTopologyChange() throws Exception {
        testConstantTopologyChange(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticSerializableTxConstantTopologyChange() throws Exception {
        testConstantTopologyChange(OPTIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticReadCommittedTxConstantTopologyChange() throws Exception {
        testConstantTopologyChange(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticRepeatableReadTxConstantTopologyChange() throws Exception {
        testConstantTopologyChange(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticSerializableTxConstantTopologyChange() throws Exception {
        testConstantTopologyChange(PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticReadCommittedTxTopologyChange() throws Exception {
        testTopologyChange(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticRepeatableReadTxTopologyChange() throws Exception {
        testTopologyChange(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticSerializableTxTopologyChange() throws Exception {
        testTopologyChange(OPTIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticReadCommittedTxTopologyChange() throws Exception {
        testTopologyChange(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticRepeatableReadTxTopologyChange() throws Exception {
        testTopologyChange(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticSerializableTxTopologyChange() throws Exception {
        testTopologyChange(PESSIMISTIC, SERIALIZABLE);
    }
}
