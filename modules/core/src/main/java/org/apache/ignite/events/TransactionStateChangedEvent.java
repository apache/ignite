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

package org.apache.ignite.events;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.transactions.Transaction;

/**
 * Event indicates transaction state change.
 *
 * @see EventType#EVTS_TX
 */
public class TransactionStateChangedEvent extends EventAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** Tx. */
    private Transaction tx;

    /**
     * @param node Node.
     * @param msg Message.
     * @param type Type.
     * @param tx Tx.
     */
    public TransactionStateChangedEvent(ClusterNode node, String msg, int type, Transaction tx) {
        super(node, msg, type);

        assert tx != null;

        this.tx = tx;
    }

    /**
     * Provides transaction proxy allows all 'get' operations such as {@link Transaction#label()}
     * and also {@link Transaction#setRollbackOnly()} method.
     */
    public Transaction tx() {
        return tx;
    }
}
