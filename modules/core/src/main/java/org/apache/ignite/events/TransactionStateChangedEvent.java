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
