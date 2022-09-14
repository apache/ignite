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

package org.apache.ignite.internal.transactions.proxy;

import org.apache.ignite.client.ClientTransactions;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 *  Represents {@link TransactionProxyFactory} implementation that uses Ignite thin client transaction facade to start
 *  new transaction.
 */
public class ClientTransactionProxyFactory implements TransactionProxyFactory {
    /** */
    private final ClientTransactions txs;

    /** */
    public ClientTransactionProxyFactory(ClientTransactions txs) {
        this.txs = txs;
    }

    /** {@inheritDoc} */
    @Override public TransactionProxy txStart(
        TransactionConcurrency concurrency,
        TransactionIsolation isolation,
        long timeout
    ) {
        return new ClientTransactionProxy(txs.txStart(concurrency, isolation, timeout));
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object other) {
        if (this == other)
            return true;

        if (other == null || getClass() != other.getClass())
            return false;

        return txs.equals(((ClientTransactionProxyFactory)other).txs);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return txs.hashCode();
    }
}
