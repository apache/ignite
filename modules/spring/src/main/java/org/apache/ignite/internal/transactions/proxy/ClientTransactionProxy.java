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

import org.apache.ignite.client.ClientTransaction;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Represents {@link TransactionProxy} implementation that uses {@link ClientTransaction} to perform transaction
 * operations.
 */
public class ClientTransactionProxy implements TransactionProxy {
    /** */
    private final ClientTransaction tx;

    /** */
    public ClientTransactionProxy(ClientTransaction tx) {
        this.tx = tx;
    }

    /** {@inheritDoc} */
    @Override public void commit() {
        tx.commit();
    }

    /** {@inheritDoc} */
    @Override public void rollback() {
        tx.rollback();
    }

    /** {@inheritDoc} */
    @Override public void close() {
        tx.close();
    }

    /** {@inheritDoc} */
    @Override public boolean setRollbackOnly() {
        throw new UnsupportedOperationException("Operation is not supported by thin client.");
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClientTransactionProxy.class, this);
    }
}
