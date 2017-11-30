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

package org.apache.ignite.transactions.spring;

import org.apache.ignite.transactions.Transaction;
import org.springframework.transaction.support.ResourceHolderSupport;

/**
 * A {@link org.springframework.transaction.support.ResourceHolder} for the Ignite {@link Transaction} to
 * associate the transaction with a Spring transaction manager.
 */
class IgniteTransactionHolder extends ResourceHolderSupport {
    /** */
    private Transaction transaction;

    /** */
    private boolean transactionActive;

    /**
     * Constructs the transaction holder.
     *
     * @param transaction the transaction to hold
     */
    IgniteTransactionHolder(Transaction transaction) {
        this.transaction = transaction;
    }

    /**
     * Returns true if the holder is holding a transaction.
     *
     * @return true if holding a transaction
     */
    public boolean hasTransaction() {
        return this.transaction != null;
    }

    /**
     * Sets the transaction to be held in the resource holder.
     *
     * @param transaction the transaction
     */
    void setTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    /**
     * Returns the transaction in the holder or null if none has been set.
     *
     * @return the transaction or null
     */
    Transaction getTransaction() {
        return this.transaction;
    }

    /**
     * Return whether this holder represents an active, Ignite-managed
     * transaction.
     *
     * @return true if a transaction is active
     */
    protected boolean isTransactionActive() {
        return this.transactionActive;
    }

    /**
     * Set whether this holder represents an active, Ignite-managed
     * transaction.
     *
     * @param transactionActive true if a transaction is active
     */
    protected void setTransactionActive(boolean transactionActive) {
        this.transactionActive = transactionActive;
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        super.clear();

        transactionActive = false;
        transaction.close();
    }
}
