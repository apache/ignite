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

package org.apache.ignite.internal.processors.platform.client.tx;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.transactions.TransactionState;

/**
 * Client transaction context.
 */
public class ClientTxContext {
    /** Transaction id. */
    private final int txId;

    /** Transaction. */
    private final GridNearTxLocal tx;

    /** Lock. */
    private final Lock lock = new ReentrantLock();

    /**
     * Constructor.
     */
    public ClientTxContext(int txId, GridNearTxLocal tx) {
        assert txId != 0;
        assert tx != null;

        this.txId = txId;
        this.tx = tx;
    }

    /**
     * Acquire context to work with transaction in the current thread.
     */
    @SuppressWarnings("LockAcquiredButNotSafelyReleased")
    public void acquire(boolean resumeTx) throws IgniteCheckedException {
        lock.lock();

        if (resumeTx)
            tx.resume();
    }

    /**
     * Release context.
     */
    public void release(boolean suspendTx) throws IgniteCheckedException {
        try {
            if (suspendTx) {
                TransactionState state = tx.state();

                if (state != TransactionState.COMMITTED && state != TransactionState.ROLLED_BACK)
                    tx.suspend();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Gets transaction id.
     */
    public int txId() {
        return txId;
    }

    /**
     * Gets transaction.
     */
    public GridNearTxLocal tx() {
        return tx;
    }

    /**
     * Close transaction context.
     */
    public void close() {
        lock.lock();

        try {
            tx.close();
        }
        catch (Exception ignore) {
            // No-op.
        }
        finally {
            lock.unlock();
        }
    }
}
