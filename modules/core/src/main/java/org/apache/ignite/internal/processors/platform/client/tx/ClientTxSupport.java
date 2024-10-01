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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.odbc.ClientListenerAbstractConnectionContext;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/** */
public interface ClientTxSupport {
    /** */
    ClientListenerAbstractConnectionContext context();

    /**
     * @return Transaction id.
     */
    default int startClientTransaction(
        TransactionConcurrency concurrency,
        TransactionIsolation isolation,
        long timeout,
        String lb
    ) {
        GridNearTxLocal tx;

        ClientListenerAbstractConnectionContext ctx = context();

        ctx.kernalContext().gateway().readLock();

        try {
            tx = ctx.kernalContext().cache().context().tm().newTx(
                false,
                false,
                null,
                concurrency,
                isolation,
                timeout,
                true,
                0,
                lb
            );
        }
        finally {
            ctx.kernalContext().gateway().readUnlock();
        }

        try {
            tx.suspend();

            int txId = ctx.nextTxId();

            ctx.addTxContext(new ClientTxContext(txId, tx));

            return txId;
        }
        catch (Exception e) {
            try {
                tx.close();
            }
            catch (Exception e1) {
                e.addSuppressed(e1);
            }

            throw startTxException(e);
        }
    }

    /** End transaction asynchronously. */
    default IgniteInternalFuture<IgniteInternalTx> endTxAsync(
        int txId,
        boolean committed
    ) {
        ClientListenerAbstractConnectionContext ctx = context();

        ClientTxContext txCtx = ctx.txContext(txId);

        if (txCtx == null && !committed)
            return new GridFinishedFuture<>();

        if (txCtx == null)
            throw transactionNotFoundException();

        try {
            txCtx.acquire(committed);

            if (committed)
                return txCtx.tx().context().commitTxAsync(txCtx.tx());
            else
                return txCtx.tx().rollbackAsync();
        }
        catch (IgniteCheckedException e) {
            throw endTxException(e);
        }
        finally {
            ctx.removeTxContext(txId);

            try {
                txCtx.release(false);
            }
            catch (IgniteCheckedException ignore) {
                // No-op.
            }
        }
    }

    /** */
    default RuntimeException startTxException(Exception cause) {
        return new UnsupportedOperationException();
    }

    /** */
    default RuntimeException endTxException(IgniteCheckedException cause) {
        return new UnsupportedOperationException();
    }

    /** */
    default RuntimeException transactionNotFoundException() {
        return new UnsupportedOperationException();
    }
}
