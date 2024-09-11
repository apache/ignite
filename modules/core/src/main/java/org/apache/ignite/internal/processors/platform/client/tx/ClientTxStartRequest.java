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

import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.odbc.ClientListenerAbstractConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientIntResponse;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.apache.ignite.internal.processors.platform.client.IgniteClientException;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 * Start transaction request.
 */
public class ClientTxStartRequest extends ClientRequest {
    /** */
    private final ClientTransactionData data;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientTxStartRequest(BinaryRawReader reader) {
        super(reader);

        data = ClientTransactionData.read(reader);
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        return new ClientIntResponse(requestId(), startClientTransaction(ctx, data));
    }

    /**
     * @param ctx Client connection context.
     * @param data Transaction data from client.
     * @return Transaction id.
     */
    public static int startClientTransaction(ClientListenerAbstractConnectionContext ctx, ClientTransactionData data) {
        GridNearTxLocal tx;

        ctx.kernalContext().gateway().readLock();

        try {
            tx = ctx.kernalContext().cache().context().tm().newTx(
                false,
                false,
                null,
                data.concurrency,
                data.isolation,
                data.timeout,
                true,
                0,
                data.lb
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

            throw (e instanceof IgniteClientException) ? (IgniteClientException)e :
                new IgniteClientException(ClientStatus.FAILED, e.getMessage(), e);
        }
    }

    /**
     * Common data for strating client transaction.
     */
    public static class ClientTransactionData {
        /** Transaction concurrency control. */
        private final TransactionConcurrency concurrency;

        /** Transaction isolation level. */
        private final TransactionIsolation isolation;

        /** Transaction timeout. */
        private final long timeout;

        /** Transaction label. */
        private final String lb;

        /** */
        public ClientTransactionData(TransactionConcurrency concurrency, TransactionIsolation isolation, long timeout, String lb) {
            this.concurrency = concurrency;
            this.isolation = isolation;
            this.timeout = timeout;
            this.lb = lb;
        }

        /** */
        public void write(BinaryRawWriter writer) {
            write(writer, concurrency, isolation, timeout, lb);
        }

        /** */
        public static ClientTransactionData read(BinaryRawReader reader) {
            return new ClientTransactionData(
                TransactionConcurrency.fromOrdinal(reader.readByte()),
                TransactionIsolation.fromOrdinal(reader.readByte()),
                reader.readLong(),
                reader.readString()
            );
        }

        /** */
        public static void write(
            BinaryRawWriter writer,
            TransactionConcurrency concurrency,
            TransactionIsolation isolation,
            long timeout,
            String lb
        ) {
            writer.writeByte((byte)concurrency.ordinal());
            writer.writeByte((byte)isolation.ordinal());
            writer.writeLong(timeout);
            writer.writeString(lb);
        }
    }
}
