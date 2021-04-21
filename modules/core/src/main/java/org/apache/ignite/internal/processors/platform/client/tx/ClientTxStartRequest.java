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
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
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
    /** Transaction concurrency control. */
    private final TransactionConcurrency concurrency;

    /** Transaction isolation level. */
    private final TransactionIsolation isolation;

    /** Transaction timeout. */
    private final long timeout;

    /** Transaction label. */
    private final String lb;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientTxStartRequest(BinaryRawReader reader) {
        super(reader);

        concurrency = TransactionConcurrency.fromOrdinal(reader.readByte());
        isolation = TransactionIsolation.fromOrdinal(reader.readByte());
        timeout = reader.readLong();
        lb = reader.readString();
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        GridNearTxLocal tx;

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
                null,
                0,
                lb,
                false
            );
        }
        finally {
            ctx.kernalContext().gateway().readUnlock();
        }

        try {
            tx.suspend();

            int txId = ctx.nextTxId();

            ctx.addTxContext(new ClientTxContext(txId, tx));

            return new ClientIntResponse(requestId(), txId);
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
}
