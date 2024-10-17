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
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.odbc.ClientTxSupport;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.apache.ignite.internal.processors.platform.client.IgniteClientException;
import org.apache.ignite.internal.util.lang.GridClosureException;

/**
 * End the transaction request.
 */
public class ClientTxEndRequest extends ClientRequest implements ClientTxSupport {
    /** Transaction id. */
    private final int txId;

    /** Transaction committed. */
    private final boolean committed;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientTxEndRequest(BinaryRawReader reader) {
        super(reader);

        txId = reader.readInt();
        committed = reader.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public boolean isAsync(ClientConnectionContext ctx) {
        return true;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<ClientResponse> processAsync(ClientConnectionContext ctx) {
        return endTxAsync(ctx, txId, committed).chain(f -> {
            if (f.error() != null)
                throw new GridClosureException(f.error());
            else
                return process(ctx);
        });
    }

    /** {@inheritDoc} */
    @Override public RuntimeException transactionNotFoundException() {
        return new IgniteClientException(ClientStatus.TX_NOT_FOUND, "Transaction with id " + txId + " not found.");
    }

    /** {@inheritDoc} */
    @Override public RuntimeException endTxException(IgniteCheckedException cause) {
        return new IgniteClientException(ClientStatus.FAILED, cause.getMessage(), cause);
    }
}
