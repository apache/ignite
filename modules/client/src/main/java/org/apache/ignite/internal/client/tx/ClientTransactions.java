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

package org.apache.ignite.internal.client.tx;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.Transaction;

/**
 * Client transactions implementation.
 */
public class ClientTransactions implements IgniteTransactions {
    /** Channel. */
    private final ReliableChannel ch;

    /**
     * Constructor.
     *
     * @param ch Channel.
     */
    public ClientTransactions(ReliableChannel ch) {
        this.ch = ch;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteTransactions withTimeout(long timeout) {
        // TODO: IGNITE-16193
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public Transaction begin() {
        return beginAsync().join();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Transaction> beginAsync() {
        return ch.serviceAsync(ClientOp.TX_BEGIN, w -> {},  r -> new ClientTransaction(r.clientChannel(), r.in().unpackLong()));
    }
}
