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

package org.apache.ignite.client.handler.requests.tx;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.ClientResource;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.tx.IgniteTransactions;

/**
 * Client transaction begin request.
 */
public class ClientTransactionBeginRequest {
    /**
     * Processes the request.
     *
     * @param out          Packer.
     * @param transactions Transactions.
     * @param resources    Resources.
     * @return Future.
     */
    public static CompletableFuture<Void> process(
            ClientMessagePacker out,
            IgniteTransactions transactions,
            ClientResourceRegistry resources) {
        return transactions.beginAsync().thenAccept(t -> out.packLong(resources.put(new ClientResource(t, t::rollback))));
    }
}
