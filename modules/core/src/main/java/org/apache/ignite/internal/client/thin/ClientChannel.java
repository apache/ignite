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

package org.apache.ignite.internal.client.thin;

import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.client.ClientAuthorizationException;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;

/**
 * Processing thin client requests and responses.
 */
interface ClientChannel extends AutoCloseable {
    /**
     * Send request and handle response for client operation.
     *
     * @param op Operation.
     * @param payloadWriter Payload writer to stream or {@code null} if request has no payload.
     * @param payloadReader Payload reader from stream.
     * @return Received operation payload or {@code null} if response has no payload.
     * @throws ClientException Thrown by {@code payloadWriter} or {@code payloadReader}.
     * @throws ClientAuthorizationException When user has no permission to perform operation.
     * @throws ClientServerError When failed to process request on server.
     * @throws ClientConnectionException In case of IO errors.
     */
    public <T> T service(
        ClientOperation op,
        Consumer<PayloadOutputChannel> payloadWriter,
        Function<PayloadInputChannel, T> payloadReader
    ) throws ClientException, ClientAuthorizationException, ClientServerError, ClientConnectionException;

    /**
     * @return Protocol context.
     */
    public ProtocolContext protocolCtx();

    /**
     * @return Server node ID.
     */
    public UUID serverNodeId();

    /**
     * @return Server topology version.
     */
    public AffinityTopologyVersion serverTopologyVersion();

    /**
     * Add topology change listener.
     */
    public void addTopologyChangeListener(Consumer<ClientChannel> lsnr);

    /**
     * Add notifications (from server to client) listener.
     */
    public void addNotificationListener(NotificationListener lsnr);

    /**
     * @return {@code True} channel is closed.
     */
    public boolean closed();
}
