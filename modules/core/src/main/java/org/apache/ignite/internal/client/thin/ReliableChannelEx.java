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

import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.client.ClientAuthenticationException;
import org.apache.ignite.client.ClientAuthorizationException;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClientFuture;

/**
 * Interface for communication channel with failover and partition awareness.
 */
interface ReliableChannelEx extends AutoCloseable {
    /**
     * Send request and handle response.
     *
     * @throws ClientException Thrown by {@code payloadWriter} or {@code payloadReader}.
     * @throws ClientAuthenticationException When user name or password is invalid.
     * @throws ClientAuthorizationException When user has no permission to perform operation.
     * @throws ClientProtocolError When failed to handshake with server.
     * @throws ClientServerError When failed to process request on server.
     */
    public <T> T service(
        ClientOperation op,
        Consumer<PayloadOutputChannel> payloadWriter,
        Function<PayloadInputChannel, T> payloadReader
    ) throws ClientException, ClientError;

    /**
     * Send request to one of the passed nodes and handle response.
     *
     * @throws ClientException Thrown by {@code payloadWriter} or {@code payloadReader}.
     * @throws ClientAuthenticationException When user name or password is invalid.
     * @throws ClientAuthorizationException When user has no permission to perform operation.
     * @throws ClientProtocolError When failed to handshake with server.
     * @throws ClientServerError When failed to process request on server.
     */
    public <T> T service(
        ClientOperation op,
        Consumer<PayloadOutputChannel> payloadWriter,
        Function<PayloadInputChannel, T> payloadReader,
        List<UUID> targetNodes
    ) throws ClientException, ClientError;

    /**
     * Send request and handle response asynchronously.
     */
    public <T> IgniteClientFuture<T> serviceAsync(
        ClientOperation op,
        Consumer<PayloadOutputChannel> payloadWriter,
        Function<PayloadInputChannel, T> payloadReader
    ) throws ClientException, ClientError;

    /**
     * Send request to affinity node and handle response.
     */
    public <T> T affinityService(
        int cacheId,
        Object key,
        ClientOperation op,
        Consumer<PayloadOutputChannel> payloadWriter,
        Function<PayloadInputChannel, T> payloadReader
    ) throws ClientException, ClientError;

    /**
     * Send request to affinity node and handle response.
     */
    public <T> T affinityService(
        int cacheId,
        int part,
        ClientOperation op,
        Consumer<PayloadOutputChannel> payloadWriter,
        Function<PayloadInputChannel, T> payloadReader
    ) throws ClientException, ClientError;

    /**
     * Send request to affinity node and handle response asynchronously.
     */
    public <T> IgniteClientFuture<T> affinityServiceAsync(
        int cacheId,
        Object key,
        ClientOperation op,
        Consumer<PayloadOutputChannel> payloadWriter,
        Function<PayloadInputChannel, T> payloadReader
    ) throws ClientException, ClientError;

    /**
     * Send request without payload and handle response.
     */
    public default <T> T service(
        ClientOperation op,
        Function<PayloadInputChannel, T> payloadReader
    ) throws ClientException, ClientError {
        return service(op, null, payloadReader);
    }

    /**
     * Send request without payload and handle response asynchronously.
     */
    public default <T> IgniteClientFuture<T> serviceAsync(
        ClientOperation op,
        Function<PayloadInputChannel, T> payloadReader
    ) throws ClientException, ClientError {
        return serviceAsync(op, null, payloadReader);
    }

    /**
     * Send request and handle response without payload.
     */
    public default void request(
        ClientOperation op,
        Consumer<PayloadOutputChannel> payloadWriter
    ) throws ClientException, ClientError {
        service(op, payloadWriter, null);
    }

    /**
     * Send request and handle response without payload asynchronously.
     */
    public default IgniteClientFuture<Void> requestAsync(
        ClientOperation op,
        Consumer<PayloadOutputChannel> payloadWriter
    ) throws ClientException, ClientError {
        return serviceAsync(op, payloadWriter, null);
    }
}
