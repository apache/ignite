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

package org.apache.ignite.network;

import java.util.concurrent.CompletableFuture;

/**
 * Entry point for sending messages between network members in both weak and patient mode.
 *
 * TODO: allow removing event handlers, see https://issues.apache.org/jira/browse/IGNITE-14519
 */
public interface MessagingService {
    /**
     * Tries to send the given message asynchronously to the specific member without any delivery guarantees.
     *
     * @param recipient Recipient of the message.
     * @param msg Message which should be delivered.
     */
    void weakSend(ClusterNode recipient, NetworkMessage msg);

    /**
     * Tries to send the given message asynchronously to the specific cluster member with the following guarantees:
     * <ul>
     *     <li>Messages will be delivered in the same order as they were sent;</li>
     *     <li>If a message N has been successfully delivered to a member implies that all messages preceding N
     *     have also been successfully delivered.</li>
     * </ul>
     *
     * @param recipient Recipient of the message.
     * @param msg Message which should be delivered.
     */
    CompletableFuture<Void> send(ClusterNode recipient, NetworkMessage msg);

    /**
     * Same as {@link #send(ClusterNode, NetworkMessage)} but attaches the given correlation ID to the given message.
     *
     * @param recipient Recipient of the message.
     * @param msg Message which should be delivered.
     * @param correlationId Correlation id when replying to the request.
     */
    CompletableFuture<Void> send(ClusterNode recipient, NetworkMessage msg, String correlationId);

    /**
     * Sends a message asynchronously with same guarantees as {@link #send(ClusterNode, NetworkMessage)} and
     * returns a future that will be completed successfully upon receiving a response.
     *
     * @param recipient Recipient of the message.
     * @param msg A message.
     * @param timeout Waiting for response timeout in milliseconds.
     * @return A future holding the response or error if the expected response was not received.
     */
    CompletableFuture<NetworkMessage> invoke(ClusterNode recipient, NetworkMessage msg, long timeout);

    /**
     * Registers a handler for network message events.
     */
    void addMessageHandler(NetworkMessageHandler handler);
}
