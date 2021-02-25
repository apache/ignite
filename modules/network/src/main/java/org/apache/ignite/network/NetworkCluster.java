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

import java.util.Collection;
import java.util.concurrent.Future;

/**
 * Main interface for interaction with network. It allows to get information about network members and send messages to
 * them.
 */
public interface NetworkCluster {
    /**
     * Stop the processing of network connection immediately. Sending and receiving messages or obtaining network
     * members information after this method successfully finished will be impossible.
     *
     * @throws Exception If something went wrong.
     */
    void shutdown() throws Exception;

    /**
     * @return Information about local network member.
     */
    NetworkMember localMember();

    /**
     * @return Information about all members which have seen by the local member(including local member itself).
     */
    Collection<NetworkMember> allMembers();

    /**
     * Try to send the message asynchronously to the specific member without any guarantees that this message would be
     * delivered.
     *
     * @param member Netwrok member which should receive the message.
     * @param msg Message which should be delivered.
     */
    void weakSend(NetworkMember member, Object msg);

    /**
     * Try to send the message asynchronously to the specific member with next guarantees:
     * * Messages which was sent from one thread to one member will be delivered in the same order as they were sent.
     * * If message N was successfully delivered to the member that means all messages preceding N also were successfully delivered.
     *
     * @param member Network member which should receive the message.
     * @param msg Message which should be delivered.
     */
    Future<?> guaranteedSend(NetworkMember member, Object msg);

    /**
     * Add provider which allows to get configured handlers for different cluster events(ex. received message).
     *
     * @param networkHandlersProvider Provider for obtaining cluster event handlers.
     */
    void addHandlersProvider(NetworkHandlersProvider networkHandlersProvider);
}
