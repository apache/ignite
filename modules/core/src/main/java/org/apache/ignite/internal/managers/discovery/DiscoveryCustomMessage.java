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

package org.apache.ignite.internal.managers.discovery;

import java.io.Serializable;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddedMessage;
import org.jetbrains.annotations.Nullable;

/**
 * <b>DiscoveryCustomMessage</b> messages are handled by discovery protocol which provides some guarantees around them.
 *
 * When some node sends <b>DiscoveryCustomMessage</b> with {@link GridDiscoveryManager#sendCustomEvent(DiscoveryCustomMessage)}
 * call, message firstly goes to current coordinator, is verified there and after that gets sent to the cluster.
 * Only after verification it is delivered to listeners on all nodes starting from coordinator.
 *
 * To register a listener {@link GridDiscoveryManager#setCustomEventListener(Class, CustomEventListener)} method is used.
 *
 * Discovery protocol guarantees include:
 * <ol>
 *     <li>
 *         All discovery messages are observed by all nodes in exactly the same order,
 *         it is guaranteed by handling them in single-threaded mode.
 *     </li>
 *     <li>
 *         New server node joining process in default implementation involves two passes of different messages across the cluster:
 *         {@link TcpDiscoveryNodeAddedMessage} and
 *         {@link TcpDiscoveryNodeAddFinishedMessage} messages.
 *         It is guaranteed that all discovery messages observed by coordinator in between these two messages
 *         are reordered and guaranteed to be delivered to newly joined node.
 *     </li>
 * </ol>
 *
 * Yet there are some features and limitations one should be aware of when using custom discovery messaging mechanism:
 * <ol>
 *     <li>
 *         Guarantee #2 doesn't encompass <b>DiscoveryCustomMessage</b>s created automatically on
 *         {@link DiscoveryCustomMessage#ackMessage()} method call.
 *
 *         If there were messages of this type in between <b>TcpDiscoveryNodeAddedMessage</b> and
 *         <b>TcpDiscoveryNodeAddFinishedMessage</b> messages, they won't be delivered to new joiner node.
 *     </li>
 *     <li>
 *         There is no guarantee for a given <b>DiscoveryCustomMessage</b> to be delivered only once.
 *         It is possible that because of node failure antecedent node will resend messages
 *         it thinks were not sent by failed node.
 *         Duplicate messages are not filtered out on receiver side.
 *     </li>
 *     <li>
 *         <b>DiscoveryCustomMessage</b>s are delivered to client nodes in asynchronous fashion
 *         as clients don't participate in the cluster ring.
 *     </li>
 *     <li>
 *         Any blocking operations like obtaining locks or doing I/O <b>must</b> be avoided in message handlers
 *         as they may lead to deadlocks and cluster failures.
 *     </li>
 * </ol>
 */
public interface DiscoveryCustomMessage extends Serializable {
    /**
     * @return Unique custom message ID.
     */
    public IgniteUuid id();

    /**
     * Called when custom message has been handled by all nodes.
     *
     * @return Ack message or {@code null} if ack is not required.
     */
    @Nullable public DiscoveryCustomMessage ackMessage();

    /**
     * @return {@code True} if message can be modified during listener notification. Changes will be sent to next nodes.
     */
    public boolean isMutable();

    /**
     * Creates new discovery cache if message caused topology version change.
     *
     * @param mgr Discovery manager.
     * @param topVer New topology version.
     * @param discoCache Current discovery cache.
     * @return Reused discovery cache.
     */
    public DiscoCache createDiscoCache(GridDiscoveryManager mgr, AffinityTopologyVersion topVer, DiscoCache discoCache);
}
