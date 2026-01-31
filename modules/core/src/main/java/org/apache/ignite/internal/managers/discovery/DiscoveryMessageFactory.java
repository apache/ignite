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

import org.apache.ignite.internal.codegen.CacheStatisticsModeChangeMessageSerializer;
import org.apache.ignite.internal.codegen.InetAddressMessageSerializer;
import org.apache.ignite.internal.codegen.InetSocketAddressMessageSerializer;
import org.apache.ignite.internal.codegen.TcpDiscoveryAuthFailedMessageSerializer;
import org.apache.ignite.internal.codegen.TcpDiscoveryCacheMetricsMessageSerializer;
import org.apache.ignite.internal.codegen.TcpDiscoveryCheckFailedMessageSerializer;
import org.apache.ignite.internal.codegen.TcpDiscoveryClientAckResponseSerializer;
import org.apache.ignite.internal.codegen.TcpDiscoveryClientMetricsUpdateMessageSerializer;
import org.apache.ignite.internal.codegen.TcpDiscoveryClientNodesMetricsMessageSerializer;
import org.apache.ignite.internal.codegen.TcpDiscoveryClientPingRequestSerializer;
import org.apache.ignite.internal.codegen.TcpDiscoveryClientPingResponseSerializer;
import org.apache.ignite.internal.codegen.TcpDiscoveryConnectionCheckMessageSerializer;
import org.apache.ignite.internal.codegen.TcpDiscoveryCustomEventMessageSerializer;
import org.apache.ignite.internal.codegen.TcpDiscoveryDiscardMessageSerializer;
import org.apache.ignite.internal.codegen.TcpDiscoveryDuplicateIdMessageSerializer;
import org.apache.ignite.internal.codegen.TcpDiscoveryHandshakeRequestSerializer;
import org.apache.ignite.internal.codegen.TcpDiscoveryHandshakeResponseSerializer;
import org.apache.ignite.internal.codegen.TcpDiscoveryLoopbackProblemMessageSerializer;
import org.apache.ignite.internal.codegen.TcpDiscoveryMetricsUpdateMessageSerializer;
import org.apache.ignite.internal.codegen.TcpDiscoveryNodeFullMetricsMessageSerializer;
import org.apache.ignite.internal.codegen.TcpDiscoveryNodeLeftMessageSerializer;
import org.apache.ignite.internal.codegen.TcpDiscoveryNodeMetricsMessageSerializer;
import org.apache.ignite.internal.codegen.TcpDiscoveryPingRequestSerializer;
import org.apache.ignite.internal.codegen.TcpDiscoveryPingResponseSerializer;
import org.apache.ignite.internal.codegen.TcpDiscoveryRingLatencyCheckMessageSerializer;
import org.apache.ignite.internal.codegen.TcpDiscoveryServerOnlyCustomEventMessageSerializer;
import org.apache.ignite.internal.processors.cache.CacheStatisticsModeChangeMessage;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.spi.discovery.tcp.messages.InetAddressMessage;
import org.apache.ignite.spi.discovery.tcp.messages.InetSocketAddressMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAuthFailedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCacheMetricsMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCheckFailedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientAckResponse;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientMetricsUpdateMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientNodesMetricsMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientPingRequest;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientPingResponse;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryConnectionCheckMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryDiscardMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryDuplicateIdMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryHandshakeRequest;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryHandshakeResponse;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryLoopbackProblemMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryMetricsUpdateMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeFullMetricsMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeLeftMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeMetricsMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryPingRequest;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryPingResponse;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryRingLatencyCheckMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryServerOnlyCustomEventMessage;

/** Message factory for discovery messages. */
public class DiscoveryMessageFactory implements MessageFactoryProvider {
    /** {@inheritDoc} */
    @Override public void registerAll(MessageFactory factory) {
        factory.register((short)-105, TcpDiscoveryNodeFullMetricsMessage::new,
            new TcpDiscoveryNodeFullMetricsMessageSerializer());
        factory.register((short)-104, TcpDiscoveryClientNodesMetricsMessage::new, new TcpDiscoveryClientNodesMetricsMessageSerializer());
        factory.register((short)-103, TcpDiscoveryCacheMetricsMessage::new, new TcpDiscoveryCacheMetricsMessageSerializer());
        factory.register((short)-102, TcpDiscoveryNodeMetricsMessage::new, new TcpDiscoveryNodeMetricsMessageSerializer());
        factory.register((short)-101, InetSocketAddressMessage::new, new InetSocketAddressMessageSerializer());
        factory.register((short)-100, InetAddressMessage::new, new InetAddressMessageSerializer());

        // TcpDiscoveryAbstractMessage
        factory.register((short)0, TcpDiscoveryCheckFailedMessage::new, new TcpDiscoveryCheckFailedMessageSerializer());
        factory.register((short)1, TcpDiscoveryPingRequest::new, new TcpDiscoveryPingRequestSerializer());
        factory.register((short)2, TcpDiscoveryPingResponse::new, new TcpDiscoveryPingResponseSerializer());
        factory.register((short)3, TcpDiscoveryClientPingRequest::new, new TcpDiscoveryClientPingRequestSerializer());
        factory.register((short)4, TcpDiscoveryClientPingResponse::new, new TcpDiscoveryClientPingResponseSerializer());
        factory.register((short)5, TcpDiscoveryLoopbackProblemMessage::new, new TcpDiscoveryLoopbackProblemMessageSerializer());
        factory.register((short)6, TcpDiscoveryConnectionCheckMessage::new, new TcpDiscoveryConnectionCheckMessageSerializer());
        factory.register((short)7, TcpDiscoveryRingLatencyCheckMessage::new, new TcpDiscoveryRingLatencyCheckMessageSerializer());
        factory.register((short)8, TcpDiscoveryHandshakeRequest::new, new TcpDiscoveryHandshakeRequestSerializer());
        factory.register((short)9, TcpDiscoveryDiscardMessage::new, new TcpDiscoveryDiscardMessageSerializer());
        factory.register((short)10, TcpDiscoveryHandshakeResponse::new, new TcpDiscoveryHandshakeResponseSerializer());
        factory.register((short)11, TcpDiscoveryAuthFailedMessage::new, new TcpDiscoveryAuthFailedMessageSerializer());
        factory.register((short)12, TcpDiscoveryDuplicateIdMessage::new, new TcpDiscoveryDuplicateIdMessageSerializer());
        factory.register((short)13, TcpDiscoveryClientMetricsUpdateMessage::new, new TcpDiscoveryClientMetricsUpdateMessageSerializer());
        factory.register((short)14, TcpDiscoveryMetricsUpdateMessage::new, new TcpDiscoveryMetricsUpdateMessageSerializer());
        factory.register((short)15, TcpDiscoveryClientAckResponse::new, new TcpDiscoveryClientAckResponseSerializer());
        factory.register((short)16, TcpDiscoveryNodeLeftMessage::new, new TcpDiscoveryNodeLeftMessageSerializer());
        factory.register((short)17, TcpDiscoveryCustomEventMessage::new, new TcpDiscoveryCustomEventMessageSerializer());
        factory.register((short)18, TcpDiscoveryServerOnlyCustomEventMessage::new,
            new TcpDiscoveryServerOnlyCustomEventMessageSerializer());

        // DiscoveryCustomMessage
        factory.register((short)500, CacheStatisticsModeChangeMessage::new, new CacheStatisticsModeChangeMessageSerializer());
    }
}
