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

import java.util.function.Supplier;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.CacheStatisticsModeChangeMessage;
import org.apache.ignite.internal.processors.cache.CacheStatisticsModeChangeMessageSerializer;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.spi.discovery.tcp.internal.DiscoveryDataPacket;
import org.apache.ignite.spi.discovery.tcp.internal.DiscoveryDataPacketSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.ClusterNodeCollectionMessage;
import org.apache.ignite.spi.discovery.tcp.messages.ClusterNodeCollectionMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.ClusterNodeMessage;
import org.apache.ignite.spi.discovery.tcp.messages.ClusterNodeMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.IgniteProductVersionMessage;
import org.apache.ignite.spi.discovery.tcp.messages.IgniteProductVersionMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.InetAddressMessage;
import org.apache.ignite.spi.discovery.tcp.messages.InetAddressMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.InetSocketAddressMessage;
import org.apache.ignite.spi.discovery.tcp.messages.InetSocketAddressMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.NodeSpecificData;
import org.apache.ignite.spi.discovery.tcp.messages.NodeSpecificDataSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAuthFailedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAuthFailedMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCacheMetricsMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCacheMetricsMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCheckFailedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCheckFailedMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientAckResponse;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientAckResponseSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientMetricsUpdateMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientMetricsUpdateMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientNodesMetricsMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientNodesMetricsMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientPingRequest;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientPingRequestSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientPingResponse;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientPingResponseSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientReconnectMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientReconnectMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCollectionMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCollectionMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryConnectionCheckMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryConnectionCheckMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryDiscardMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryDiscardMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryDuplicateIdMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryDuplicateIdMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryHandshakeRequest;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryHandshakeRequestSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryHandshakeResponse;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryHandshakeResponseSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryJoinRequestMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryJoinRequestMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryLoopbackProblemMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryLoopbackProblemMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryMarshallableMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryMetricsUpdateMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryMetricsUpdateMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddedMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeFailedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeFailedMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeFullMetricsMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeFullMetricsMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeLeftMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeLeftMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeMetricsMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeMetricsMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryPingRequest;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryPingRequestSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryPingResponse;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryPingResponseSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryRingLatencyCheckMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryRingLatencyCheckMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryServerOnlyCustomEventMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryServerOnlyCustomEventMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryStatusCheckMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryStatusCheckMessageSerializer;
import org.jetbrains.annotations.Nullable;

/**
 * Message factory for discovery messages. Allows to create an enhanced {@link MessageFactory} allowing to create
 * automated pre- and post- marshalling message serializer for {@link TcpDiscoveryMarshallableMessage}.
 */
public class DiscoveryMessageFactory implements MessageFactoryProvider {
    /** Custom data marshaller. */
    private final @Nullable Marshaller cstDataMarshall;

    /** Class loader for the custom data marshalling. */
    private final @Nullable ClassLoader cstDataMarshallClsLdr;

    /**
     * @param cstDataMarshall Custom data marshaller.
     * @param cstDataMarshallClsLdr Class loader for the custom data marshalling.
     */
    public DiscoveryMessageFactory(@Nullable Marshaller cstDataMarshall, @Nullable ClassLoader cstDataMarshallClsLdr) {
        assert cstDataMarshall == null && cstDataMarshallClsLdr == null || cstDataMarshall != null && cstDataMarshallClsLdr != null;

        this.cstDataMarshall = cstDataMarshall;
        this.cstDataMarshallClsLdr = cstDataMarshallClsLdr;
    }

    /** {@inheritDoc} */
    @Override public void registerAll(MessageFactory factory) {
        factory = enhanceMessageFactory(factory);

        // Utility messages.
        factory.register((short)-112, TcpDiscoveryNodeMessage::new, new TcpDiscoveryNodeMessageSerializer());
        factory.register((short)-111, ClusterNodeCollectionMessage::new, new ClusterNodeCollectionMessageSerializer());
        factory.register((short)-110, ClusterNodeMessage::new, new ClusterNodeMessageSerializer());
        factory.register((short)-109, IgniteProductVersionMessage::new, new IgniteProductVersionMessageSerializer());
        factory.register((short)-108, TcpDiscoveryCollectionMessage::new, new TcpDiscoveryCollectionMessageSerializer());
        factory.register((short)-107, NodeSpecificData::new, new NodeSpecificDataSerializer());
        factory.register((short)-106, DiscoveryDataPacket::new, new DiscoveryDataPacketSerializer());
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
        factory.register((short)17, TcpDiscoveryNodeFailedMessage::new, new TcpDiscoveryNodeFailedMessageSerializer());
        factory.register((short)18, TcpDiscoveryStatusCheckMessage::new, new TcpDiscoveryStatusCheckMessageSerializer());
        factory.register((short)19, TcpDiscoveryNodeAddFinishedMessage::new, new TcpDiscoveryNodeAddFinishedMessageSerializer());
        factory.register((short)20, TcpDiscoveryJoinRequestMessage::new, new TcpDiscoveryJoinRequestMessageSerializer());
        factory.register((short)21, TcpDiscoveryCustomEventMessage::new, new TcpDiscoveryCustomEventMessageSerializer());
        factory.register((short)22, TcpDiscoveryServerOnlyCustomEventMessage::new,
            new TcpDiscoveryServerOnlyCustomEventMessageSerializer());
        factory.register((short)23, TcpDiscoveryClientReconnectMessage::new, new TcpDiscoveryClientReconnectMessageSerializer());
        factory.register((short)24, TcpDiscoveryNodeAddedMessage::new, new TcpDiscoveryNodeAddedMessageSerializer());

        // DiscoveryCustomMessage
        factory.register((short)500, CacheStatisticsModeChangeMessage::new, new CacheStatisticsModeChangeMessageSerializer());
    }

    /**
     * @return Enhanced {@link MessageFactory} allowing to create automated pre- and post- marshalling message serializer
     * for {@link TcpDiscoveryMarshallableMessage}.
     */
    private MessageFactory enhanceMessageFactory(MessageFactory mf) {
        if (cstDataMarshall == null || cstDataMarshallClsLdr == null)
            return mf;

        return new MessageFactory() {
            @Override public void register(
                short directType,
                Supplier<Message> supplier,
                MessageSerializer serializer
            ) throws IgniteException {
                if (supplier.get() instanceof TcpDiscoveryMarshallableMessage) {
                    final MessageSerializer serializer0 = serializer;

                    serializer = new MessageSerializer() {
                        private Message curMarshallableMsg;

                        @Override public boolean writeTo(Message msg, MessageWriter writer) {
                            if (msg instanceof TcpDiscoveryMarshallableMessage && curMarshallableMsg == null) {
                                curMarshallableMsg = msg;

                                ((TcpDiscoveryMarshallableMessage)msg).prepareMarshal(cstDataMarshall);
                            }

                            boolean res = serializer0.writeTo(msg, writer);

                            if (res && curMarshallableMsg != null) {
                                assert msg instanceof TcpDiscoveryMarshallableMessage;

                                curMarshallableMsg = null;
                            }

                            return res;
                        }

                        @Override public boolean readFrom(Message msg, MessageReader reader) {
                            boolean res = serializer0.readFrom(msg, reader);

                            if (res && msg instanceof TcpDiscoveryMarshallableMessage)
                                ((TcpDiscoveryMarshallableMessage)msg).finishUnmarshal(cstDataMarshall, cstDataMarshallClsLdr);

                            return res;
                        }
                    };
                }

                mf.register(directType, supplier, serializer);
            }

            @Override public void register(short directType, Supplier<Message> supplier) throws IgniteException {
                mf.register(directType, supplier);
            }

            @Override public Message create(short type) {
                return mf.create(type);
            }

            @Override public MessageSerializer serializer(short type) {
                return mf.serializer(type);
            }
        };
    }
}
