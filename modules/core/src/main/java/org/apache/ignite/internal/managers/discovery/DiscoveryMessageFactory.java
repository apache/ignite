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

import org.apache.ignite.internal.managers.communication.ErrorMessage;
import org.apache.ignite.internal.managers.communication.ErrorMessageSerializer;
import org.apache.ignite.internal.processors.authentication.User;
import org.apache.ignite.internal.processors.authentication.UserAcceptedMessage;
import org.apache.ignite.internal.processors.authentication.UserAcceptedMessageSerializer;
import org.apache.ignite.internal.processors.authentication.UserManagementOperation;
import org.apache.ignite.internal.processors.authentication.UserManagementOperationSerializer;
import org.apache.ignite.internal.processors.authentication.UserProposedMessage;
import org.apache.ignite.internal.processors.authentication.UserProposedMessageSerializer;
import org.apache.ignite.internal.processors.authentication.UserSerializer;
import org.apache.ignite.internal.processors.cache.CacheAffinityChangeMessage;
import org.apache.ignite.internal.processors.cache.CacheAffinityChangeMessageSerializer;
import org.apache.ignite.internal.processors.cache.CacheStatisticsModeChangeMessage;
import org.apache.ignite.internal.processors.cache.CacheStatisticsModeChangeMessageSerializer;
import org.apache.ignite.internal.processors.cache.ClientCacheChangeDiscoveryMessage;
import org.apache.ignite.internal.processors.cache.ClientCacheChangeDiscoveryMessageSerializer;
import org.apache.ignite.internal.processors.cache.TxTimeoutOnPartitionMapExchangeChangeMessage;
import org.apache.ignite.internal.processors.cache.TxTimeoutOnPartitionMapExchangeChangeMessageSerializer;
import org.apache.ignite.internal.processors.cache.WalStateFinishMessage;
import org.apache.ignite.internal.processors.cache.WalStateFinishMessageSerializer;
import org.apache.ignite.internal.processors.cache.WalStateProposeMessage;
import org.apache.ignite.internal.processors.cache.WalStateProposeMessageSerializer;
import org.apache.ignite.internal.processors.cache.binary.MetadataRemoveAcceptedMessage;
import org.apache.ignite.internal.processors.cache.binary.MetadataRemoveAcceptedMessageSerializer;
import org.apache.ignite.internal.processors.cache.binary.MetadataRemoveProposedMessage;
import org.apache.ignite.internal.processors.cache.binary.MetadataRemoveProposedMessageSerializer;
import org.apache.ignite.internal.processors.cache.binary.MetadataUpdateAcceptedMessage;
import org.apache.ignite.internal.processors.cache.binary.MetadataUpdateAcceptedMessageSerializer;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateFinishMessage;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateFinishMessageSerializer;
import org.apache.ignite.internal.processors.continuous.StopRoutineAckDiscoveryMessage;
import org.apache.ignite.internal.processors.continuous.StopRoutineAckDiscoveryMessageSerializer;
import org.apache.ignite.internal.processors.continuous.StopRoutineDiscoveryMessage;
import org.apache.ignite.internal.processors.continuous.StopRoutineDiscoveryMessageSerializer;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageCasAckMessage;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageCasAckMessageSerializer;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageCasMessage;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageCasMessageSerializer;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUpdateAckMessage;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUpdateAckMessageSerializer;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUpdateMessage;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUpdateMessageSerializer;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.QueryFieldSerializer;
import org.apache.ignite.internal.processors.query.schema.message.SchemaFinishDiscoveryMessage;
import org.apache.ignite.internal.processors.query.schema.message.SchemaFinishDiscoveryMessageSerializer;
import org.apache.ignite.internal.processors.query.schema.message.SchemaProposeDiscoveryMessage;
import org.apache.ignite.internal.processors.query.schema.message.SchemaProposeDiscoveryMessageSerializer;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaAddQueryEntityOperation;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaAddQueryEntityOperationSerializer;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaAlterTableAddColumnOperation;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaAlterTableAddColumnOperationSerializer;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaAlterTableDropColumnOperation;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaAlterTableDropColumnOperationSerializer;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaIndexCreateOperation;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaIndexCreateOperationSerializer;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaIndexDropOperation;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaIndexDropOperationSerializer;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.spi.communication.tcp.internal.TcpConnectionRequestDiscoveryMessage;
import org.apache.ignite.spi.communication.tcp.internal.TcpConnectionRequestDiscoveryMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.internal.DiscoveryDataPacket;
import org.apache.ignite.spi.discovery.tcp.internal.DiscoveryDataPacketSerializer;
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
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientReconnectMessageMarshallableSerializer;
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
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryJoinRequestMessageMarshallableSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryLoopbackProblemMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryLoopbackProblemMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryMetricsUpdateMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryMetricsUpdateMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessageMarshallableSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeFailedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeFailedMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeFullMetricsMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeFullMetricsMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeLeftMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeLeftMessageSerializer;
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

/** Message factory for discovery messages. */
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
        factory.register((short)-115, SchemaAlterTableAddColumnOperation::new, new SchemaAlterTableAddColumnOperationSerializer());
        factory.register((short)-114, SchemaIndexCreateOperation::new, new SchemaIndexCreateOperationSerializer());
        factory.register((short)-113, SchemaIndexDropOperation::new, new SchemaIndexDropOperationSerializer());
        factory.register((short)-112, SchemaAlterTableDropColumnOperation::new, new SchemaAlterTableDropColumnOperationSerializer());
        factory.register((short)-111, SchemaAddQueryEntityOperation::new, new SchemaAddQueryEntityOperationSerializer());
        factory.register((short)-110, QueryField::new, new QueryFieldSerializer());
        factory.register((short)-109, User::new, new UserSerializer());
        factory.register((short)-108, UserManagementOperation::new, new UserManagementOperationSerializer());
        factory.register((short)-107, NodeSpecificData::new, new NodeSpecificDataSerializer());
        factory.register((short)-106, DiscoveryDataPacket::new, new DiscoveryDataPacketSerializer());
        factory.register((short)-105, TcpDiscoveryNodeFullMetricsMessage::new,
            new TcpDiscoveryNodeFullMetricsMessageSerializer());
        factory.register((short)-104, TcpDiscoveryClientNodesMetricsMessage::new, new TcpDiscoveryClientNodesMetricsMessageSerializer());
        factory.register((short)-103, TcpDiscoveryCacheMetricsMessage::new, new TcpDiscoveryCacheMetricsMessageSerializer());
        factory.register((short)-102, TcpDiscoveryNodeMetricsMessage::new, new TcpDiscoveryNodeMetricsMessageSerializer());
        factory.register((short)-101, InetSocketAddressMessage::new, new InetSocketAddressMessageSerializer());
        factory.register((short)-100, InetAddressMessage::new, new InetAddressMessageSerializer());
        factory.register((short)-66, ErrorMessage::new, new ErrorMessageSerializer());

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
        factory.register((short)19, TcpDiscoveryNodeAddFinishedMessage::new,
            new TcpDiscoveryNodeAddFinishedMessageMarshallableSerializer(cstDataMarshall, cstDataMarshallClsLdr));
        factory.register((short)20, TcpDiscoveryJoinRequestMessage::new,
            new TcpDiscoveryJoinRequestMessageMarshallableSerializer(cstDataMarshall, cstDataMarshallClsLdr));
        factory.register((short)21, TcpDiscoveryCustomEventMessage::new, new TcpDiscoveryCustomEventMessageSerializer());
        factory.register((short)22, TcpDiscoveryServerOnlyCustomEventMessage::new,
            new TcpDiscoveryServerOnlyCustomEventMessageSerializer());
        factory.register((short)23, TcpConnectionRequestDiscoveryMessage::new, new TcpConnectionRequestDiscoveryMessageSerializer());
        factory.register((short)24, DistributedMetaStorageUpdateMessage::new, new DistributedMetaStorageUpdateMessageSerializer());
        factory.register((short)25, DistributedMetaStorageUpdateAckMessage::new, new DistributedMetaStorageUpdateAckMessageSerializer());
        factory.register((short)26, DistributedMetaStorageCasMessage::new, new DistributedMetaStorageCasMessageSerializer());
        factory.register((short)27, DistributedMetaStorageCasAckMessage::new, new DistributedMetaStorageCasAckMessageSerializer());
        factory.register((short)28, TcpDiscoveryClientReconnectMessage::new,
            new TcpDiscoveryClientReconnectMessageMarshallableSerializer(cstDataMarshall, cstDataMarshallClsLdr));

        // DiscoveryCustomMessage
        factory.register((short)500, CacheStatisticsModeChangeMessage::new, new CacheStatisticsModeChangeMessageSerializer());
        factory.register((short)501, SecurityAwareCustomMessageWrapper::new, new SecurityAwareCustomMessageWrapperSerializer());
        factory.register((short)502, MetadataRemoveAcceptedMessage::new, new MetadataRemoveAcceptedMessageSerializer());
        factory.register((short)503, MetadataRemoveProposedMessage::new, new MetadataRemoveProposedMessageSerializer());
        factory.register((short)504, SchemaProposeDiscoveryMessage::new, new SchemaProposeDiscoveryMessageSerializer());
        factory.register((short)505, SchemaFinishDiscoveryMessage::new, new SchemaFinishDiscoveryMessageSerializer());
        factory.register((short)506, WalStateFinishMessage::new, new WalStateFinishMessageSerializer());
        factory.register((short)507, WalStateProposeMessage::new, new WalStateProposeMessageSerializer());
        factory.register((short)508, MetadataUpdateAcceptedMessage::new,
            new MetadataUpdateAcceptedMessageSerializer());
        factory.register((short)509, TxTimeoutOnPartitionMapExchangeChangeMessage::new,
            new TxTimeoutOnPartitionMapExchangeChangeMessageSerializer());
        factory.register((short)510, UserAcceptedMessage::new, new UserAcceptedMessageSerializer());
        factory.register((short)511, UserProposedMessage::new, new UserProposedMessageSerializer());
        factory.register((short)512, ChangeGlobalStateFinishMessage::new, new ChangeGlobalStateFinishMessageSerializer());
        factory.register((short)513, StopRoutineAckDiscoveryMessage::new, new StopRoutineAckDiscoveryMessageSerializer());
        factory.register((short)514, StopRoutineDiscoveryMessage::new, new StopRoutineDiscoveryMessageSerializer());
        factory.register((short)515, CacheAffinityChangeMessage::new, new CacheAffinityChangeMessageSerializer());
        factory.register((short)516, ClientCacheChangeDiscoveryMessage::new, new ClientCacheChangeDiscoveryMessageSerializer());
    }
}
