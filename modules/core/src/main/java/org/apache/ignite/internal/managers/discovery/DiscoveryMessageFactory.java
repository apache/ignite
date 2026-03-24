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
import org.apache.ignite.internal.managers.communication.ErrorMessageMarshallableSerializer;
import org.apache.ignite.internal.managers.encryption.ChangeCacheEncryptionRequest;
import org.apache.ignite.internal.managers.encryption.ChangeCacheEncryptionRequestSerializer;
import org.apache.ignite.internal.managers.encryption.MasterKeyChangeRequest;
import org.apache.ignite.internal.managers.encryption.MasterKeyChangeRequestSerializer;
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
import org.apache.ignite.internal.processors.cache.CacheStatisticsClearMessage;
import org.apache.ignite.internal.processors.cache.CacheStatisticsClearMessageSerializer;
import org.apache.ignite.internal.processors.cache.CacheStatisticsModeChangeMessage;
import org.apache.ignite.internal.processors.cache.CacheStatisticsModeChangeMessageSerializer;
import org.apache.ignite.internal.processors.cache.ClientCacheChangeDiscoveryMessage;
import org.apache.ignite.internal.processors.cache.ClientCacheChangeDiscoveryMessageSerializer;
import org.apache.ignite.internal.processors.cache.ClientCacheChangeDummyDiscoveryMessage;
import org.apache.ignite.internal.processors.cache.ClientCacheChangeDummyDiscoveryMessageMarshallableSerializer;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatchMarshallableSerializer;
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
import org.apache.ignite.internal.processors.cache.persistence.snapshot.DataStreamerUpdatesHandlerResult;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.DataStreamerUpdatesHandlerResultSerializer;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IncrementalSnapshotVerifyResult;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IncrementalSnapshotVerifyResultMarshallableSerializer;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotCheckHandlersNodeResponse;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotCheckHandlersNodeResponseSerializer;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotCheckHandlersResponse;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotCheckHandlersResponseSerializer;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotCheckPartitionHashesResponse;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotCheckPartitionHashesResponseMarshallableSerializer;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotCheckProcessRequest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotCheckProcessRequestSerializer;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotCheckResponse;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotCheckResponseSerializer;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotHandlerResult;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotHandlerResultSerializer;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotMetadataResponse;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotMetadataResponseMarshallableSerializer;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotOperationEndRequest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotOperationEndRequestSerializer;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotOperationRequest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotOperationRequestSerializer;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotOperationResponse;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotOperationResponseSerializer;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotPartitionsVerifyHandlerResponse;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotPartitionsVerifyHandlerResponseMarshallableSerializer;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotRestoreOperationResponse;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotRestoreOperationResponseMarshallableSerializer;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotRestoreStartRequest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotRestoreStartRequestSerializer;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotStartDiscoveryMessage;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotStartDiscoveryMessageSerializer;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionSerializer;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateFinishMessage;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateFinishMessageSerializer;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateMessage;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateMessageMarshallableSerializer;
import org.apache.ignite.internal.processors.continuous.StopRoutineAckDiscoveryMessage;
import org.apache.ignite.internal.processors.continuous.StopRoutineAckDiscoveryMessageSerializer;
import org.apache.ignite.internal.processors.continuous.StopRoutineDiscoveryMessage;
import org.apache.ignite.internal.processors.continuous.StopRoutineDiscoveryMessageSerializer;
import org.apache.ignite.internal.processors.marshaller.MappingAcceptedMessage;
import org.apache.ignite.internal.processors.marshaller.MappingAcceptedMessageSerializer;
import org.apache.ignite.internal.processors.marshaller.MappingProposedMessage;
import org.apache.ignite.internal.processors.marshaller.MappingProposedMessageSerializer;
import org.apache.ignite.internal.processors.marshaller.MarshallerMappingItem;
import org.apache.ignite.internal.processors.marshaller.MarshallerMappingItemSerializer;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageCasAckMessage;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageCasAckMessageSerializer;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageCasMessage;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageCasMessageSerializer;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUpdateAckMessage;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUpdateAckMessageSerializer;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUpdateMessage;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUpdateMessageSerializer;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.QueryFieldMarshallableSerializer;
import org.apache.ignite.internal.processors.query.schema.message.SchemaFinishDiscoveryMessage;
import org.apache.ignite.internal.processors.query.schema.message.SchemaFinishDiscoveryMessageSerializer;
import org.apache.ignite.internal.processors.query.schema.message.SchemaProposeDiscoveryMessage;
import org.apache.ignite.internal.processors.query.schema.message.SchemaProposeDiscoveryMessageSerializer;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaAddQueryEntityOperation;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaAddQueryEntityOperationMarshallableSerializer;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaAlterTableAddColumnOperation;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaAlterTableAddColumnOperationSerializer;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaAlterTableDropColumnOperation;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaAlterTableDropColumnOperationSerializer;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaIndexCreateOperation;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaIndexCreateOperationMarshallableSerializer;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaIndexDropOperation;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaIndexDropOperationSerializer;
import org.apache.ignite.internal.util.distributed.FullMessage;
import org.apache.ignite.internal.util.distributed.FullMessageSerializer;
import org.apache.ignite.internal.util.distributed.InitMessage;
import org.apache.ignite.internal.util.distributed.InitMessageSerializer;
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
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientReconnectMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCollectionMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCollectionMessageMarshallableSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryConnectionCheckMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryConnectionCheckMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessageMarshallableSerializer;
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
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddedMessageMarshallableSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeFailedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeFailedMessageMarshallableSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeFullMetricsMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeFullMetricsMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeLeftMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeLeftMessageMarshallableSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeMetricsMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeMetricsMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryPingRequest;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryPingRequestSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryPingResponse;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryPingResponseSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryRingLatencyCheckMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryRingLatencyCheckMessageSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryServerOnlyCustomEventMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryServerOnlyCustomEventMessageMarshallableSerializer;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryStatusCheckMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryStatusCheckMessageSerializer;

/** Message factory for discovery messages. */
public class DiscoveryMessageFactory implements MessageFactoryProvider {
    /** Custom data marshaller. */
    private final Marshaller marsh;

    /** Class loader for the custom data marshalling. */
    private final ClassLoader clsLdr;

    /**
     * @param marsh Custom data marshaller.
     * @param clsLdr Class loader for the custom data marshalling.
     */
    public DiscoveryMessageFactory(Marshaller marsh, ClassLoader clsLdr) {
        this.marsh = marsh;
        this.clsLdr = clsLdr;
    }

    /** {@inheritDoc} */
    @Override public void registerAll(MessageFactory factory) {
        factory.register(TcpDiscoveryCollectionMessage::new,
            new TcpDiscoveryCollectionMessageMarshallableSerializer(marsh, clsLdr, -200));

        factory.register(SchemaAlterTableAddColumnOperation::new, new SchemaAlterTableAddColumnOperationSerializer(-115));
        factory.register(SchemaIndexCreateOperation::new, new SchemaIndexCreateOperationMarshallableSerializer(marsh, clsLdr, -114));
        factory.register(SchemaIndexDropOperation::new, new SchemaIndexDropOperationSerializer(-113));
        factory.register(SchemaAlterTableDropColumnOperation::new, new SchemaAlterTableDropColumnOperationSerializer(-112));
        factory.register(SchemaAddQueryEntityOperation::new, new SchemaAddQueryEntityOperationMarshallableSerializer(marsh, clsLdr, -111));
        factory.register(QueryField::new, new QueryFieldMarshallableSerializer(marsh, clsLdr, -110));
        factory.register(User::new, new UserSerializer(-109));
        factory.register(UserManagementOperation::new, new UserManagementOperationSerializer(-108));
        factory.register(NodeSpecificData::new, new NodeSpecificDataSerializer(-107));
        factory.register(DiscoveryDataPacket::new, new DiscoveryDataPacketSerializer(-106));
        factory.register(TcpDiscoveryNodeFullMetricsMessage::new, new TcpDiscoveryNodeFullMetricsMessageSerializer(-105));
        factory.register(TcpDiscoveryClientNodesMetricsMessage::new, new TcpDiscoveryClientNodesMetricsMessageSerializer(-104));
        factory.register(TcpDiscoveryCacheMetricsMessage::new, new TcpDiscoveryCacheMetricsMessageSerializer(-103));
        factory.register(TcpDiscoveryNodeMetricsMessage::new, new TcpDiscoveryNodeMetricsMessageSerializer(-102));
        factory.register(InetSocketAddressMessage::new, new InetSocketAddressMessageSerializer(-101));
        factory.register(InetAddressMessage::new, new InetAddressMessageSerializer(-100));
        factory.register(ErrorMessage::new, new ErrorMessageMarshallableSerializer(marsh, clsLdr, -66));

        // TcpDiscoveryAbstractMessage
        factory.register(TcpDiscoveryCheckFailedMessage::new, new TcpDiscoveryCheckFailedMessageSerializer(0));
        factory.register(TcpDiscoveryPingRequest::new, new TcpDiscoveryPingRequestSerializer(1));
        factory.register(TcpDiscoveryPingResponse::new, new TcpDiscoveryPingResponseSerializer(2));
        factory.register(TcpDiscoveryClientPingRequest::new, new TcpDiscoveryClientPingRequestSerializer(3));
        factory.register(TcpDiscoveryClientPingResponse::new, new TcpDiscoveryClientPingResponseSerializer(4));
        factory.register(TcpDiscoveryLoopbackProblemMessage::new, new TcpDiscoveryLoopbackProblemMessageSerializer(5));
        factory.register(TcpDiscoveryConnectionCheckMessage::new, new TcpDiscoveryConnectionCheckMessageSerializer(6));
        factory.register(TcpDiscoveryRingLatencyCheckMessage::new, new TcpDiscoveryRingLatencyCheckMessageSerializer(7));
        factory.register(TcpDiscoveryHandshakeRequest::new, new TcpDiscoveryHandshakeRequestSerializer(8));
        factory.register(TcpDiscoveryDiscardMessage::new, new TcpDiscoveryDiscardMessageSerializer(9));
        factory.register(TcpDiscoveryHandshakeResponse::new, new TcpDiscoveryHandshakeResponseSerializer(10));
        factory.register(TcpDiscoveryAuthFailedMessage::new, new TcpDiscoveryAuthFailedMessageSerializer(11));
        factory.register(TcpDiscoveryDuplicateIdMessage::new, new TcpDiscoveryDuplicateIdMessageSerializer(12));
        factory.register(TcpDiscoveryClientMetricsUpdateMessage::new, new TcpDiscoveryClientMetricsUpdateMessageSerializer(13));
        factory.register(TcpDiscoveryMetricsUpdateMessage::new, new TcpDiscoveryMetricsUpdateMessageSerializer(14));
        factory.register(TcpDiscoveryClientAckResponse::new, new TcpDiscoveryClientAckResponseSerializer(15));
        factory.register(TcpDiscoveryNodeLeftMessage::new, new TcpDiscoveryNodeLeftMessageMarshallableSerializer(marsh, clsLdr, 16));
        factory.register(TcpDiscoveryNodeFailedMessage::new, new TcpDiscoveryNodeFailedMessageMarshallableSerializer(marsh, clsLdr, 17));
        factory.register(TcpDiscoveryStatusCheckMessage::new, new TcpDiscoveryStatusCheckMessageSerializer(18));
        factory.register(TcpDiscoveryNodeAddFinishedMessage::new,
            new TcpDiscoveryNodeAddFinishedMessageMarshallableSerializer(marsh, clsLdr, 19));
        factory.register(TcpDiscoveryJoinRequestMessage::new,
            new TcpDiscoveryJoinRequestMessageMarshallableSerializer(marsh, clsLdr, 20));
        factory.register(TcpDiscoveryCustomEventMessage::new,
            new TcpDiscoveryCustomEventMessageMarshallableSerializer(marsh, clsLdr, 21));
        factory.register(TcpDiscoveryServerOnlyCustomEventMessage::new,
            new TcpDiscoveryServerOnlyCustomEventMessageMarshallableSerializer(marsh, clsLdr, 22));
        factory.register(TcpConnectionRequestDiscoveryMessage::new, new TcpConnectionRequestDiscoveryMessageSerializer(23));
        factory.register(DistributedMetaStorageUpdateMessage::new, new DistributedMetaStorageUpdateMessageSerializer(24));
        factory.register(DistributedMetaStorageUpdateAckMessage::new, new DistributedMetaStorageUpdateAckMessageSerializer(25));
        factory.register(DistributedMetaStorageCasMessage::new, new DistributedMetaStorageCasMessageSerializer(26));
        factory.register(DistributedMetaStorageCasAckMessage::new, new DistributedMetaStorageCasAckMessageSerializer(27));
        factory.register(TcpDiscoveryClientReconnectMessage::new, new TcpDiscoveryClientReconnectMessageSerializer(28));
        factory.register(TcpDiscoveryNodeAddedMessage::new, new TcpDiscoveryNodeAddedMessageMarshallableSerializer(marsh, clsLdr, 29));
        factory.register(FullMessage::new, new FullMessageSerializer(30));
        factory.register(InitMessage::new, new InitMessageSerializer(31));
        factory.register(SnapshotStartDiscoveryMessage::new, new SnapshotStartDiscoveryMessageSerializer(32));
        factory.register(SnapshotCheckProcessRequest::new, new SnapshotCheckProcessRequestSerializer(33));
        factory.register(SnapshotOperationRequest::new, new SnapshotOperationRequestSerializer(34));
        factory.register(MasterKeyChangeRequest::new, new MasterKeyChangeRequestSerializer(35));
        factory.register(SnapshotOperationEndRequest::new, new SnapshotOperationEndRequestSerializer(36));
        factory.register(SnapshotRestoreStartRequest::new, new SnapshotRestoreStartRequestSerializer(37));
        factory.register(ChangeCacheEncryptionRequest::new, new ChangeCacheEncryptionRequestSerializer(38));

        factory.register(GridCacheVersion::new, new GridCacheVersionSerializer(86));

        // DiscoveryCustomMessage
        factory.register(CacheStatisticsModeChangeMessage::new, new CacheStatisticsModeChangeMessageSerializer(500));
        factory.register(SecurityAwareCustomMessageWrapper::new,
            new SecurityAwareCustomMessageWrapperMarshallableSerializer(marsh, clsLdr, 501));
        factory.register(MetadataRemoveAcceptedMessage::new, new MetadataRemoveAcceptedMessageSerializer(502));
        factory.register(MetadataRemoveProposedMessage::new, new MetadataRemoveProposedMessageSerializer(503));
        factory.register(SchemaProposeDiscoveryMessage::new, new SchemaProposeDiscoveryMessageSerializer(504));
        factory.register(SchemaFinishDiscoveryMessage::new, new SchemaFinishDiscoveryMessageSerializer(505));
        factory.register(WalStateFinishMessage::new, new WalStateFinishMessageSerializer(506));
        factory.register(WalStateProposeMessage::new, new WalStateProposeMessageSerializer(507));
        factory.register(MetadataUpdateAcceptedMessage::new, new MetadataUpdateAcceptedMessageSerializer(508));
        factory.register(TxTimeoutOnPartitionMapExchangeChangeMessage::new,
            new TxTimeoutOnPartitionMapExchangeChangeMessageSerializer(509));
        factory.register(UserAcceptedMessage::new, new UserAcceptedMessageSerializer(510));
        factory.register(UserProposedMessage::new, new UserProposedMessageSerializer(511));
        factory.register(ChangeGlobalStateFinishMessage::new, new ChangeGlobalStateFinishMessageSerializer(512));
        factory.register(StopRoutineAckDiscoveryMessage::new, new StopRoutineAckDiscoveryMessageSerializer(513));
        factory.register(StopRoutineDiscoveryMessage::new, new StopRoutineDiscoveryMessageSerializer(514));
        factory.register(CacheAffinityChangeMessage::new, new CacheAffinityChangeMessageSerializer(515));
        factory.register(ClientCacheChangeDiscoveryMessage::new, new ClientCacheChangeDiscoveryMessageSerializer(516));
        factory.register(MappingAcceptedMessage::new, new MappingAcceptedMessageSerializer(517));
        factory.register(MappingProposedMessage::new, new MappingProposedMessageSerializer(518));
        factory.register(MarshallerMappingItem::new, new MarshallerMappingItemSerializer(519));
        factory.register(SnapshotOperationResponse::new, new SnapshotOperationResponseSerializer(520));
        factory.register(SnapshotHandlerResult::new, new SnapshotHandlerResultSerializer(521));
        factory.register(DataStreamerUpdatesHandlerResult::new, new DataStreamerUpdatesHandlerResultSerializer(522));
        factory.register(SnapshotCheckResponse::new, new SnapshotCheckResponseSerializer(523));
        factory.register(IncrementalSnapshotVerifyResult::new,
            new IncrementalSnapshotVerifyResultMarshallableSerializer(marsh, clsLdr, 524));
        factory.register(SnapshotRestoreOperationResponse::new,
            new SnapshotRestoreOperationResponseMarshallableSerializer(marsh, clsLdr, 525));
        factory.register(SnapshotMetadataResponse::new,
            new SnapshotMetadataResponseMarshallableSerializer(marsh, clsLdr, 526));
        factory.register(SnapshotCheckPartitionHashesResponse::new,
            new SnapshotCheckPartitionHashesResponseMarshallableSerializer(marsh, clsLdr, 527));
        factory.register(SnapshotCheckHandlersResponse::new, new SnapshotCheckHandlersResponseSerializer(528));
        factory.register(SnapshotCheckHandlersNodeResponse::new, new SnapshotCheckHandlersNodeResponseSerializer(529));
        factory.register(SnapshotPartitionsVerifyHandlerResponse::new,
            new SnapshotPartitionsVerifyHandlerResponseMarshallableSerializer(marsh, clsLdr, 530));
        factory.register(CacheStatisticsClearMessage::new, new CacheStatisticsClearMessageSerializer(531));
        factory.register(ChangeGlobalStateMessage::new,
            new ChangeGlobalStateMessageMarshallableSerializer(marsh, clsLdr, 532));
        factory.register(ClientCacheChangeDummyDiscoveryMessage::new,
            new ClientCacheChangeDummyDiscoveryMessageMarshallableSerializer(marsh, clsLdr, 533));
        factory.register(DynamicCacheChangeBatch::new,
            new DynamicCacheChangeBatchMarshallableSerializer(marsh, clsLdr, 534));
    }
}
