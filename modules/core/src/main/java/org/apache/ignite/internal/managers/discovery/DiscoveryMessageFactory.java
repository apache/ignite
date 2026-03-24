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
        factory.register((short)-200, TcpDiscoveryCollectionMessage::new,
            new TcpDiscoveryCollectionMessageMarshallableSerializer(marsh, clsLdr));

        factory.register((short)-115, SchemaAlterTableAddColumnOperation::new,
            new SchemaAlterTableAddColumnOperationSerializer());
        factory.register((short)-114, SchemaIndexCreateOperation::new,
            new SchemaIndexCreateOperationMarshallableSerializer(marsh, clsLdr));
        factory.register((short)-113, SchemaIndexDropOperation::new, new SchemaIndexDropOperationSerializer());
        factory.register((short)-112, SchemaAlterTableDropColumnOperation::new,
            new SchemaAlterTableDropColumnOperationSerializer());
        factory.register((short)-111, SchemaAddQueryEntityOperation::new,
            new SchemaAddQueryEntityOperationMarshallableSerializer(marsh, clsLdr));
        factory.register((short)-110, QueryField::new, new QueryFieldMarshallableSerializer(marsh, clsLdr));
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
        factory.register((short)-66, ErrorMessage::new, new ErrorMessageMarshallableSerializer(marsh, clsLdr));

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
        factory.register((short)16, TcpDiscoveryNodeLeftMessage::new, new TcpDiscoveryNodeLeftMessageMarshallableSerializer(marsh, clsLdr));
        factory.register((short)17, TcpDiscoveryNodeFailedMessage::new,
            new TcpDiscoveryNodeFailedMessageMarshallableSerializer(marsh, clsLdr));
        factory.register((short)18, TcpDiscoveryStatusCheckMessage::new, new TcpDiscoveryStatusCheckMessageSerializer());
        factory.register((short)19, TcpDiscoveryNodeAddFinishedMessage::new,
            new TcpDiscoveryNodeAddFinishedMessageMarshallableSerializer(marsh, clsLdr));
        factory.register((short)20, TcpDiscoveryJoinRequestMessage::new,
            new TcpDiscoveryJoinRequestMessageMarshallableSerializer(marsh, clsLdr));
        factory.register((short)21, TcpDiscoveryCustomEventMessage::new,
            new TcpDiscoveryCustomEventMessageMarshallableSerializer(marsh, clsLdr));
        factory.register((short)22, TcpDiscoveryServerOnlyCustomEventMessage::new,
            new TcpDiscoveryServerOnlyCustomEventMessageMarshallableSerializer(marsh, clsLdr));
        factory.register((short)23, TcpConnectionRequestDiscoveryMessage::new, new TcpConnectionRequestDiscoveryMessageSerializer());
        factory.register((short)24, DistributedMetaStorageUpdateMessage::new, new DistributedMetaStorageUpdateMessageSerializer());
        factory.register((short)25, DistributedMetaStorageUpdateAckMessage::new, new DistributedMetaStorageUpdateAckMessageSerializer());
        factory.register((short)26, DistributedMetaStorageCasMessage::new, new DistributedMetaStorageCasMessageSerializer());
        factory.register((short)27, DistributedMetaStorageCasAckMessage::new, new DistributedMetaStorageCasAckMessageSerializer());
        factory.register((short)28, TcpDiscoveryClientReconnectMessage::new, new TcpDiscoveryClientReconnectMessageSerializer());
        factory.register((short)29, TcpDiscoveryNodeAddedMessage::new,
            new TcpDiscoveryNodeAddedMessageMarshallableSerializer(marsh, clsLdr));
        factory.register((short)30, FullMessage::new, new FullMessageSerializer());
        factory.register((short)31, InitMessage::new, new InitMessageSerializer());
        factory.register((short)32, SnapshotStartDiscoveryMessage::new, new SnapshotStartDiscoveryMessageSerializer());
        factory.register((short)33, SnapshotCheckProcessRequest::new, new SnapshotCheckProcessRequestSerializer());
        factory.register((short)34, SnapshotOperationRequest::new, new SnapshotOperationRequestSerializer());
        factory.register((short)35, MasterKeyChangeRequest::new, new MasterKeyChangeRequestSerializer());
        factory.register((short)36, SnapshotOperationEndRequest::new, new SnapshotOperationEndRequestSerializer());
        factory.register((short)37, SnapshotRestoreStartRequest::new, new SnapshotRestoreStartRequestSerializer());
        factory.register((short)38, ChangeCacheEncryptionRequest::new, new ChangeCacheEncryptionRequestSerializer());

        factory.register((short)86, GridCacheVersion::new, new GridCacheVersionSerializer());

        // DiscoveryCustomMessage
        factory.register((short)500, CacheStatisticsModeChangeMessage::new, new CacheStatisticsModeChangeMessageSerializer());
        factory.register((short)501, SecurityAwareCustomMessageWrapper::new,
            new SecurityAwareCustomMessageWrapperMarshallableSerializer(marsh, clsLdr));
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
        factory.register((short)517, MappingAcceptedMessage::new, new MappingAcceptedMessageSerializer());
        factory.register((short)518, MappingProposedMessage::new, new MappingProposedMessageSerializer());
        factory.register((short)519, MarshallerMappingItem::new, new MarshallerMappingItemSerializer());
        factory.register((short)520, SnapshotOperationResponse::new, new SnapshotOperationResponseSerializer());
        factory.register((short)521, SnapshotHandlerResult::new, new SnapshotHandlerResultSerializer());
        factory.register((short)522, DataStreamerUpdatesHandlerResult::new, new DataStreamerUpdatesHandlerResultSerializer());
        factory.register((short)523, SnapshotCheckResponse::new, new SnapshotCheckResponseSerializer());
        factory.register((short)524, IncrementalSnapshotVerifyResult::new,
            new IncrementalSnapshotVerifyResultMarshallableSerializer(marsh, clsLdr));
        factory.register((short)525, SnapshotRestoreOperationResponse::new,
            new SnapshotRestoreOperationResponseMarshallableSerializer(marsh, clsLdr));
        factory.register((short)526, SnapshotMetadataResponse::new, new SnapshotMetadataResponseMarshallableSerializer(marsh, clsLdr));
        factory.register((short)527, SnapshotCheckPartitionHashesResponse::new,
            new SnapshotCheckPartitionHashesResponseMarshallableSerializer(marsh, clsLdr));
        factory.register((short)528, SnapshotCheckHandlersResponse::new, new SnapshotCheckHandlersResponseSerializer());
        factory.register((short)529, SnapshotCheckHandlersNodeResponse::new, new SnapshotCheckHandlersNodeResponseSerializer());
        factory.register((short)530, SnapshotPartitionsVerifyHandlerResponse::new,
            new SnapshotPartitionsVerifyHandlerResponseMarshallableSerializer(marsh, clsLdr));
        factory.register((short)531, CacheStatisticsClearMessage::new, new CacheStatisticsClearMessageSerializer());
        factory.register((short)532, ChangeGlobalStateMessage::new,
            new ChangeGlobalStateMessageMarshallableSerializer(marsh, clsLdr));
        factory.register((short)533, ClientCacheChangeDummyDiscoveryMessage::new,
            new ClientCacheChangeDummyDiscoveryMessageMarshallableSerializer(marsh, clsLdr));
        factory.register((short)534, DynamicCacheChangeBatch::new,
            new DynamicCacheChangeBatchMarshallableSerializer(marsh, clsLdr));
    }
}
