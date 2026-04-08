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

package org.apache.ignite.internal;

import java.lang.reflect.Constructor;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.cache.query.index.IndexQueryResultMeta;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.managers.checkpoint.GridCheckpointRequest;
import org.apache.ignite.internal.managers.communication.CompressedMessage;
import org.apache.ignite.internal.managers.communication.ErrorMessage;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.communication.GridIoSecurityAwareMessage;
import org.apache.ignite.internal.managers.communication.GridIoUserMessage;
import org.apache.ignite.internal.managers.communication.IgniteIoTestMessage;
import org.apache.ignite.internal.managers.communication.SessionChannelMessage;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfoBean;
import org.apache.ignite.internal.managers.deployment.GridDeploymentRequest;
import org.apache.ignite.internal.managers.deployment.GridDeploymentResponse;
import org.apache.ignite.internal.managers.discovery.SecurityAwareCustomMessageWrapper;
import org.apache.ignite.internal.managers.encryption.ChangeCacheEncryptionRequest;
import org.apache.ignite.internal.managers.encryption.GenerateEncryptionKeyRequest;
import org.apache.ignite.internal.managers.encryption.GenerateEncryptionKeyResponse;
import org.apache.ignite.internal.managers.encryption.MasterKeyChangeRequest;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageMessage;
import org.apache.ignite.internal.processors.authentication.User;
import org.apache.ignite.internal.processors.authentication.UserAcceptedMessage;
import org.apache.ignite.internal.processors.authentication.UserAuthenticateRequestMessage;
import org.apache.ignite.internal.processors.authentication.UserAuthenticateResponseMessage;
import org.apache.ignite.internal.processors.authentication.UserManagementOperation;
import org.apache.ignite.internal.processors.authentication.UserManagementOperationFinishedMessage;
import org.apache.ignite.internal.processors.authentication.UserProposedMessage;
import org.apache.ignite.internal.processors.cache.CacheAffinityChangeMessage;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicateAdapter;
import org.apache.ignite.internal.processors.cache.CacheEvictionEntry;
import org.apache.ignite.internal.processors.cache.CacheInvokeDirectResult;
import org.apache.ignite.internal.processors.cache.CacheStatisticsClearMessage;
import org.apache.ignite.internal.processors.cache.CacheStatisticsModeChangeMessage;
import org.apache.ignite.internal.processors.cache.ClientCacheChangeDiscoveryMessage;
import org.apache.ignite.internal.processors.cache.ClientCacheChangeDummyDiscoveryMessage;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.processors.cache.ExchangeFailureMessage;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridChangeGlobalStateMessageResponse;
import org.apache.ignite.internal.processors.cache.TxTimeoutOnPartitionMapExchangeChangeMessage;
import org.apache.ignite.internal.processors.cache.WalStateAckMessage;
import org.apache.ignite.internal.processors.cache.WalStateFinishMessage;
import org.apache.ignite.internal.processors.cache.WalStateProposeMessage;
import org.apache.ignite.internal.processors.cache.binary.BinaryMetadataVersionInfo;
import org.apache.ignite.internal.processors.cache.binary.MetadataRemoveAcceptedMessage;
import org.apache.ignite.internal.processors.cache.binary.MetadataRemoveProposedMessage;
import org.apache.ignite.internal.processors.cache.binary.MetadataRequestMessage;
import org.apache.ignite.internal.processors.cache.binary.MetadataResponseMessage;
import org.apache.ignite.internal.processors.cache.binary.MetadataUpdateAcceptedMessage;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheTtlUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheTxRecoveryRequest;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheTxRecoveryResponse;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedLockResponse;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.GridNearUnlockRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtAffinityAssignmentRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtAffinityAssignmentResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLockResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxOnePhaseCommitAckRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtUnlockRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.PartitionUpdateCountersMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.TransactionAttributesAwareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.AtomicApplicationAttributesAwareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicDeferredUpdateResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicNearResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicSingleUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicUpdateResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicCheckUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicFullUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateFilterRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateInvokeRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicUpdateResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.NearCacheUpdates;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.UpdateErrors;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CacheGroupAffinityMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionFullCountersMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionPartialCountersMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtForceKeysRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtForceKeysResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GroupPartitionIdPair;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtDemandedPartitionsMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtPartitionHistorySuppliersMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.latch.LatchAckMessage;
import org.apache.ignite.internal.processors.cache.distributed.near.CacheVersionedValue;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.DataStreamerUpdatesHandlerResult;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IncrementalSnapshotAwareMessage;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IncrementalSnapshotVerifyResult;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotCheckHandlersResponse;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotCheckPartitionHashesResponse;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotCheckProcessRequest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotCheckResponse;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotFilesFailureMessage;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotFilesRequestMessage;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotHandlerResult;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotMetadataResponse;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotOperationEndRequest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotOperationRequest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotOperationResponse;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotPartitionsVerifyHandlerResponse;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotRestoreOperationResponse;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotRestoreStartRequest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotStartDiscoveryMessage;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryRequest;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryResponse;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryBatchAck;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.transactions.TxEntryValueHolder;
import org.apache.ignite.internal.processors.cache.transactions.TxLock;
import org.apache.ignite.internal.processors.cache.transactions.TxLocksRequest;
import org.apache.ignite.internal.processors.cache.transactions.TxLocksResponse;
import org.apache.ignite.internal.processors.cache.version.GridCacheRawVersionedEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionEx;
import org.apache.ignite.internal.processors.cluster.CacheMetricsMessage;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateFinishMessage;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateMessage;
import org.apache.ignite.internal.processors.cluster.ClusterMetricsUpdateMessage;
import org.apache.ignite.internal.processors.cluster.NodeFullMetricsMessage;
import org.apache.ignite.internal.processors.cluster.NodeMetricsMessage;
import org.apache.ignite.internal.processors.continuous.ContinuousRoutineStartResultMessage;
import org.apache.ignite.internal.processors.continuous.GridContinuousMessage;
import org.apache.ignite.internal.processors.continuous.StartRequestData;
import org.apache.ignite.internal.processors.continuous.StartRoutineAckDiscoveryMessage;
import org.apache.ignite.internal.processors.continuous.StartRoutineDiscoveryMessage;
import org.apache.ignite.internal.processors.continuous.StartRoutineDiscoveryMessageV2;
import org.apache.ignite.internal.processors.continuous.StopRoutineAckDiscoveryMessage;
import org.apache.ignite.internal.processors.continuous.StopRoutineDiscoveryMessage;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerEntry;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerRequest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerResponse;
import org.apache.ignite.internal.processors.marshaller.MappingAcceptedMessage;
import org.apache.ignite.internal.processors.marshaller.MappingProposedMessage;
import org.apache.ignite.internal.processors.marshaller.MarshallerMappingItem;
import org.apache.ignite.internal.processors.marshaller.MissingMappingRequestMessage;
import org.apache.ignite.internal.processors.marshaller.MissingMappingResponseMessage;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageCasAckMessage;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageCasMessage;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUpdateAckMessage;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageUpdateMessage;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryCancelRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryFailResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryNextPageRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryNextPageResponse;
import org.apache.ignite.internal.processors.query.messages.GridQueryKillRequest;
import org.apache.ignite.internal.processors.query.messages.GridQueryKillResponse;
import org.apache.ignite.internal.processors.query.schema.message.SchemaFinishDiscoveryMessage;
import org.apache.ignite.internal.processors.query.schema.message.SchemaOperationStatusMessage;
import org.apache.ignite.internal.processors.query.schema.message.SchemaProposeDiscoveryMessage;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaAddQueryEntityOperation;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaAlterTableAddColumnOperation;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaAlterTableDropColumnOperation;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaIndexCreateOperation;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaIndexDropOperation;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsColumnData;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsDecimalMessage;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsKeyMessage;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsObjectData;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsRequest;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsResponse;
import org.apache.ignite.internal.processors.rest.handlers.task.GridTaskResultRequest;
import org.apache.ignite.internal.processors.rest.handlers.task.GridTaskResultResponse;
import org.apache.ignite.internal.processors.service.ServiceChangeBatchRequest;
import org.apache.ignite.internal.processors.service.ServiceClusterDeploymentResult;
import org.apache.ignite.internal.processors.service.ServiceClusterDeploymentResultBatch;
import org.apache.ignite.internal.processors.service.ServiceDeploymentProcessId;
import org.apache.ignite.internal.processors.service.ServiceDeploymentRequest;
import org.apache.ignite.internal.processors.service.ServiceSingleNodeDeploymentResult;
import org.apache.ignite.internal.processors.service.ServiceSingleNodeDeploymentResultBatch;
import org.apache.ignite.internal.processors.service.ServiceUndeploymentRequest;
import org.apache.ignite.internal.util.GridByteArrayList;
import org.apache.ignite.internal.util.GridPartitionStateMap;
import org.apache.ignite.internal.util.distributed.FullMessage;
import org.apache.ignite.internal.util.distributed.InitMessage;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.MarshallableMessage;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.spi.collision.jobstealing.JobStealingRequest;
import org.apache.ignite.spi.communication.tcp.internal.TcpConnectionRequestDiscoveryMessage;
import org.apache.ignite.spi.communication.tcp.internal.TcpInverseConnectionResponseMessage;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeMessage;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeWaitMessage;
import org.apache.ignite.spi.communication.tcp.messages.NodeIdMessage;
import org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessage;
import org.apache.ignite.spi.discovery.tcp.internal.DiscoveryDataPacket;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.spi.discovery.tcp.messages.InetAddressMessage;
import org.apache.ignite.spi.discovery.tcp.messages.InetSocketAddressMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAuthFailedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCheckFailedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientAckResponse;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientMetricsUpdateMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientNodesMetricsMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientPingRequest;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientPingResponse;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryClientReconnectMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCollectionMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryConnectionCheckMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryDiscardMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryDuplicateIdMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryHandshakeRequest;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryHandshakeResponse;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryJoinRequestMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryLoopbackProblemMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryMetricsUpdateMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeFailedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeLeftMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryPingRequest;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryPingResponse;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryRingLatencyCheckMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryServerOnlyCustomEventMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryStatusCheckMessage;
import org.jetbrains.annotations.Nullable;

/** */
public class CoreMessagesProvider implements MessageFactoryProvider {
    /** Node ID message type. */
    public static final short NODE_ID_MSG_TYPE = 11500;

    /** Handshake message type. */
    public static final short HANDSHAKE_MSG_TYPE = NODE_ID_MSG_TYPE + 1;

    /** Handshake wait message type. */
    public static final short HANDSHAKE_WAIT_MSG_TYPE = HANDSHAKE_MSG_TYPE + 1;

    /** Custom data marshaller. */
    private final Marshaller marsh;

    /** Class loader for the custom data marshalling. */
    private final ClassLoader clsLdr;

    /** */
    private short msgIdx;

    /** */
    private @Nullable MessageFactory factory;

    /**
     * @param marsh Custom data marshaller.
     * @param clsLdr Class loader for the custom data marshalling.
     */
    public CoreMessagesProvider(Marshaller marsh, ClassLoader clsLdr) {
        this.marsh = marsh;
        this.clsLdr = clsLdr;
    }

    /** {@inheritDoc} */
    @Override public void registerAll(MessageFactory factory) {
        assert this.factory == null;

        this.factory = factory;

        // [-44, 0..2, 42, 200..204, 210, 302] - Use in tests.
        // [300..307, 350..352] - CalciteMessageFactory.
        // [-4..-22, -30..-35, -54..-57] - SQL

        // [5000 - 5500]: Utility messages. Most of them originally come from Discovery.
        msgIdx = 5000;
        // We don't use the code‑generated serializer for CompressedMessage - serialization is highly customized.
        factory.register(msgIdx++, CompressedMessage::new);
        register(ErrorMessage.class);
        register(InetSocketAddressMessage.class);
        register(InetAddressMessage.class);
        register(TcpDiscoveryNode.class);
        register(IgniteProductVersion.class);
        register(DiscoveryDataPacket.class);
        register(GridByteArrayList.class);
        register(CacheVersionedValue.class);
        register(GridCacheVersion.class);
        register(GridCacheVersionEx.class);

        msgIdx = 5500;
        register(TcpDiscoveryCollectionMessage.class);

        // [5700 - 5900]: Discovery originated messages.
        msgIdx = 5700;
        register(TcpDiscoveryHandshakeRequest.class);
        register(TcpDiscoveryHandshakeResponse.class);
        register(TcpDiscoveryJoinRequestMessage.class);
        register(TcpDiscoveryNodeAddedMessage.class);
        register(TcpDiscoveryNodeAddFinishedMessage.class);
        register(TcpDiscoveryNodeLeftMessage.class);
        register(TcpDiscoveryNodeFailedMessage.class);
        register(TcpDiscoveryConnectionCheckMessage.class);
        register(TcpDiscoveryPingRequest.class);
        register(TcpDiscoveryPingResponse.class);
        register(TcpDiscoveryClientPingRequest.class);
        register(TcpDiscoveryClientPingResponse.class);
        register(TcpDiscoveryClientAckResponse.class);
        register(TcpDiscoveryClientReconnectMessage.class);
        register(TcpDiscoveryDiscardMessage.class);
        register(TcpDiscoveryCheckFailedMessage.class);
        register(TcpDiscoveryLoopbackProblemMessage.class);
        register(TcpDiscoveryRingLatencyCheckMessage.class);
        register(TcpDiscoveryDuplicateIdMessage.class);
        register(TcpDiscoveryCustomEventMessage.class);
        register(TcpDiscoveryServerOnlyCustomEventMessage.class);

        msgIdx = 5900;
        register(TcpDiscoveryStatusCheckMessage.class);

        // [6000 - 6200]: Snapshot operation messages. Most of them originally come from Discovery.
        msgIdx = 6000;
        register(SnapshotStartDiscoveryMessage.class);
        register(SnapshotCheckProcessRequest.class);
        register(SnapshotOperationRequest.class);
        register(SnapshotOperationEndRequest.class);
        register(SnapshotRestoreStartRequest.class);
        register(SnapshotOperationResponse.class);
        register(SnapshotHandlerResult.class);
        register(SnapshotCheckResponse.class);
        register(SnapshotPartitionsVerifyHandlerResponse.class);
        register(SnapshotRestoreOperationResponse.class);
        register(SnapshotMetadataResponse.class);
        register(SnapshotCheckPartitionHashesResponse.class);
        register(SnapshotCheckHandlersResponse.class);
        register(SnapshotFilesRequestMessage.class);
        register(SnapshotFilesFailureMessage.class);
        register(IncrementalSnapshotVerifyResult.class);
        register(IncrementalSnapshotAwareMessage.class);

        // [6300 - 6400]: Services messages. Most of them originally come from Discovery.
        msgIdx = 6300;
        register(ServiceDeploymentProcessId.class);
        register(ServiceSingleNodeDeploymentResult.class);
        register(ServiceClusterDeploymentResult.class);
        register(ServiceDeploymentRequest.class);
        register(ServiceUndeploymentRequest.class);
        register(ServiceClusterDeploymentResultBatch.class);
        register(ServiceChangeBatchRequest.class);
        register(ServiceSingleNodeDeploymentResultBatch.class);

        // [6500 - 6700]: DiscoveryCustomMessage
        msgIdx = 6500;
        register(TcpConnectionRequestDiscoveryMessage.class);
        register(DistributedMetaStorageUpdateMessage.class);
        register(DistributedMetaStorageUpdateAckMessage.class);
        register(DistributedMetaStorageCasMessage.class);
        register(DistributedMetaStorageCasAckMessage.class);
        register(FullMessage.class);
        register(InitMessage.class);
        register(CacheStatisticsModeChangeMessage.class);
        register(SecurityAwareCustomMessageWrapper.class);
        register(MetadataRemoveAcceptedMessage.class);
        register(MetadataRemoveProposedMessage.class);
        register(WalStateFinishMessage.class);
        register(WalStateProposeMessage.class);
        register(MetadataUpdateAcceptedMessage.class);
        register(TxTimeoutOnPartitionMapExchangeChangeMessage.class);
        register(UserAcceptedMessage.class);
        register(UserProposedMessage.class);
        register(ChangeGlobalStateFinishMessage.class);
        register(StopRoutineAckDiscoveryMessage.class);
        register(StopRoutineDiscoveryMessage.class);
        register(CacheAffinityChangeMessage.class);
        register(ClientCacheChangeDiscoveryMessage.class);
        register(MappingAcceptedMessage.class);
        register(MappingProposedMessage.class);
        register(ExchangeFailureMessage.class);
        register(CacheStatisticsClearMessage.class);
        register(ClientCacheChangeDummyDiscoveryMessage.class);
        register(DynamicCacheChangeBatch.class);

        // [10000 - 10200]: Transaction and lock related messages. Most of the originally comes from Communication.
        msgIdx = 10000;
        register(TxInfo.class);
        register(TxEntriesInfo.class);
        register(TxLock.class);
        register(TxLocksRequest.class);
        register(TxLocksResponse.class);
        register(IgniteTxKey.class);
        register(IgniteTxEntry.class);
        register(TxEntryValueHolder.class);
        register(GridCacheTxRecoveryRequest.class);
        register(GridCacheTxRecoveryResponse.class);
        register(GridDistributedTxFinishRequest.class);
        register(GridDistributedTxFinishResponse.class);
        register(GridDistributedTxPrepareRequest.class);
        register(GridDistributedTxPrepareResponse.class);
        register(GridDhtTxFinishRequest.class);
        register(GridDhtTxFinishResponse.class);
        register(GridDhtTxPrepareRequest.class);
        register(GridDhtTxPrepareResponse.class);
        register(GridNearTxFinishRequest.class);
        register(GridNearTxFinishResponse.class);
        register(GridNearTxPrepareRequest.class);
        register(GridNearTxPrepareResponse.class);
        register(GridDhtLockRequest.class);
        register(GridDhtLockResponse.class);
        register(GridDhtUnlockRequest.class);
        register(GridNearLockRequest.class);
        register(GridNearLockResponse.class);
        register(GridNearUnlockRequest.class);
        register(GridDistributedLockRequest.class);
        register(GridDistributedLockResponse.class);
        register(GridDhtTxOnePhaseCommitAckRequest.class);
        register(TransactionAttributesAwareRequest.class);

        // [10300 - 10500]: Cache, DHT messages.
        msgIdx = 10300;
        register(GridDhtForceKeysRequest.class);
        register(GridDhtForceKeysResponse.class);
        register(GridDhtAtomicDeferredUpdateResponse.class);
        register(GridDhtAtomicUpdateRequest.class);
        register(GridDhtAtomicUpdateResponse.class);
        register(GridNearAtomicFullUpdateRequest.class);
        register(GridDhtAtomicSingleUpdateRequest.class);
        register(GridNearAtomicUpdateResponse.class);
        register(GridNearAtomicSingleUpdateRequest.class);
        register(GridNearAtomicSingleUpdateInvokeRequest.class);
        register(GridNearAtomicSingleUpdateFilterRequest.class);
        register(GridNearAtomicCheckUpdateRequest.class);
        register(NearCacheUpdates.class);
        register(GridNearGetRequest.class);
        register(GridNearGetResponse.class);
        register(GridNearSingleGetRequest.class);
        register(GridNearSingleGetResponse.class);
        register(GridDhtAtomicNearResponse.class);
        register(GridCacheTtlUpdateRequest.class);
        register(GridCacheReturn.class);
        register(GridCacheEntryInfo.class);
        register(CacheInvokeDirectResult.class);
        register(GridCacheRawVersionedEntry.class);
        register(CacheEvictionEntry.class);
        register(CacheEntryPredicateAdapter.class);
        register(GridContinuousMessage.class);
        register(ContinuousRoutineStartResultMessage.class);
        register(UpdateErrors.class);
        register(LatchAckMessage.class);
        register(AtomicApplicationAttributesAwareRequest.class);
        register(StartRequestData.class);
        register(StartRoutineDiscoveryMessage.class);
        register(StartRoutineAckDiscoveryMessage.class);
        register(StartRoutineDiscoveryMessageV2.class);

        // [10600-10800]: Affinity & partition maps.
        msgIdx = 10600;
        register(GridDhtAffinityAssignmentRequest.class);
        register(GridDhtAffinityAssignmentResponse.class);
        register(CacheGroupAffinityMessage.class);
        register(ExchangeInfo.class);
        register(PartitionUpdateCountersMessage.class);
        register(CachePartitionPartialCountersMap.class);
        register(IgniteDhtDemandedPartitionsMap.class);
        register(CachePartitionFullCountersMap.class);
        register(GroupPartitionIdPair.class);
        register(IgniteDhtPartitionHistorySuppliersMap.class);
        register(GridPartitionStateMap.class);
        register(GridDhtPartitionMap.class);
        register(GridDhtPartitionFullMap.class);
        register(GridDhtPartitionExchangeId.class);
        register(GridCheckpointRequest.class);
        register(GridDhtPartitionDemandMessage.class);
        register(GridDhtPartitionSupplyMessage.class);
        register(GridDhtPartitionsFullMessage.class);
        register(GridDhtPartitionsSingleMessage.class);
        register(GridDhtPartitionsSingleRequest.class);

        // [10900-11100]: Query, schema and SQL related messages.
        msgIdx = 10900;
        register(SchemaAlterTableAddColumnOperation.class);
        register(SchemaIndexCreateOperation.class);
        register(SchemaIndexDropOperation.class);
        register(SchemaAlterTableDropColumnOperation.class);
        register(SchemaAddQueryEntityOperation.class);
        register(SchemaOperationStatusMessage.class);
        register(SchemaProposeDiscoveryMessage.class);
        register(SchemaFinishDiscoveryMessage.class);
        register(QueryField.class);
        register(GridCacheSqlQuery.class);
        register(GridCacheQueryRequest.class);
        register(GridCacheQueryResponse.class);
        register(GridQueryCancelRequest.class);
        register(GridQueryFailResponse.class);
        register(GridQueryNextPageRequest.class);
        register(GridQueryNextPageResponse.class);
        register(GridQueryKillRequest.class);
        register(GridQueryKillResponse.class);
        register(IndexKeyDefinition.class);
        register(IndexKeyTypeSettings.class);
        register(IndexQueryResultMeta.class);
        register(StatisticsKeyMessage.class);
        register(StatisticsDecimalMessage.class);
        register(StatisticsObjectData.class);
        register(StatisticsColumnData.class);
        register(StatisticsRequest.class);
        register(StatisticsResponse.class);
        register(CacheContinuousQueryBatchAck.class);
        register(CacheContinuousQueryEntry.class);

        // [11200 - 11300]: Compute, distributed process messages.
        msgIdx = 11200;
        register(GridJobCancelRequest.class);
        register(GridJobExecuteRequest.class);
        register(GridJobExecuteResponse.class);
        register(GridJobSiblingsRequest.class);
        register(GridJobSiblingsResponse.class);
        register(GridTaskCancelRequest.class);
        register(GridTaskSessionRequest.class);
        register(GridTaskResultRequest.class);
        register(GridTaskResultResponse.class);
        register(JobStealingRequest.class);
        register(SingleNodeMessage.class);

        // [11500 - 11600]:  IO, networking messages.
        msgIdx = NODE_ID_MSG_TYPE;
        register(NodeIdMessage.class);
        register(HandshakeMessage.class);
        register(HandshakeWaitMessage.class);
        register(GridIoMessage.class);
        factory.register(msgIdx++, IgniteIoTestMessage::new);
        register(GridIoUserMessage.class);
        register(GridIoSecurityAwareMessage.class);
        register(RecoveryLastReceivedMessage.class);
        register(TcpInverseConnectionResponseMessage.class);
        register(SessionChannelMessage.class);

        // [11700 - 11800]: Datastreamer messages.
        msgIdx = 11700;
        register(DataStreamerUpdatesHandlerResult.class);
        register(DataStreamerEntry.class);
        register(DataStreamerRequest.class);
        register(DataStreamerResponse.class);

        // [11900 - 12000]: Metrics, monitoring messages.
        msgIdx = 11900;
        register(CacheMetricsMessage.class);
        register(NodeMetricsMessage.class);
        register(NodeFullMetricsMessage.class);
        register(ClusterMetricsUpdateMessage.class);
        register(TcpDiscoveryClientNodesMetricsMessage.class);
        register(TcpDiscoveryMetricsUpdateMessage.class);
        register(TcpDiscoveryClientMetricsUpdateMessage.class);

        // [12000 - 12100]: Authentication, security messages.
        msgIdx = 12000;
        register(User.class);
        register(UserManagementOperation.class);
        register(UserManagementOperationFinishedMessage.class);
        register(UserAuthenticateRequestMessage.class);
        register(UserAuthenticateResponseMessage.class);
        register(TcpDiscoveryAuthFailedMessage.class);

        // [12200 - 12300]: Binary, classloading and marshalling messages.
        msgIdx = 12200;
        register(GridDeploymentInfoBean.class);
        register(GridDeploymentRequest.class);
        register(GridDeploymentResponse.class);
        register(MissingMappingRequestMessage.class);
        register(MissingMappingResponseMessage.class);
        register(MetadataRequestMessage.class);
        register(MetadataResponseMessage.class);
        register(MarshallerMappingItem.class);
        register(BinaryMetadataVersionInfo.class);

        // [12400 - 12500]: Encryption messages.
        msgIdx = 12400;
        register(GenerateEncryptionKeyRequest.class);
        register(GenerateEncryptionKeyResponse.class);
        register(ChangeCacheEncryptionRequest.class);
        register(MasterKeyChangeRequest.class);

        // [13000 - 13300]: Control, diagnostincs and other messages.
        msgIdx = 13000;
        register(GridEventStorageMessage.class);
        register(ChangeGlobalStateMessage.class);
        register(GridChangeGlobalStateMessageResponse.class);
        register(IgniteDiagnosticRequest.class);
        register(IgniteDiagnosticResponse.class);
        register(WalStateAckMessage.class);
    }

    /** Registers message incrementing {@link #msgIdx}. */
    private <T extends Message> void register(Class<T> cls) {
        Constructor<T> ctor;
        MessageSerializer<T> serializer;

        try {
            ctor = cls.getConstructor();

            boolean marshallable = MarshallableMessage.class.isAssignableFrom(cls);

            Class<?> serCls = Class.forName(cls.getName() + (marshallable ? "MarshallableSerializer" : "Serializer"));

            serializer = marshallable
                ? (MessageSerializer<T>)serCls.getConstructor(Marshaller.class, ClassLoader.class).newInstance(marsh, clsLdr)
                : (MessageSerializer<T>)serCls.getConstructor().newInstance();
        }
        catch (Exception e) {
            throw new IgniteException("Failted to register message of type " + cls.getSimpleName(), e);
        }

        factory.register(
            msgIdx++,
            () -> {
                try {
                    return ctor.newInstance();
                }
                catch (Exception e) {
                    throw new IgniteException("Failted to create message of type " + cls.getSimpleName(), e);
                }
            },
            serializer
        );
    }
}
