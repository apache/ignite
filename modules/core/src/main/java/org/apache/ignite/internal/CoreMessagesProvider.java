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
        register(factory, ErrorMessage.class);
        register(factory, InetSocketAddressMessage.class);
        register(factory, InetAddressMessage.class);
        register(factory, TcpDiscoveryNode.class);
        register(factory, IgniteProductVersion.class);
        register(factory, DiscoveryDataPacket.class);
        register(factory, GridByteArrayList.class);
        register(factory, CacheVersionedValue.class);
        register(factory, GridCacheVersion.class);
        register(factory, GridCacheVersionEx.class);

        msgIdx = 5500;
        register(factory, TcpDiscoveryCollectionMessage.class);

        // [5700 - 5900]: TcpDiscoveryAbstractMessage.
        msgIdx = 5700;
        register(factory, TcpDiscoveryCheckFailedMessage.class);
        register(factory, TcpDiscoveryPingRequest.class);
        register(factory, TcpDiscoveryPingResponse.class);
        register(factory, TcpDiscoveryClientPingRequest.class);
        register(factory, TcpDiscoveryClientPingResponse.class);
        register(factory, TcpDiscoveryLoopbackProblemMessage.class);
        register(factory, TcpDiscoveryConnectionCheckMessage.class);
        register(factory, TcpDiscoveryRingLatencyCheckMessage.class);
        register(factory, TcpDiscoveryHandshakeRequest.class);
        register(factory, TcpDiscoveryDiscardMessage.class);
        register(factory, TcpDiscoveryHandshakeResponse.class);
        register(factory, TcpDiscoveryAuthFailedMessage.class);
        register(factory, TcpDiscoveryDuplicateIdMessage.class);
        register(factory, TcpDiscoveryClientMetricsUpdateMessage.class);
        register(factory, TcpDiscoveryMetricsUpdateMessage.class);
        register(factory, TcpDiscoveryClientAckResponse.class);
        register(factory, TcpDiscoveryNodeLeftMessage.class);
        register(factory, TcpDiscoveryNodeFailedMessage.class);
        register(factory, TcpDiscoveryNodeAddFinishedMessage.class);
        register(factory, TcpDiscoveryJoinRequestMessage.class);
        register(factory, TcpDiscoveryCustomEventMessage.class);
        register(factory, TcpDiscoveryServerOnlyCustomEventMessage.class);
        register(factory, TcpDiscoveryNodeAddedMessage.class);
        register(factory, TcpDiscoveryClientReconnectMessage.class);

        msgIdx = 5900;
        register(factory, TcpDiscoveryStatusCheckMessage.class);

        // [6000 - 6200]: Snapshot operation messages. Most of them originally come from Discovery.
        msgIdx = 6000;
        register(factory, SnapshotStartDiscoveryMessage.class);
        register(factory, SnapshotCheckProcessRequest.class);
        register(factory, SnapshotOperationRequest.class);
        register(factory, SnapshotOperationEndRequest.class);
        register(factory, SnapshotRestoreStartRequest.class);
        register(factory, SnapshotOperationResponse.class);
        register(factory, SnapshotHandlerResult.class);
        register(factory, SnapshotCheckResponse.class);
        register(factory, SnapshotPartitionsVerifyHandlerResponse.class);
        register(factory, SnapshotRestoreOperationResponse.class);
        register(factory, SnapshotMetadataResponse.class);
        register(factory, SnapshotCheckPartitionHashesResponse.class);
        register(factory, SnapshotCheckHandlersResponse.class);
        register(factory, SnapshotFilesRequestMessage.class);
        register(factory, SnapshotFilesFailureMessage.class);
        register(factory, IncrementalSnapshotVerifyResult.class);
        register(factory, IncrementalSnapshotAwareMessage.class);

        // [6300 - 6400]: Services messages. Most of them originally come from Discovery.
        msgIdx = 6300;
        register(factory, ServiceDeploymentProcessId.class);
        register(factory, ServiceSingleNodeDeploymentResult.class);
        register(factory, ServiceClusterDeploymentResult.class);
        register(factory, ServiceDeploymentRequest.class);
        register(factory, ServiceUndeploymentRequest.class);
        register(factory, ServiceClusterDeploymentResultBatch.class);
        register(factory, ServiceChangeBatchRequest.class);
        register(factory, ServiceSingleNodeDeploymentResultBatch.class);

        // [6500 - 6700]: DiscoveryCustomMessage
        msgIdx = 6500;
        register(factory, TcpConnectionRequestDiscoveryMessage.class);
        register(factory, DistributedMetaStorageUpdateMessage.class);
        register(factory, DistributedMetaStorageUpdateAckMessage.class);
        register(factory, DistributedMetaStorageCasMessage.class);
        register(factory, DistributedMetaStorageCasAckMessage.class);
        register(factory, FullMessage.class);
        register(factory, InitMessage.class);
        register(factory, CacheStatisticsModeChangeMessage.class);
        register(factory, SecurityAwareCustomMessageWrapper.class);
        register(factory, MetadataRemoveAcceptedMessage.class);
        register(factory, MetadataRemoveProposedMessage.class);
        register(factory, WalStateFinishMessage.class);
        register(factory, WalStateProposeMessage.class);
        register(factory, MetadataUpdateAcceptedMessage.class);
        register(factory, TxTimeoutOnPartitionMapExchangeChangeMessage.class);
        register(factory, UserAcceptedMessage.class);
        register(factory, UserProposedMessage.class);
        register(factory, ChangeGlobalStateFinishMessage.class);
        register(factory, StopRoutineAckDiscoveryMessage.class);
        register(factory, StopRoutineDiscoveryMessage.class);
        register(factory, CacheAffinityChangeMessage.class);
        register(factory, ClientCacheChangeDiscoveryMessage.class);
        register(factory, MappingAcceptedMessage.class);
        register(factory, MappingProposedMessage.class);
        register(factory, ExchangeFailureMessage.class);
        register(factory, CacheStatisticsClearMessage.class);
        register(factory, ChangeGlobalStateMessage.class);
        register(factory, ClientCacheChangeDummyDiscoveryMessage.class);
        register(factory, DynamicCacheChangeBatch.class);

        // [10000 - 10200]: Transaction and lock related messages. Most of the originally comes from Communication.
        msgIdx = 10000;
        register(factory, TxInfo.class);
        register(factory, TxEntriesInfo.class);
        register(factory, TxLock.class);
        register(factory, TxLocksRequest.class);
        register(factory, TxLocksResponse.class);
        register(factory, IgniteTxKey.class);
        register(factory, IgniteTxEntry.class);
        register(factory, TxEntryValueHolder.class);
        register(factory, GridCacheTxRecoveryRequest.class);
        register(factory, GridCacheTxRecoveryResponse.class);
        register(factory, GridDistributedTxFinishRequest.class);
        register(factory, GridDistributedTxFinishResponse.class);
        register(factory, GridDistributedTxPrepareRequest.class);
        register(factory, GridDistributedTxPrepareResponse.class);
        register(factory, GridDhtTxFinishRequest.class);
        register(factory, GridDhtTxFinishResponse.class);
        register(factory, GridDhtTxPrepareRequest.class);
        register(factory, GridDhtTxPrepareResponse.class);
        register(factory, GridNearTxFinishRequest.class);
        register(factory, GridNearTxFinishResponse.class);
        register(factory, GridNearTxPrepareRequest.class);
        register(factory, GridNearTxPrepareResponse.class);
        register(factory, GridDhtLockRequest.class);
        register(factory, GridDhtLockResponse.class);
        register(factory, GridDhtUnlockRequest.class);
        register(factory, GridNearLockRequest.class);
        register(factory, GridNearLockResponse.class);
        register(factory, GridNearUnlockRequest.class);
        register(factory, GridDistributedLockRequest.class);
        register(factory, GridDistributedLockResponse.class);
        register(factory, GridDhtTxOnePhaseCommitAckRequest.class);
        register(factory, TransactionAttributesAwareRequest.class);

        // [10300 - 10500]: Cache, DHT messages.
        msgIdx = 10300;
        register(factory, GridDhtForceKeysRequest.class);
        register(factory, GridDhtForceKeysResponse.class);
        register(factory, GridDhtAtomicDeferredUpdateResponse.class);
        register(factory, GridDhtAtomicUpdateRequest.class);
        register(factory, GridDhtAtomicUpdateResponse.class);
        register(factory, GridNearAtomicFullUpdateRequest.class);
        register(factory, GridDhtAtomicSingleUpdateRequest.class);
        register(factory, GridNearAtomicUpdateResponse.class);
        register(factory, GridNearAtomicSingleUpdateRequest.class);
        register(factory, GridNearAtomicSingleUpdateInvokeRequest.class);
        register(factory, GridNearAtomicSingleUpdateFilterRequest.class);
        register(factory, GridNearAtomicCheckUpdateRequest.class);
        register(factory, NearCacheUpdates.class);
        register(factory, GridNearGetRequest.class);
        register(factory, GridNearGetResponse.class);
        register(factory, GridNearSingleGetRequest.class);
        register(factory, GridNearSingleGetResponse.class);
        register(factory, GridDhtAtomicNearResponse.class);
        register(factory, GridCacheTtlUpdateRequest.class);
        register(factory, GridCacheReturn.class);
        register(factory, GridCacheEntryInfo.class);
        register(factory, CacheInvokeDirectResult.class);
        register(factory, GridCacheRawVersionedEntry.class);
        register(factory, CacheEvictionEntry.class);
        register(factory, CacheEntryPredicateAdapter.class);
        register(factory, GridContinuousMessage.class);
        register(factory, ContinuousRoutineStartResultMessage.class);
        register(factory, UpdateErrors.class);
        register(factory, LatchAckMessage.class);
        register(factory, AtomicApplicationAttributesAwareRequest.class);
        register(factory, StartRequestData.class);
        register(factory, StartRoutineDiscoveryMessage.class);
        register(factory, StartRoutineAckDiscoveryMessage.class);
        register(factory, StartRoutineDiscoveryMessageV2.class);

        // [10600-10800]: Affinity & partition maps.
        msgIdx = 10600;
        register(factory, GridDhtAffinityAssignmentRequest.class);
        register(factory, GridDhtAffinityAssignmentResponse.class);
        register(factory, CacheGroupAffinityMessage.class);
        register(factory, ExchangeInfo.class);
        register(factory, PartitionUpdateCountersMessage.class);
        register(factory, CachePartitionPartialCountersMap.class);
        register(factory, IgniteDhtDemandedPartitionsMap.class);
        register(factory, CachePartitionFullCountersMap.class);
        register(factory, GroupPartitionIdPair.class);
        register(factory, IgniteDhtPartitionHistorySuppliersMap.class);
        register(factory, GridPartitionStateMap.class);
        register(factory, GridDhtPartitionMap.class);
        register(factory, GridDhtPartitionFullMap.class);
        register(factory, GridDhtPartitionExchangeId.class);
        register(factory, GridCheckpointRequest.class);
        register(factory, GridDhtPartitionDemandMessage.class);
        register(factory, GridDhtPartitionSupplyMessage.class);
        register(factory, GridDhtPartitionsFullMessage.class);
        register(factory, GridDhtPartitionsSingleMessage.class);
        register(factory, GridDhtPartitionsSingleRequest.class);

        // [10900-11100]: Query, schema and SQL related messages.
        msgIdx = 10900;
        register(factory, SchemaAlterTableAddColumnOperation.class);
        register(factory, SchemaIndexCreateOperation.class);
        register(factory, SchemaIndexDropOperation.class);
        register(factory, SchemaAlterTableDropColumnOperation.class);
        register(factory, SchemaAddQueryEntityOperation.class);
        register(factory, SchemaOperationStatusMessage.class);
        register(factory, SchemaProposeDiscoveryMessage.class);
        register(factory, SchemaFinishDiscoveryMessage.class);
        register(factory, QueryField.class);
        register(factory, GridCacheSqlQuery.class);
        register(factory, GridCacheQueryRequest.class);
        register(factory, GridCacheQueryResponse.class);
        register(factory, GridQueryCancelRequest.class);
        register(factory, GridQueryFailResponse.class);
        register(factory, GridQueryNextPageRequest.class);
        register(factory, GridQueryNextPageResponse.class);
        register(factory, GridQueryKillRequest.class);
        register(factory, GridQueryKillResponse.class);
        register(factory, IndexKeyDefinition.class);
        register(factory, IndexKeyTypeSettings.class);
        register(factory, IndexQueryResultMeta.class);
        register(factory, StatisticsKeyMessage.class);
        register(factory, StatisticsDecimalMessage.class);
        register(factory, StatisticsObjectData.class);
        register(factory, StatisticsColumnData.class);
        register(factory, StatisticsRequest.class);
        register(factory, StatisticsResponse.class);
        register(factory, CacheContinuousQueryBatchAck.class);
        register(factory, CacheContinuousQueryEntry.class);

        // [11200 - 11300]: Compute, distributed process messages.
        msgIdx = 11200;
        register(factory, GridJobCancelRequest.class);
        register(factory, GridJobExecuteRequest.class);
        register(factory, GridJobExecuteResponse.class);
        register(factory, GridJobSiblingsRequest.class);
        register(factory, GridJobSiblingsResponse.class);
        register(factory, GridTaskCancelRequest.class);
        register(factory, GridTaskSessionRequest.class);
        register(factory, GridTaskResultRequest.class);
        register(factory, GridTaskResultResponse.class);
        register(factory, JobStealingRequest.class);
        register(factory, SingleNodeMessage.class);

        // [11500 - 11600]:  IO, networking messages.
        msgIdx = NODE_ID_MSG_TYPE;
        register(factory, NodeIdMessage.class);
        register(factory, HandshakeMessage.class);
        register(factory, HandshakeWaitMessage.class);
        register(factory, GridIoMessage.class);
        factory.register(msgIdx++, IgniteIoTestMessage::new);
        register(factory, GridIoUserMessage.class);
        register(factory, GridIoSecurityAwareMessage.class);
        register(factory, RecoveryLastReceivedMessage.class);
        register(factory, TcpInverseConnectionResponseMessage.class);
        register(factory, SessionChannelMessage.class);

        // [11700 - 11800]: Datastreamer messages.
        msgIdx = 11700;
        register(factory, DataStreamerUpdatesHandlerResult.class);
        register(factory, DataStreamerEntry.class);
        register(factory, DataStreamerRequest.class);
        register(factory, DataStreamerResponse.class);

        // [11900 - 12000]: Metrics, monitoring messages.
        msgIdx = 11900;
        register(factory, CacheMetricsMessage.class);
        register(factory, NodeMetricsMessage.class);
        register(factory, NodeFullMetricsMessage.class);
        register(factory, ClusterMetricsUpdateMessage.class);
        register(factory, TcpDiscoveryClientNodesMetricsMessage.class);

        // [12000 - 12100]: Authentication, security messages.
        msgIdx = 12000;
        register(factory, User.class);
        register(factory, UserManagementOperation.class);
        register(factory, UserManagementOperationFinishedMessage.class);
        register(factory, UserAuthenticateRequestMessage.class);
        register(factory, UserAuthenticateResponseMessage.class);

        // [12200 - 12300]: Binary, classloading and marshalling messages.
        msgIdx = 12200;
        register(factory, GridDeploymentInfoBean.class);
        register(factory, GridDeploymentRequest.class);
        register(factory, GridDeploymentResponse.class);
        register(factory, MissingMappingRequestMessage.class);
        register(factory, MissingMappingResponseMessage.class);
        register(factory, MetadataRequestMessage.class);
        register(factory, MetadataResponseMessage.class);
        register(factory, MarshallerMappingItem.class);
        register(factory, BinaryMetadataVersionInfo.class);

        // [12400 - 12500]: Encryption messages.
        msgIdx = 12400;
        register(factory, GenerateEncryptionKeyRequest.class);
        register(factory, GenerateEncryptionKeyResponse.class);
        register(factory, ChangeCacheEncryptionRequest.class);
        register(factory, MasterKeyChangeRequest.class);

        // [13000 - 13300]: Control, diagnostincs and other messages.
        msgIdx = 13000;
        register(factory, GridEventStorageMessage.class);
        register(factory, GridChangeGlobalStateMessageResponse.class);
        register(factory, IgniteDiagnosticRequest.class);
        register(factory, IgniteDiagnosticResponse.class);
        register(factory, WalStateAckMessage.class);

        this.factory = null;
    }

    /** Registers message incrementing {@link #msgIdx}. */
    private <T extends Message> void register(MessageFactory factory, Class<T> cls) {
        Constructor<T> ctor;
        MessageSerializer<T> serializer;

        try {
            ctor = cls.getConstructor();

            boolean marshallable = MarshallableMessage.class.isAssignableFrom(cls);

            Class<?> serCls = Class.forName(cls.getName() + (marshallable ? "Marshallable" : "") + "Serializer");

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
