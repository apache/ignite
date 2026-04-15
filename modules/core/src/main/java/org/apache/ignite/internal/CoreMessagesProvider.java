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
import org.apache.ignite.internal.binary.BinaryMarshaller;
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
import org.apache.ignite.internal.processors.cache.binary.MetadataUpdateProposedMessage;
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
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
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

    /** */
    public static final short MAX_MESSAGE_ID = 15_000;

    /** Binary marshaller. */
    private final Marshaller schemaAwareMarhaller;

    /** Binary marshaller. */
    private final Marshaller schemaLessMarshaller;

    /** Resolved classloader. */
    private final ClassLoader resolvedClsLdr;

    /** */
    private short msgIdx;

    /** */
    private @Nullable MessageFactory factory;

    /**
     * @param schemaAwareMarhaller Schema-aware marshaller like {@link BinaryMarshaller}.
     * @param schemaLessMarshaller Pure, schemaless marshaller like {@link JdkMarshaller}.
     * @param resolvedClsLdr Resolved classloader.
     */
    public CoreMessagesProvider(Marshaller schemaAwareMarhaller, Marshaller schemaLessMarshaller, ClassLoader resolvedClsLdr) {
        this.schemaAwareMarhaller = schemaAwareMarhaller;
        this.schemaLessMarshaller = schemaLessMarshaller;
        this.resolvedClsLdr = resolvedClsLdr;
    }

    /** The order is important. If wish to remove a message, put 'msgIdx++' on its place. */
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
        withNoSchema(ErrorMessage.class);
        withNoSchema(InetSocketAddressMessage.class);
        withNoSchema(InetAddressMessage.class);
        withNoSchema(TcpDiscoveryNode.class);
        withNoSchema(IgniteProductVersion.class);
        withNoSchema(DiscoveryDataPacket.class);
        withNoSchema(GridByteArrayList.class);
        withNoSchema(CacheVersionedValue.class);
        withNoSchema(GridCacheVersion.class);
        withNoSchema(GridCacheVersionEx.class);

        msgIdx = 5500;
        withNoSchema(TcpDiscoveryCollectionMessage.class);

        // [5700 - 5900]: Discovery originated messages.
        msgIdx = 5700;
        withNoSchema(TcpDiscoveryHandshakeRequest.class);
        withNoSchema(TcpDiscoveryHandshakeResponse.class);
        withNoSchema(TcpDiscoveryJoinRequestMessage.class);
        withNoSchema(TcpDiscoveryNodeAddedMessage.class);
        withNoSchema(TcpDiscoveryNodeAddFinishedMessage.class);
        withNoSchema(TcpDiscoveryNodeLeftMessage.class);
        withNoSchema(TcpDiscoveryNodeFailedMessage.class);
        withNoSchema(TcpDiscoveryConnectionCheckMessage.class);
        withNoSchema(TcpDiscoveryPingRequest.class);
        withNoSchema(TcpDiscoveryPingResponse.class);
        withNoSchema(TcpDiscoveryClientPingRequest.class);
        withNoSchema(TcpDiscoveryClientPingResponse.class);
        withNoSchema(TcpDiscoveryClientAckResponse.class);
        withNoSchema(TcpDiscoveryClientReconnectMessage.class);
        withNoSchema(TcpDiscoveryDiscardMessage.class);
        withNoSchema(TcpDiscoveryCheckFailedMessage.class);
        withNoSchema(TcpDiscoveryLoopbackProblemMessage.class);
        withNoSchema(TcpDiscoveryRingLatencyCheckMessage.class);
        withNoSchema(TcpDiscoveryDuplicateIdMessage.class);
        withNoSchema(TcpDiscoveryCustomEventMessage.class);
        withNoSchema(TcpDiscoveryServerOnlyCustomEventMessage.class);

        msgIdx = 5900;
        withNoSchema(TcpDiscoveryStatusCheckMessage.class);

        // [6000 - 6200]: Snapshot operation messages. Most of them originally come from Discovery.
        msgIdx = 6000;
        withNoSchema(SnapshotStartDiscoveryMessage.class);
        withNoSchema(SnapshotCheckProcessRequest.class);
        withNoSchema(SnapshotOperationRequest.class);
        withNoSchema(SnapshotOperationEndRequest.class);
        withNoSchema(SnapshotRestoreStartRequest.class);
        withNoSchema(SnapshotOperationResponse.class);
        withNoSchema(SnapshotHandlerResult.class);
        withNoSchema(SnapshotCheckResponse.class);
        withNoSchema(SnapshotPartitionsVerifyHandlerResponse.class);
        withNoSchema(SnapshotRestoreOperationResponse.class);
        withNoSchema(SnapshotMetadataResponse.class);
        withNoSchema(SnapshotCheckPartitionHashesResponse.class);
        withNoSchema(SnapshotCheckHandlersResponse.class);
        withNoSchema(SnapshotFilesRequestMessage.class);
        withNoSchema(SnapshotFilesFailureMessage.class);
        withNoSchema(IncrementalSnapshotVerifyResult.class);
        withNoSchema(IncrementalSnapshotAwareMessage.class);

        // [6300 - 6400]: Services messages. Most of them originally come from Discovery.
        msgIdx = 6300;
        withNoSchema(ServiceDeploymentProcessId.class);
        withNoSchema(ServiceSingleNodeDeploymentResult.class);
        withNoSchema(ServiceClusterDeploymentResult.class);
        withNoSchema(ServiceDeploymentRequest.class);
        withNoSchema(ServiceUndeploymentRequest.class);
        withNoSchema(ServiceClusterDeploymentResultBatch.class);
        withNoSchema(ServiceChangeBatchRequest.class);
        withNoSchema(ServiceSingleNodeDeploymentResultBatch.class);

        // [6500 - 6700]: DiscoveryCustomMessage
        msgIdx = 6500;
        withNoSchema(TcpConnectionRequestDiscoveryMessage.class);
        withNoSchema(DistributedMetaStorageUpdateMessage.class);
        withNoSchema(DistributedMetaStorageUpdateAckMessage.class);
        withNoSchema(DistributedMetaStorageCasMessage.class);
        withNoSchema(DistributedMetaStorageCasAckMessage.class);
        withNoSchema(FullMessage.class);
        withNoSchema(InitMessage.class);
        withNoSchema(CacheStatisticsModeChangeMessage.class);
        withNoSchema(SecurityAwareCustomMessageWrapper.class);
        withNoSchema(MetadataRemoveAcceptedMessage.class);
        withNoSchema(MetadataRemoveProposedMessage.class);
        withNoSchema(WalStateFinishMessage.class);
        withNoSchema(WalStateProposeMessage.class);
        withNoSchema(MetadataUpdateAcceptedMessage.class);
        withNoSchema(MetadataUpdateProposedMessage.class);
        withNoSchema(TxTimeoutOnPartitionMapExchangeChangeMessage.class);
        withNoSchema(UserAcceptedMessage.class);
        withNoSchema(UserProposedMessage.class);
        withNoSchema(ChangeGlobalStateFinishMessage.class);
        withNoSchema(StopRoutineAckDiscoveryMessage.class);
        withNoSchema(StopRoutineDiscoveryMessage.class);
        withNoSchema(CacheAffinityChangeMessage.class);
        withNoSchema(ClientCacheChangeDiscoveryMessage.class);
        withNoSchema(MappingAcceptedMessage.class);
        withNoSchema(MappingProposedMessage.class);
        withNoSchema(ExchangeFailureMessage.class);
        withNoSchema(CacheStatisticsClearMessage.class);
        withNoSchema(ClientCacheChangeDummyDiscoveryMessage.class);
        withNoSchemaResolvedClassLoader(DynamicCacheChangeBatch.class);

        // [10000 - 10200]: Transaction and lock related messages. Most of them originally comes from Communication.
        msgIdx = 10000;
        withNoSchema(TxInfo.class);
        withSchema(TxEntriesInfo.class);
        withNoSchema(TxLock.class);
        withSchema(TxLocksRequest.class);
        withSchema(TxLocksResponse.class);
        withSchema(IgniteTxKey.class);
        withSchema(IgniteTxEntry.class);
        withSchema(TxEntryValueHolder.class);
        withNoSchema(GridCacheTxRecoveryRequest.class);
        withNoSchema(GridCacheTxRecoveryResponse.class);
        withNoSchema(GridDistributedTxFinishRequest.class);
        withNoSchema(GridDistributedTxFinishResponse.class);
        withSchema(GridDistributedTxPrepareRequest.class);
        withNoSchema(GridDistributedTxPrepareResponse.class);
        withNoSchema(GridDhtTxFinishRequest.class);
        withSchema(GridDhtTxFinishResponse.class);
        withSchema(GridDhtTxPrepareRequest.class);
        withSchema(GridDhtTxPrepareResponse.class);
        withNoSchema(GridNearTxFinishRequest.class);
        withNoSchema(GridNearTxFinishResponse.class);
        withNoSchema(GridNearTxPrepareRequest.class);
        withSchema(GridNearTxPrepareResponse.class);
        withSchema(GridDhtLockRequest.class);
        withSchema(GridDhtLockResponse.class);
        withSchema(GridDhtUnlockRequest.class);
        withNoSchema(GridNearLockRequest.class);
        withNoSchema(GridNearLockResponse.class);
        withSchema(GridNearUnlockRequest.class);
        withSchema(GridDistributedLockRequest.class);
        withSchema(GridDistributedLockResponse.class);
        withNoSchema(GridDhtTxOnePhaseCommitAckRequest.class);
        withSchema(TransactionAttributesAwareRequest.class);

        // [10300 - 10500]: Cache, DHT messages.
        msgIdx = 10300;
        withSchema(GridDhtForceKeysRequest.class);
        withSchema(GridDhtForceKeysResponse.class);
        withNoSchema(GridDhtAtomicDeferredUpdateResponse.class);
        withSchema(GridDhtAtomicUpdateRequest.class);
        withSchema(GridDhtAtomicUpdateResponse.class);
        withSchema(GridNearAtomicFullUpdateRequest.class);
        withSchema(GridDhtAtomicSingleUpdateRequest.class);
        withSchema(GridNearAtomicUpdateResponse.class);
        withSchema(GridNearAtomicSingleUpdateRequest.class);
        withSchema(GridNearAtomicSingleUpdateInvokeRequest.class);
        withSchema(GridNearAtomicSingleUpdateFilterRequest.class);
        withNoSchema(GridNearAtomicCheckUpdateRequest.class);
        withSchema(NearCacheUpdates.class);
        withSchema(GridNearGetRequest.class);
        withSchema(GridNearGetResponse.class);
        withSchema(GridNearSingleGetRequest.class);
        withSchema(GridNearSingleGetResponse.class);
        withNoSchema(GridDhtAtomicNearResponse.class);
        withSchema(GridCacheTtlUpdateRequest.class);
        withSchema(GridCacheReturn.class);
        withSchema(GridCacheEntryInfo.class);
        withSchema(CacheInvokeDirectResult.class);
        withNoSchema(GridCacheRawVersionedEntry.class);
        withSchema(CacheEvictionEntry.class);
        withSchema(CacheEntryPredicateAdapter.class);
        withNoSchema(GridContinuousMessage.class);
        withNoSchema(ContinuousRoutineStartResultMessage.class);
        withSchema(UpdateErrors.class);
        withNoSchema(LatchAckMessage.class);
        withSchema(AtomicApplicationAttributesAwareRequest.class);
        withNoSchema(StartRequestData.class);
        withNoSchema(StartRoutineDiscoveryMessage.class);
        withNoSchema(StartRoutineAckDiscoveryMessage.class);
        withNoSchema(StartRoutineDiscoveryMessageV2.class);

        // [10600-10800]: Affinity & partition maps.
        msgIdx = 10600;
        withNoSchema(GridDhtAffinityAssignmentRequest.class);
        withNoSchema(GridDhtAffinityAssignmentResponse.class);
        withNoSchema(CacheGroupAffinityMessage.class);
        withNoSchema(ExchangeInfo.class);
        withNoSchema(PartitionUpdateCountersMessage.class);
        withNoSchema(CachePartitionPartialCountersMap.class);
        withNoSchema(IgniteDhtDemandedPartitionsMap.class);
        withNoSchema(CachePartitionFullCountersMap.class);
        withNoSchema(GroupPartitionIdPair.class);
        withNoSchema(GridPartitionStateMap.class);
        withNoSchema(GridDhtPartitionMap.class);
        withNoSchema(GridDhtPartitionFullMap.class);
        withNoSchema(GridDhtPartitionExchangeId.class);
        withNoSchema(GridCheckpointRequest.class);
        withNoSchema(GridDhtPartitionDemandMessage.class);
        withSchema(GridDhtPartitionSupplyMessage.class);
        withNoSchema(GridDhtPartitionsFullMessage.class);
        withNoSchema(GridDhtPartitionsSingleMessage.class);
        withNoSchema(GridDhtPartitionsSingleRequest.class);

        // [10900-11100]: Query, schema and SQL related messages.
        msgIdx = 10900;
        withNoSchema(SchemaAlterTableAddColumnOperation.class);
        withNoSchema(SchemaIndexCreateOperation.class);
        withNoSchema(SchemaIndexDropOperation.class);
        withNoSchema(SchemaAlterTableDropColumnOperation.class);
        withNoSchema(SchemaAddQueryEntityOperation.class);
        withNoSchema(SchemaOperationStatusMessage.class);
        withNoSchema(SchemaProposeDiscoveryMessage.class);
        withNoSchema(SchemaFinishDiscoveryMessage.class);
        withNoSchema(QueryField.class);
        withNoSchema(GridCacheSqlQuery.class);
        withSchema(GridCacheQueryRequest.class);
        withSchema(GridCacheQueryResponse.class);
        withNoSchema(GridQueryCancelRequest.class);
        withNoSchema(GridQueryFailResponse.class);
        withNoSchema(GridQueryNextPageRequest.class);
        withNoSchema(GridQueryNextPageResponse.class);
        withNoSchema(GridQueryKillRequest.class);
        withNoSchema(GridQueryKillResponse.class);
        withNoSchema(IndexKeyDefinition.class);
        withNoSchema(IndexKeyTypeSettings.class);
        withNoSchema(IndexQueryResultMeta.class);
        withNoSchema(StatisticsKeyMessage.class);
        withNoSchema(StatisticsDecimalMessage.class);
        withNoSchema(StatisticsObjectData.class);
        withNoSchema(StatisticsColumnData.class);
        withNoSchema(StatisticsRequest.class);
        withNoSchema(StatisticsResponse.class);
        withNoSchema(CacheContinuousQueryBatchAck.class);
        withSchema(CacheContinuousQueryEntry.class);

        // [11200 - 11300]: Compute, distributed process messages.
        msgIdx = 11200;
        withNoSchema(GridJobCancelRequest.class);
        withSchema(GridJobExecuteRequest.class);
        withSchema(GridJobExecuteResponse.class);
        withNoSchema(GridJobSiblingsRequest.class);
        withSchema(GridJobSiblingsResponse.class);
        withNoSchema(GridTaskCancelRequest.class);
        withSchema(GridTaskSessionRequest.class);
        withNoSchema(GridTaskResultRequest.class);
        withSchema(GridTaskResultResponse.class);
        withNoSchema(JobStealingRequest.class);
        withNoSchema(SingleNodeMessage.class);

        // [11500 - 11600]:  IO, networking messages.
        msgIdx = NODE_ID_MSG_TYPE;
        withNoSchema(NodeIdMessage.class);
        withNoSchema(HandshakeMessage.class);
        withNoSchema(HandshakeWaitMessage.class);
        withSchema(GridIoMessage.class);
        withNoSchema(IgniteIoTestMessage.class);
        withSchema(GridIoUserMessage.class);
        withSchema(GridIoSecurityAwareMessage.class);
        withNoSchema(RecoveryLastReceivedMessage.class);
        withNoSchema(TcpInverseConnectionResponseMessage.class);
        withNoSchema(SessionChannelMessage.class);

        // [11700 - 11800]: Datastreamer messages.
        msgIdx = 11700;
        withNoSchema(DataStreamerUpdatesHandlerResult.class);
        withSchema(DataStreamerEntry.class);
        withNoSchema(DataStreamerRequest.class);
        withNoSchema(DataStreamerResponse.class);

        // [11900 - 12000]: Metrics, monitoring messages.
        msgIdx = 11900;
        withNoSchema(CacheMetricsMessage.class);
        withNoSchema(NodeMetricsMessage.class);
        withNoSchema(NodeFullMetricsMessage.class);
        withNoSchema(ClusterMetricsUpdateMessage.class);
        withNoSchema(TcpDiscoveryClientNodesMetricsMessage.class);
        withNoSchema(TcpDiscoveryMetricsUpdateMessage.class);
        withNoSchema(TcpDiscoveryClientMetricsUpdateMessage.class);

        // [12000 - 12100]: Authentication, security messages.
        msgIdx = 12000;
        withNoSchema(User.class);
        withNoSchema(UserManagementOperation.class);
        withNoSchema(UserManagementOperationFinishedMessage.class);
        withNoSchema(UserAuthenticateRequestMessage.class);
        withNoSchema(UserAuthenticateResponseMessage.class);
        withNoSchema(TcpDiscoveryAuthFailedMessage.class);

        // [12200 - 12300]: Binary, classloading and marshalling messages.
        msgIdx = 12200;
        withNoSchema(GridDeploymentInfoBean.class);
        withNoSchema(GridDeploymentRequest.class);
        withNoSchema(GridDeploymentResponse.class);
        withNoSchema(MissingMappingRequestMessage.class);
        withNoSchema(MissingMappingResponseMessage.class);
        withNoSchema(MetadataRequestMessage.class);
        withNoSchema(MetadataResponseMessage.class);
        withNoSchema(MarshallerMappingItem.class);
        withNoSchema(BinaryMetadataVersionInfo.class);

        // [12400 - 12500]: Encryption messages.
        msgIdx = 12400;
        withNoSchema(GenerateEncryptionKeyRequest.class);
        withNoSchema(GenerateEncryptionKeyResponse.class);
        withNoSchema(ChangeCacheEncryptionRequest.class);
        withNoSchema(MasterKeyChangeRequest.class);

        // [13000 - 13300]: Control, diagnostincs and other messages.
        msgIdx = 13000;
        withSchema(GridEventStorageMessage.class);
        withNoSchema(ChangeGlobalStateMessage.class);
        withNoSchema(GridChangeGlobalStateMessageResponse.class);
        withSchema(IgniteDiagnosticRequest.class);
        withNoSchema(IgniteDiagnosticResponse.class);
        withNoSchema(WalStateAckMessage.class);

        assert msgIdx <= MAX_MESSAGE_ID;
    }

    /** Registers message using {@link #schemaAwareMarhaller} and {@link U#gridClassLoader()}. */
    private <T extends Message> void withSchema(Class<T> cls) {
        register(cls, schemaAwareMarhaller, U.gridClassLoader());
    }

    /** Registers message using {@link #schemaLessMarshaller} and {@link U#gridClassLoader()}. */
    private <T extends Message> void withNoSchema(Class<T> cls) {
        register(cls, schemaLessMarshaller, U.gridClassLoader());
    }

    /** Registers message using {@link #schemaLessMarshaller} and {@link #resolvedClsLdr}. */
    private <T extends Message> void withNoSchemaResolvedClassLoader(Class<T> cls) {
        register(cls, schemaLessMarshaller, resolvedClsLdr);
    }

    /** Registers message using incrementing {@link #msgIdx} as the message id/type. */
    private <T extends Message> void register(Class<T> cls, Marshaller marsh, ClassLoader clsLrd) {
        Constructor<T> ctor;
        MessageSerializer<T> serializer;

        try {
            ctor = cls.getConstructor();

            boolean marshallable = MarshallableMessage.class.isAssignableFrom(cls);

            Class<?> serCls = Class.forName(cls.getName() + (marshallable ? "MarshallableSerializer" : "Serializer"));

            serializer = marshallable
                ? (MessageSerializer<T>)serCls.getConstructor(Marshaller.class, ClassLoader.class).newInstance(marsh, clsLrd)
                : (MessageSerializer<T>)serCls.getConstructor().newInstance();
        }
        catch (Exception e) {
            throw new IgniteException("Failed to register message of type " + cls.getSimpleName(), e);
        }

        factory.register(
            msgIdx++,
            () -> {
                try {
                    return ctor.newInstance();
                }
                catch (Exception e) {
                    throw new IgniteException("Failed to create message of type " + cls.getSimpleName(), e);
                }
            },
            serializer
        );
    }
}
