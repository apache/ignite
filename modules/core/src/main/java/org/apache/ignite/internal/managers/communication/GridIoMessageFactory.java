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

package org.apache.ignite.internal.managers.communication;

import org.apache.ignite.internal.ExchangeInfo;
import org.apache.ignite.internal.GridJobCancelRequest;
import org.apache.ignite.internal.GridJobExecuteRequest;
import org.apache.ignite.internal.GridJobExecuteResponse;
import org.apache.ignite.internal.GridJobSiblingsRequest;
import org.apache.ignite.internal.GridJobSiblingsResponse;
import org.apache.ignite.internal.GridTaskCancelRequest;
import org.apache.ignite.internal.GridTaskSessionRequest;
import org.apache.ignite.internal.IgniteDiagnosticRequest;
import org.apache.ignite.internal.IgniteDiagnosticResponse;
import org.apache.ignite.internal.TxEntriesInfo;
import org.apache.ignite.internal.TxInfo;
import org.apache.ignite.internal.cache.query.index.IndexKeyTypeMessage;
import org.apache.ignite.internal.cache.query.index.IndexQueryResultMeta;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.codegen.AtomicApplicationAttributesAwareRequestSerializer;
import org.apache.ignite.internal.codegen.BinaryMetadataVersionInfoSerializer;
import org.apache.ignite.internal.codegen.CacheContinuousQueryBatchAckSerializer;
import org.apache.ignite.internal.codegen.CacheEntryInfoCollectionSerializer;
import org.apache.ignite.internal.codegen.CacheEntryPredicateAdapterSerializer;
import org.apache.ignite.internal.codegen.CacheEvictionEntrySerializer;
import org.apache.ignite.internal.codegen.CacheGroupAffinityMessageSerializer;
import org.apache.ignite.internal.codegen.CacheInvokeDirectResultSerializer;
import org.apache.ignite.internal.codegen.CacheMetricsMessageSerializer;
import org.apache.ignite.internal.codegen.CachePartitionFullCountersMapSerializer;
import org.apache.ignite.internal.codegen.CachePartitionPartialCountersMapSerializer;
import org.apache.ignite.internal.codegen.CachePartitionsToReloadMapSerializer;
import org.apache.ignite.internal.codegen.CacheVersionedValueSerializer;
import org.apache.ignite.internal.codegen.ClusterMetricsUpdateMessageSerializer;
import org.apache.ignite.internal.codegen.ContinuousRoutineStartResultMessageSerializer;
import org.apache.ignite.internal.codegen.ErrorMessageSerializer;
import org.apache.ignite.internal.codegen.ExchangeInfoSerializer;
import org.apache.ignite.internal.codegen.GenerateEncryptionKeyRequestSerializer;
import org.apache.ignite.internal.codegen.GridCacheEntryInfoSerializer;
import org.apache.ignite.internal.codegen.GridCacheQueryResponseSerializer;
import org.apache.ignite.internal.codegen.GridCacheReturnSerializer;
import org.apache.ignite.internal.codegen.GridCacheSqlQuerySerializer;
import org.apache.ignite.internal.codegen.GridCacheTtlUpdateRequestSerializer;
import org.apache.ignite.internal.codegen.GridCacheTxRecoveryRequestSerializer;
import org.apache.ignite.internal.codegen.GridCacheTxRecoveryResponseSerializer;
import org.apache.ignite.internal.codegen.GridCacheVersionExSerializer;
import org.apache.ignite.internal.codegen.GridCacheVersionSerializer;
import org.apache.ignite.internal.codegen.GridChangeGlobalStateMessageResponseSerializer;
import org.apache.ignite.internal.codegen.GridCheckpointRequestSerializer;
import org.apache.ignite.internal.codegen.GridDeploymentResponseSerializer;
import org.apache.ignite.internal.codegen.GridDhtAffinityAssignmentRequestSerializer;
import org.apache.ignite.internal.codegen.GridDhtAffinityAssignmentResponseSerializer;
import org.apache.ignite.internal.codegen.GridDhtAtomicDeferredUpdateResponseSerializer;
import org.apache.ignite.internal.codegen.GridDhtAtomicNearResponseSerializer;
import org.apache.ignite.internal.codegen.GridDhtAtomicSingleUpdateRequestSerializer;
import org.apache.ignite.internal.codegen.GridDhtAtomicUpdateRequestSerializer;
import org.apache.ignite.internal.codegen.GridDhtAtomicUpdateResponseSerializer;
import org.apache.ignite.internal.codegen.GridDhtForceKeysRequestSerializer;
import org.apache.ignite.internal.codegen.GridDhtForceKeysResponseSerializer;
import org.apache.ignite.internal.codegen.GridDhtLockRequestSerializer;
import org.apache.ignite.internal.codegen.GridDhtLockResponseSerializer;
import org.apache.ignite.internal.codegen.GridDhtPartitionDemandMessageSerializer;
import org.apache.ignite.internal.codegen.GridDhtPartitionExchangeIdSerializer;
import org.apache.ignite.internal.codegen.GridDhtPartitionSupplyMessageSerializer;
import org.apache.ignite.internal.codegen.GridDhtPartitionsFullMessageSerializer;
import org.apache.ignite.internal.codegen.GridDhtPartitionsSingleMessageSerializer;
import org.apache.ignite.internal.codegen.GridDhtPartitionsSingleRequestSerializer;
import org.apache.ignite.internal.codegen.GridDhtTxFinishRequestSerializer;
import org.apache.ignite.internal.codegen.GridDhtTxFinishResponseSerializer;
import org.apache.ignite.internal.codegen.GridDhtTxOnePhaseCommitAckRequestSerializer;
import org.apache.ignite.internal.codegen.GridDhtTxPrepareRequestSerializer;
import org.apache.ignite.internal.codegen.GridDhtTxPrepareResponseSerializer;
import org.apache.ignite.internal.codegen.GridDhtUnlockRequestSerializer;
import org.apache.ignite.internal.codegen.GridDistributedLockRequestSerializer;
import org.apache.ignite.internal.codegen.GridDistributedLockResponseSerializer;
import org.apache.ignite.internal.codegen.GridDistributedTxFinishRequestSerializer;
import org.apache.ignite.internal.codegen.GridDistributedTxFinishResponseSerializer;
import org.apache.ignite.internal.codegen.GridDistributedTxPrepareRequestSerializer;
import org.apache.ignite.internal.codegen.GridDistributedTxPrepareResponseSerializer;
import org.apache.ignite.internal.codegen.GridJobCancelRequestSerializer;
import org.apache.ignite.internal.codegen.GridJobExecuteRequestSerializer;
import org.apache.ignite.internal.codegen.GridJobExecuteResponseSerializer;
import org.apache.ignite.internal.codegen.GridJobSiblingsRequestSerializer;
import org.apache.ignite.internal.codegen.GridJobSiblingsResponseSerializer;
import org.apache.ignite.internal.codegen.GridNearAtomicCheckUpdateRequestSerializer;
import org.apache.ignite.internal.codegen.GridNearAtomicFullUpdateRequestSerializer;
import org.apache.ignite.internal.codegen.GridNearAtomicSingleUpdateFilterRequestSerializer;
import org.apache.ignite.internal.codegen.GridNearAtomicSingleUpdateInvokeRequestSerializer;
import org.apache.ignite.internal.codegen.GridNearAtomicSingleUpdateRequestSerializer;
import org.apache.ignite.internal.codegen.GridNearAtomicUpdateResponseSerializer;
import org.apache.ignite.internal.codegen.GridNearGetRequestSerializer;
import org.apache.ignite.internal.codegen.GridNearGetResponseSerializer;
import org.apache.ignite.internal.codegen.GridNearLockRequestSerializer;
import org.apache.ignite.internal.codegen.GridNearLockResponseSerializer;
import org.apache.ignite.internal.codegen.GridNearSingleGetRequestSerializer;
import org.apache.ignite.internal.codegen.GridNearSingleGetResponseSerializer;
import org.apache.ignite.internal.codegen.GridNearTxFinishRequestSerializer;
import org.apache.ignite.internal.codegen.GridNearTxFinishResponseSerializer;
import org.apache.ignite.internal.codegen.GridNearTxPrepareRequestSerializer;
import org.apache.ignite.internal.codegen.GridNearTxPrepareResponseSerializer;
import org.apache.ignite.internal.codegen.GridNearUnlockRequestSerializer;
import org.apache.ignite.internal.codegen.GridQueryCancelRequestSerializer;
import org.apache.ignite.internal.codegen.GridQueryFailResponseSerializer;
import org.apache.ignite.internal.codegen.GridQueryKillRequestSerializer;
import org.apache.ignite.internal.codegen.GridQueryKillResponseSerializer;
import org.apache.ignite.internal.codegen.GridQueryNextPageRequestSerializer;
import org.apache.ignite.internal.codegen.GridQueryNextPageResponseSerializer;
import org.apache.ignite.internal.codegen.GridTaskCancelRequestSerializer;
import org.apache.ignite.internal.codegen.GridTaskResultRequestSerializer;
import org.apache.ignite.internal.codegen.GridTaskResultResponseSerializer;
import org.apache.ignite.internal.codegen.GridTaskSessionRequestSerializer;
import org.apache.ignite.internal.codegen.GroupPartitionIdPairSerializer;
import org.apache.ignite.internal.codegen.HandshakeMessageSerializer;
import org.apache.ignite.internal.codegen.HandshakeWaitMessageSerializer;
import org.apache.ignite.internal.codegen.IgniteDhtDemandedPartitionsMapSerializer;
import org.apache.ignite.internal.codegen.IgniteDhtPartitionCountersMapSerializer;
import org.apache.ignite.internal.codegen.IgniteDhtPartitionHistorySuppliersMapSerializer;
import org.apache.ignite.internal.codegen.IgniteDhtPartitionsToReloadMapSerializer;
import org.apache.ignite.internal.codegen.IgniteDiagnosticRequestSerializer;
import org.apache.ignite.internal.codegen.IgniteDiagnosticResponseSerializer;
import org.apache.ignite.internal.codegen.IgniteTxKeySerializer;
import org.apache.ignite.internal.codegen.IncrementalSnapshotAwareMessageSerializer;
import org.apache.ignite.internal.codegen.IndexKeyDefinitionSerializer;
import org.apache.ignite.internal.codegen.IndexKeyTypeMessageSerializer;
import org.apache.ignite.internal.codegen.IndexKeyTypeSettingsSerializer;
import org.apache.ignite.internal.codegen.IndexQueryResultMetaSerializer;
import org.apache.ignite.internal.codegen.IntLongMapSerializer;
import org.apache.ignite.internal.codegen.JobStealingRequestSerializer;
import org.apache.ignite.internal.codegen.LatchAckMessageSerializer;
import org.apache.ignite.internal.codegen.MetadataRequestMessageSerializer;
import org.apache.ignite.internal.codegen.MetadataResponseMessageSerializer;
import org.apache.ignite.internal.codegen.MissingMappingRequestMessageSerializer;
import org.apache.ignite.internal.codegen.MissingMappingResponseMessageSerializer;
import org.apache.ignite.internal.codegen.NearCacheUpdatesSerializer;
import org.apache.ignite.internal.codegen.NodeFullMetricsMessageSerializer;
import org.apache.ignite.internal.codegen.NodeIdMessageSerializer;
import org.apache.ignite.internal.codegen.NodeMetricsMessageSerializer;
import org.apache.ignite.internal.codegen.PartitionReservationsMapSerializer;
import org.apache.ignite.internal.codegen.PartitionUpdateCountersMessageSerializer;
import org.apache.ignite.internal.codegen.PartitionsToReloadSerializer;
import org.apache.ignite.internal.codegen.RecoveryLastReceivedMessageSerializer;
import org.apache.ignite.internal.codegen.SchemaOperationStatusMessageSerializer;
import org.apache.ignite.internal.codegen.ServiceDeploymentProcessIdSerializer;
import org.apache.ignite.internal.codegen.ServiceSingleNodeDeploymentResultBatchSerializer;
import org.apache.ignite.internal.codegen.SessionChannelMessageSerializer;
import org.apache.ignite.internal.codegen.SnapshotFilesFailureMessageSerializer;
import org.apache.ignite.internal.codegen.SnapshotFilesRequestMessageSerializer;
import org.apache.ignite.internal.codegen.TcpInverseConnectionResponseMessageSerializer;
import org.apache.ignite.internal.codegen.TransactionAttributesAwareRequestSerializer;
import org.apache.ignite.internal.codegen.TxEntriesInfoSerializer;
import org.apache.ignite.internal.codegen.TxEntryValueHolderSerializer;
import org.apache.ignite.internal.codegen.TxInfoSerializer;
import org.apache.ignite.internal.codegen.TxLockListSerializer;
import org.apache.ignite.internal.codegen.TxLockSerializer;
import org.apache.ignite.internal.codegen.TxLocksRequestSerializer;
import org.apache.ignite.internal.codegen.TxLocksResponseSerializer;
import org.apache.ignite.internal.codegen.UUIDCollectionMessageSerializer;
import org.apache.ignite.internal.codegen.UpdateErrorsSerializer;
import org.apache.ignite.internal.codegen.UserAuthenticateRequestMessageSerializer;
import org.apache.ignite.internal.codegen.UserAuthenticateResponseMessageSerializer;
import org.apache.ignite.internal.codegen.UserManagementOperationFinishedMessageSerializer;
import org.apache.ignite.internal.codegen.WalStateAckMessageSerializer;
import org.apache.ignite.internal.managers.checkpoint.GridCheckpointRequest;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfoBean;
import org.apache.ignite.internal.managers.deployment.GridDeploymentRequest;
import org.apache.ignite.internal.managers.deployment.GridDeploymentResponse;
import org.apache.ignite.internal.managers.encryption.GenerateEncryptionKeyRequest;
import org.apache.ignite.internal.managers.encryption.GenerateEncryptionKeyResponse;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageMessage;
import org.apache.ignite.internal.processors.authentication.UserAuthenticateRequestMessage;
import org.apache.ignite.internal.processors.authentication.UserAuthenticateResponseMessage;
import org.apache.ignite.internal.processors.authentication.UserManagementOperationFinishedMessage;
import org.apache.ignite.internal.processors.cache.CacheEntryInfoCollection;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicateAdapter;
import org.apache.ignite.internal.processors.cache.CacheEvictionEntry;
import org.apache.ignite.internal.processors.cache.CacheInvokeDirectResult;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridChangeGlobalStateMessageResponse;
import org.apache.ignite.internal.processors.cache.WalStateAckMessage;
import org.apache.ignite.internal.processors.cache.binary.BinaryMetadataVersionInfo;
import org.apache.ignite.internal.processors.cache.binary.MetadataRequestMessage;
import org.apache.ignite.internal.processors.cache.binary.MetadataResponseMessage;
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
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionsToReloadMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtForceKeysRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtForceKeysResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GroupPartitionIdPair;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtDemandedPartitionsMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtPartitionCountersMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtPartitionHistorySuppliersMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtPartitionsToReloadMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IntLongMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionReservationsMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsToReload;
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
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IncrementalSnapshotAwareMessage;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotFilesFailureMessage;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotFilesRequestMessage;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryRequest;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryResponse;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryBatchAck;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.transactions.TxEntryValueHolder;
import org.apache.ignite.internal.processors.cache.transactions.TxLock;
import org.apache.ignite.internal.processors.cache.transactions.TxLockList;
import org.apache.ignite.internal.processors.cache.transactions.TxLocksRequest;
import org.apache.ignite.internal.processors.cache.transactions.TxLocksResponse;
import org.apache.ignite.internal.processors.cache.version.GridCacheRawVersionedEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionEx;
import org.apache.ignite.internal.processors.cluster.CacheMetricsMessage;
import org.apache.ignite.internal.processors.cluster.ClusterMetricsUpdateMessage;
import org.apache.ignite.internal.processors.cluster.NodeFullMetricsMessage;
import org.apache.ignite.internal.processors.cluster.NodeMetricsMessage;
import org.apache.ignite.internal.processors.continuous.ContinuousRoutineStartResultMessage;
import org.apache.ignite.internal.processors.continuous.GridContinuousMessage;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerEntry;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerRequest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerResponse;
import org.apache.ignite.internal.processors.marshaller.MissingMappingRequestMessage;
import org.apache.ignite.internal.processors.marshaller.MissingMappingResponseMessage;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryCancelRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryFailResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryNextPageRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryNextPageResponse;
import org.apache.ignite.internal.processors.query.messages.GridQueryKillRequest;
import org.apache.ignite.internal.processors.query.messages.GridQueryKillResponse;
import org.apache.ignite.internal.processors.query.schema.message.SchemaOperationStatusMessage;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsColumnData;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsDecimalMessage;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsKeyMessage;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsObjectData;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsRequest;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsResponse;
import org.apache.ignite.internal.processors.rest.handlers.task.GridTaskResultRequest;
import org.apache.ignite.internal.processors.rest.handlers.task.GridTaskResultResponse;
import org.apache.ignite.internal.processors.service.ServiceDeploymentProcessId;
import org.apache.ignite.internal.processors.service.ServiceSingleNodeDeploymentResult;
import org.apache.ignite.internal.processors.service.ServiceSingleNodeDeploymentResultBatch;
import org.apache.ignite.internal.util.GridByteArrayList;
import org.apache.ignite.internal.util.UUIDCollectionMessage;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.spi.collision.jobstealing.JobStealingRequest;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.communication.tcp.internal.TcpInverseConnectionResponseMessage;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeMessage;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeWaitMessage;
import org.apache.ignite.spi.communication.tcp.messages.NodeIdMessage;
import org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessage;

/**
 * Message factory implementation.
 */
public class GridIoMessageFactory implements MessageFactoryProvider {
    /** {@inheritDoc} */
    @Override public void registerAll(MessageFactory factory) {
        // -54 is reserved for SQL.
        factory.register((short)-100, ErrorMessage::new, new ErrorMessageSerializer());
        factory.register((short)-65, TxInfo::new, new TxInfoSerializer());
        factory.register((short)-64, TxEntriesInfo::new, new TxEntriesInfoSerializer());
        factory.register((short)-63, ExchangeInfo::new, new ExchangeInfoSerializer());
        factory.register((short)-62, IgniteDiagnosticResponse::new, new IgniteDiagnosticResponseSerializer());
        factory.register((short)-61, IgniteDiagnosticRequest::new, new IgniteDiagnosticRequestSerializer());
        factory.register((short)-53, SchemaOperationStatusMessage::new, new SchemaOperationStatusMessageSerializer());
        factory.register((short)-51, NearCacheUpdates::new, new NearCacheUpdatesSerializer());
        factory.register((short)-50, GridNearAtomicCheckUpdateRequest::new, new GridNearAtomicCheckUpdateRequestSerializer());
        factory.register((short)-49, UpdateErrors::new, new UpdateErrorsSerializer());
        factory.register((short)-48, GridDhtAtomicNearResponse::new, new GridDhtAtomicNearResponseSerializer());
        factory.register((short)-45, GridChangeGlobalStateMessageResponse::new, new GridChangeGlobalStateMessageResponseSerializer());
        factory.register((short)-43, IgniteIoTestMessage::new);
        factory.register((short)-36, GridDhtAtomicSingleUpdateRequest::new, new GridDhtAtomicSingleUpdateRequestSerializer());
        factory.register((short)-27, GridDhtTxOnePhaseCommitAckRequest::new, new GridDhtTxOnePhaseCommitAckRequestSerializer());
        factory.register((short)-26, TxLockList::new, new TxLockListSerializer());
        factory.register((short)-25, TxLock::new, new TxLockSerializer());
        factory.register((short)-24, TxLocksRequest::new, new TxLocksRequestSerializer());
        factory.register((short)-23, TxLocksResponse::new, new TxLocksResponseSerializer());
        factory.register(TcpCommunicationSpi.NODE_ID_MSG_TYPE, NodeIdMessage::new, new NodeIdMessageSerializer());
        factory.register(TcpCommunicationSpi.RECOVERY_LAST_ID_MSG_TYPE, RecoveryLastReceivedMessage::new,
            new RecoveryLastReceivedMessageSerializer());
        factory.register(TcpCommunicationSpi.HANDSHAKE_MSG_TYPE, HandshakeMessage::new, new HandshakeMessageSerializer());
        factory.register(TcpCommunicationSpi.HANDSHAKE_WAIT_MSG_TYPE, HandshakeWaitMessage::new, new HandshakeWaitMessageSerializer());
        factory.register((short)0, GridJobCancelRequest::new, new GridJobCancelRequestSerializer());
        factory.register((short)1, GridJobExecuteRequest::new, new GridJobExecuteRequestSerializer());
        factory.register((short)2, GridJobExecuteResponse::new, new GridJobExecuteResponseSerializer());
        factory.register((short)3, GridJobSiblingsRequest::new, new GridJobSiblingsRequestSerializer());
        factory.register((short)4, GridJobSiblingsResponse::new, new GridJobSiblingsResponseSerializer());
        factory.register((short)5, GridTaskCancelRequest::new, new GridTaskCancelRequestSerializer());
        factory.register((short)6, GridTaskSessionRequest::new, new GridTaskSessionRequestSerializer());
        factory.register((short)7, GridCheckpointRequest::new, new GridCheckpointRequestSerializer());
        factory.register((short)8, GridIoMessage::new);
        factory.register((short)9, GridIoUserMessage::new);
        factory.register((short)10, GridDeploymentInfoBean::new);
        factory.register((short)11, GridDeploymentRequest::new);
        factory.register((short)12, GridDeploymentResponse::new, new GridDeploymentResponseSerializer());
        factory.register((short)13, GridEventStorageMessage::new);
        factory.register((short)16, GridCacheTxRecoveryRequest::new, new GridCacheTxRecoveryRequestSerializer());
        factory.register((short)17, GridCacheTxRecoveryResponse::new, new GridCacheTxRecoveryResponseSerializer());
        factory.register((short)18, IndexQueryResultMeta::new, new IndexQueryResultMetaSerializer());
        factory.register((short)19, IndexKeyTypeSettings::new, new IndexKeyTypeSettingsSerializer());
        factory.register((short)20, GridCacheTtlUpdateRequest::new, new GridCacheTtlUpdateRequestSerializer());
        factory.register((short)21, GridDistributedLockRequest::new, new GridDistributedLockRequestSerializer());
        factory.register((short)22, GridDistributedLockResponse::new, new GridDistributedLockResponseSerializer());
        factory.register((short)23, GridDistributedTxFinishRequest::new, new GridDistributedTxFinishRequestSerializer());
        factory.register((short)24, GridDistributedTxFinishResponse::new, new GridDistributedTxFinishResponseSerializer());
        factory.register((short)25, GridDistributedTxPrepareRequest::new, new GridDistributedTxPrepareRequestSerializer());
        factory.register((short)26, GridDistributedTxPrepareResponse::new, new GridDistributedTxPrepareResponseSerializer());
        // Type 27 is former GridDistributedUnlockRequest
        factory.register((short)28, GridDhtAffinityAssignmentRequest::new, new GridDhtAffinityAssignmentRequestSerializer());
        factory.register((short)29, GridDhtAffinityAssignmentResponse::new, new GridDhtAffinityAssignmentResponseSerializer());
        factory.register((short)30, GridDhtLockRequest::new, new GridDhtLockRequestSerializer());
        factory.register((short)31, GridDhtLockResponse::new, new GridDhtLockResponseSerializer());
        factory.register((short)32, GridDhtTxFinishRequest::new, new GridDhtTxFinishRequestSerializer());
        factory.register((short)33, GridDhtTxFinishResponse::new, new GridDhtTxFinishResponseSerializer());
        factory.register((short)34, GridDhtTxPrepareRequest::new, new GridDhtTxPrepareRequestSerializer());
        factory.register((short)35, GridDhtTxPrepareResponse::new, new GridDhtTxPrepareResponseSerializer());
        factory.register((short)36, GridDhtUnlockRequest::new, new GridDhtUnlockRequestSerializer());
        factory.register((short)37, GridDhtAtomicDeferredUpdateResponse::new, new GridDhtAtomicDeferredUpdateResponseSerializer());
        factory.register((short)38, GridDhtAtomicUpdateRequest::new, new GridDhtAtomicUpdateRequestSerializer());
        factory.register((short)39, GridDhtAtomicUpdateResponse::new, new GridDhtAtomicUpdateResponseSerializer());
        factory.register((short)40, GridNearAtomicFullUpdateRequest::new, new GridNearAtomicFullUpdateRequestSerializer());
        factory.register((short)41, GridNearAtomicUpdateResponse::new, new GridNearAtomicUpdateResponseSerializer());
        factory.register((short)42, GridDhtForceKeysRequest::new, new GridDhtForceKeysRequestSerializer());
        factory.register((short)43, GridDhtForceKeysResponse::new, new GridDhtForceKeysResponseSerializer());
        factory.register((short)45, GridDhtPartitionDemandMessage::new, new GridDhtPartitionDemandMessageSerializer());
        factory.register((short)46, GridDhtPartitionsFullMessage::new, new GridDhtPartitionsFullMessageSerializer());
        factory.register((short)47, GridDhtPartitionsSingleMessage::new, new GridDhtPartitionsSingleMessageSerializer());
        factory.register((short)48, GridDhtPartitionsSingleRequest::new, new GridDhtPartitionsSingleRequestSerializer());
        factory.register((short)49, GridNearGetRequest::new, new GridNearGetRequestSerializer());
        factory.register((short)50, GridNearGetResponse::new, new GridNearGetResponseSerializer());
        factory.register((short)51, GridNearLockRequest::new, new GridNearLockRequestSerializer());
        factory.register((short)52, GridNearLockResponse::new, new GridNearLockResponseSerializer());
        factory.register((short)53, GridNearTxFinishRequest::new, new GridNearTxFinishRequestSerializer());
        factory.register((short)54, GridNearTxFinishResponse::new, new GridNearTxFinishResponseSerializer());
        factory.register((short)55, GridNearTxPrepareRequest::new, new GridNearTxPrepareRequestSerializer());
        factory.register((short)56, GridNearTxPrepareResponse::new, new GridNearTxPrepareResponseSerializer());
        factory.register((short)57, GridNearUnlockRequest::new, new GridNearUnlockRequestSerializer());
        factory.register((short)58, GridCacheQueryRequest::new);
        factory.register((short)59, GridCacheQueryResponse::new, new GridCacheQueryResponseSerializer());
        factory.register((short)61, GridContinuousMessage::new);
        factory.register((short)62, DataStreamerRequest::new);
        factory.register((short)63, DataStreamerResponse::new);
        factory.register((short)76, GridTaskResultRequest::new, new GridTaskResultRequestSerializer());
        factory.register((short)77, GridTaskResultResponse::new, new GridTaskResultResponseSerializer());
        factory.register((short)78, MissingMappingRequestMessage::new, new MissingMappingRequestMessageSerializer());
        factory.register((short)79, MissingMappingResponseMessage::new, new MissingMappingResponseMessageSerializer());
        factory.register((short)80, MetadataRequestMessage::new, new MetadataRequestMessageSerializer());
        factory.register((short)81, MetadataResponseMessage::new, new MetadataResponseMessageSerializer());
        factory.register((short)82, JobStealingRequest::new, new JobStealingRequestSerializer());
        factory.register((short)84, GridByteArrayList::new);
        factory.register((short)86, GridCacheVersion::new, new GridCacheVersionSerializer());
        factory.register((short)87, GridDhtPartitionExchangeId::new, new GridDhtPartitionExchangeIdSerializer());
        factory.register((short)88, GridCacheReturn::new, new GridCacheReturnSerializer());
        factory.register((short)91, GridCacheEntryInfo::new, new GridCacheEntryInfoSerializer());
        factory.register((short)92, CacheEntryInfoCollection::new, new CacheEntryInfoCollectionSerializer());
        factory.register((short)93, CacheInvokeDirectResult::new, new CacheInvokeDirectResultSerializer());
        factory.register((short)94, IgniteTxKey::new, new IgniteTxKeySerializer());
        factory.register((short)95, DataStreamerEntry::new);
        factory.register((short)96, CacheContinuousQueryEntry::new);
        factory.register((short)97, CacheEvictionEntry::new, new CacheEvictionEntrySerializer());
        factory.register((short)98, CacheEntryPredicateAdapter::new, new CacheEntryPredicateAdapterSerializer());
        factory.register((short)100, IgniteTxEntry::new);
        factory.register((short)101, TxEntryValueHolder::new, new TxEntryValueHolderSerializer());
        factory.register((short)102, CacheVersionedValue::new, new CacheVersionedValueSerializer());
        factory.register((short)103, GridCacheRawVersionedEntry::new);
        factory.register((short)104, GridCacheVersionEx::new, new GridCacheVersionExSerializer());
        factory.register((short)106, GridQueryCancelRequest::new, new GridQueryCancelRequestSerializer());
        factory.register((short)107, GridQueryFailResponse::new, new GridQueryFailResponseSerializer());
        factory.register((short)108, GridQueryNextPageRequest::new, new GridQueryNextPageRequestSerializer());
        factory.register((short)109, GridQueryNextPageResponse::new, new GridQueryNextPageResponseSerializer());
        factory.register((short)112, GridCacheSqlQuery::new, new GridCacheSqlQuerySerializer());
        factory.register((short)113, IndexKeyDefinition::new, new IndexKeyDefinitionSerializer());
        factory.register((short)114, GridDhtPartitionSupplyMessage::new, new GridDhtPartitionSupplyMessageSerializer());
        factory.register((short)115, UUIDCollectionMessage::new, new UUIDCollectionMessageSerializer());
        factory.register((short)116, GridNearSingleGetRequest::new, new GridNearSingleGetRequestSerializer());
        factory.register((short)117, GridNearSingleGetResponse::new, new GridNearSingleGetResponseSerializer());
        factory.register((short)118, CacheContinuousQueryBatchAck::new, new CacheContinuousQueryBatchAckSerializer());

        // [120..123] - DR
        factory.register((short)125, GridNearAtomicSingleUpdateRequest::new, new GridNearAtomicSingleUpdateRequestSerializer());
        factory.register((short)126, GridNearAtomicSingleUpdateInvokeRequest::new, new GridNearAtomicSingleUpdateInvokeRequestSerializer());
        factory.register((short)127, GridNearAtomicSingleUpdateFilterRequest::new, new GridNearAtomicSingleUpdateFilterRequestSerializer());
        factory.register((short)128, CacheGroupAffinityMessage::new, new CacheGroupAffinityMessageSerializer());
        factory.register((short)129, WalStateAckMessage::new, new WalStateAckMessageSerializer());
        factory.register((short)130, UserManagementOperationFinishedMessage::new, new UserManagementOperationFinishedMessageSerializer());
        factory.register((short)131, UserAuthenticateRequestMessage::new, new UserAuthenticateRequestMessageSerializer());
        factory.register((short)132, UserAuthenticateResponseMessage::new, new UserAuthenticateResponseMessageSerializer());
        factory.register(ClusterMetricsUpdateMessage.TYPE_CODE, ClusterMetricsUpdateMessage::new,
            new ClusterMetricsUpdateMessageSerializer());
        factory.register((short)134, ContinuousRoutineStartResultMessage::new, new ContinuousRoutineStartResultMessageSerializer());
        factory.register((short)135, LatchAckMessage::new, new LatchAckMessageSerializer());
        factory.register(CacheMetricsMessage.TYPE_CODE, CacheMetricsMessage::new, new CacheMetricsMessageSerializer());
        factory.register(NodeMetricsMessage.TYPE_CODE, NodeMetricsMessage::new, new NodeMetricsMessageSerializer());
        factory.register(NodeFullMetricsMessage.TYPE_CODE, NodeFullMetricsMessage::new, new NodeFullMetricsMessageSerializer());
        factory.register((short)157, PartitionUpdateCountersMessage::new, new PartitionUpdateCountersMessageSerializer());
        factory.register((short)162, GenerateEncryptionKeyRequest::new, new GenerateEncryptionKeyRequestSerializer());
        factory.register((short)163, GenerateEncryptionKeyResponse::new);
        factory.register((short)167, ServiceDeploymentProcessId::new, new ServiceDeploymentProcessIdSerializer());
        factory.register((short)168, ServiceSingleNodeDeploymentResultBatch::new, new ServiceSingleNodeDeploymentResultBatchSerializer());
        factory.register((short)169, ServiceSingleNodeDeploymentResult::new);
        factory.register(GridQueryKillRequest.TYPE_CODE, GridQueryKillRequest::new, new GridQueryKillRequestSerializer());
        factory.register(GridQueryKillResponse.TYPE_CODE, GridQueryKillResponse::new, new GridQueryKillResponseSerializer());
        factory.register(GridIoSecurityAwareMessage.TYPE_CODE, GridIoSecurityAwareMessage::new);
        factory.register(SessionChannelMessage.TYPE_CODE, SessionChannelMessage::new, new SessionChannelMessageSerializer());
        factory.register(SingleNodeMessage.TYPE_CODE, SingleNodeMessage::new);
        factory.register((short)177, TcpInverseConnectionResponseMessage::new, new TcpInverseConnectionResponseMessageSerializer());
        factory.register(SnapshotFilesRequestMessage.TYPE_CODE, SnapshotFilesRequestMessage::new,
            new SnapshotFilesRequestMessageSerializer());
        factory.register(SnapshotFilesFailureMessage.TYPE_CODE, SnapshotFilesFailureMessage::new,
            new SnapshotFilesFailureMessageSerializer());
        factory.register((short)180, AtomicApplicationAttributesAwareRequest::new, new AtomicApplicationAttributesAwareRequestSerializer());
        factory.register((short)181, TransactionAttributesAwareRequest::new, new TransactionAttributesAwareRequestSerializer());

        // Incremental snapshot.
        factory.register(IncrementalSnapshotAwareMessage.TYPE_CODE, IncrementalSnapshotAwareMessage::new,
            new IncrementalSnapshotAwareMessageSerializer());

        // Index statistics.
        factory.register(StatisticsKeyMessage.TYPE_CODE, StatisticsKeyMessage::new);
        factory.register(StatisticsDecimalMessage.TYPE_CODE, StatisticsDecimalMessage::new);
        factory.register(StatisticsObjectData.TYPE_CODE, StatisticsObjectData::new);
        factory.register(StatisticsColumnData.TYPE_CODE, StatisticsColumnData::new);
        factory.register(StatisticsRequest.TYPE_CODE, StatisticsRequest::new);
        factory.register(StatisticsResponse.TYPE_CODE, StatisticsResponse::new);

        factory.register(CachePartitionPartialCountersMap.TYPE_CODE, CachePartitionPartialCountersMap::new,
            new CachePartitionPartialCountersMapSerializer());
        factory.register(IgniteDhtDemandedPartitionsMap.TYPE_CODE, IgniteDhtDemandedPartitionsMap::new,
            new IgniteDhtDemandedPartitionsMapSerializer());
        factory.register(BinaryMetadataVersionInfo.TYPE_CODE, BinaryMetadataVersionInfo::new,
            new BinaryMetadataVersionInfoSerializer());
        factory.register(CachePartitionFullCountersMap.TYPE_CODE, CachePartitionFullCountersMap::new,
            new CachePartitionFullCountersMapSerializer());
        factory.register(IgniteDhtPartitionCountersMap.TYPE_CODE, IgniteDhtPartitionCountersMap::new,
            new IgniteDhtPartitionCountersMapSerializer());
        factory.register(GroupPartitionIdPair.TYPE_CODE, GroupPartitionIdPair::new, new GroupPartitionIdPairSerializer());
        factory.register(PartitionReservationsMap.TYPE_CODE, PartitionReservationsMap::new, new PartitionReservationsMapSerializer());
        factory.register(IgniteDhtPartitionHistorySuppliersMap.TYPE_CODE, IgniteDhtPartitionHistorySuppliersMap::new,
            new IgniteDhtPartitionHistorySuppliersMapSerializer());
        factory.register(PartitionsToReload.TYPE_CODE, PartitionsToReload::new, new PartitionsToReloadSerializer());
        factory.register(CachePartitionsToReloadMap.TYPE_CODE, CachePartitionsToReloadMap::new, new CachePartitionsToReloadMapSerializer());
        factory.register(IgniteDhtPartitionsToReloadMap.TYPE_CODE, IgniteDhtPartitionsToReloadMap::new,
            new IgniteDhtPartitionsToReloadMapSerializer());
        factory.register(IntLongMap.TYPE_CODE, IntLongMap::new, new IntLongMapSerializer());
        factory.register(IndexKeyTypeMessage.TYPE_CODE, IndexKeyTypeMessage::new, new IndexKeyTypeMessageSerializer());

        // [-3..119] [124..129] [-23..-28] [-36..-55] [183..188] - this
        // [120..123] - DR
        // [-44, 0..2, 42, 200..204, 210, 302] - Use in tests.
        // [300..307, 350..352] - CalciteMessageFactory.
        // [400] - Incremental snapshot.
        // [-4..-22, -30..-35, -54..-57] - SQL
        // [2048..2053] - Snapshots
        // [-42..-37] - former hadoop.
        // [64..71] - former IGFS.
    }
}
