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
import org.apache.ignite.internal.ExchangeInfoSerializer;
import org.apache.ignite.internal.GridJobCancelRequest;
import org.apache.ignite.internal.GridJobCancelRequestSerializer;
import org.apache.ignite.internal.GridJobExecuteRequest;
import org.apache.ignite.internal.GridJobExecuteRequestSerializer;
import org.apache.ignite.internal.GridJobExecuteResponse;
import org.apache.ignite.internal.GridJobExecuteResponseSerializer;
import org.apache.ignite.internal.GridJobSiblingsRequest;
import org.apache.ignite.internal.GridJobSiblingsRequestSerializer;
import org.apache.ignite.internal.GridJobSiblingsResponse;
import org.apache.ignite.internal.GridJobSiblingsResponseSerializer;
import org.apache.ignite.internal.GridTaskCancelRequest;
import org.apache.ignite.internal.GridTaskCancelRequestSerializer;
import org.apache.ignite.internal.GridTaskSessionRequest;
import org.apache.ignite.internal.GridTaskSessionRequestSerializer;
import org.apache.ignite.internal.IgniteDiagnosticRequest;
import org.apache.ignite.internal.IgniteDiagnosticRequestSerializer;
import org.apache.ignite.internal.IgniteDiagnosticResponse;
import org.apache.ignite.internal.IgniteDiagnosticResponseSerializer;
import org.apache.ignite.internal.TxEntriesInfo;
import org.apache.ignite.internal.TxEntriesInfoSerializer;
import org.apache.ignite.internal.TxInfo;
import org.apache.ignite.internal.TxInfoSerializer;
import org.apache.ignite.internal.cache.query.index.IndexQueryResultMeta;
import org.apache.ignite.internal.cache.query.index.IndexQueryResultMetaSerializer;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinitionSerializer;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettingsSerializer;
import org.apache.ignite.internal.managers.checkpoint.GridCheckpointRequest;
import org.apache.ignite.internal.managers.checkpoint.GridCheckpointRequestSerializer;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfoBean;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfoBeanSerializer;
import org.apache.ignite.internal.managers.deployment.GridDeploymentRequest;
import org.apache.ignite.internal.managers.deployment.GridDeploymentRequestSerializer;
import org.apache.ignite.internal.managers.deployment.GridDeploymentResponse;
import org.apache.ignite.internal.managers.deployment.GridDeploymentResponseSerializer;
import org.apache.ignite.internal.managers.encryption.GenerateEncryptionKeyRequest;
import org.apache.ignite.internal.managers.encryption.GenerateEncryptionKeyRequestSerializer;
import org.apache.ignite.internal.managers.encryption.GenerateEncryptionKeyResponse;
import org.apache.ignite.internal.managers.encryption.GenerateEncryptionKeyResponseSerializer;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageMessage;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageMessageMarshallableSerializer;
import org.apache.ignite.internal.processors.authentication.UserAuthenticateRequestMessage;
import org.apache.ignite.internal.processors.authentication.UserAuthenticateRequestMessageSerializer;
import org.apache.ignite.internal.processors.authentication.UserAuthenticateResponseMessage;
import org.apache.ignite.internal.processors.authentication.UserAuthenticateResponseMessageSerializer;
import org.apache.ignite.internal.processors.authentication.UserManagementOperationFinishedMessage;
import org.apache.ignite.internal.processors.authentication.UserManagementOperationFinishedMessageSerializer;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicateAdapter;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicateAdapterSerializer;
import org.apache.ignite.internal.processors.cache.CacheEvictionEntry;
import org.apache.ignite.internal.processors.cache.CacheEvictionEntrySerializer;
import org.apache.ignite.internal.processors.cache.CacheInvokeDirectResult;
import org.apache.ignite.internal.processors.cache.CacheInvokeDirectResultSerializer;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfoSerializer;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridCacheReturnSerializer;
import org.apache.ignite.internal.processors.cache.GridChangeGlobalStateMessageResponse;
import org.apache.ignite.internal.processors.cache.GridChangeGlobalStateMessageResponseSerializer;
import org.apache.ignite.internal.processors.cache.WalStateAckMessage;
import org.apache.ignite.internal.processors.cache.WalStateAckMessageSerializer;
import org.apache.ignite.internal.processors.cache.binary.BinaryMetadataVersionInfo;
import org.apache.ignite.internal.processors.cache.binary.BinaryMetadataVersionInfoSerializer;
import org.apache.ignite.internal.processors.cache.binary.MetadataRequestMessage;
import org.apache.ignite.internal.processors.cache.binary.MetadataRequestMessageSerializer;
import org.apache.ignite.internal.processors.cache.binary.MetadataResponseMessage;
import org.apache.ignite.internal.processors.cache.binary.MetadataResponseMessageSerializer;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheTtlUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheTtlUpdateRequestSerializer;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheTxRecoveryRequest;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheTxRecoveryRequestSerializer;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheTxRecoveryResponse;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheTxRecoveryResponseSerializer;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedLockRequestSerializer;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedLockResponse;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedLockResponseSerializer;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxFinishRequestSerializer;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxFinishResponseSerializer;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxPrepareRequestSerializer;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxPrepareResponseSerializer;
import org.apache.ignite.internal.processors.cache.distributed.GridNearUnlockRequest;
import org.apache.ignite.internal.processors.cache.distributed.GridNearUnlockRequestSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtAffinityAssignmentRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtAffinityAssignmentRequestSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtAffinityAssignmentResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtAffinityAssignmentResponseSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLockRequestSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLockResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLockResponseSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequestSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishResponseSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxOnePhaseCommitAckRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxOnePhaseCommitAckRequestSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequestSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponseSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtUnlockRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtUnlockRequestSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.PartitionUpdateCountersMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.PartitionUpdateCountersMessageMarshallableSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.TransactionAttributesAwareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.TransactionAttributesAwareRequestSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.AtomicApplicationAttributesAwareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.AtomicApplicationAttributesAwareRequestSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicDeferredUpdateResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicDeferredUpdateResponseSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicNearResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicNearResponseSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicSingleUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicSingleUpdateRequestSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicUpdateRequestSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicUpdateResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicUpdateResponseSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicCheckUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicCheckUpdateRequestSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicFullUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicFullUpdateRequestSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateFilterRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateFilterRequestSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateInvokeRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateInvokeRequestSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateRequestSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicUpdateResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicUpdateResponseSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.NearCacheUpdates;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.NearCacheUpdatesSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.UpdateErrors;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.UpdateErrorsSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CacheGroupAffinityMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CacheGroupAffinityMessageSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionFullCountersMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionFullCountersMapSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionPartialCountersMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionPartialCountersMapSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtForceKeysRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtForceKeysRequestSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtForceKeysResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtForceKeysResponseSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessageSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeIdSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMapSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMapSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessageSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessageMarshallableSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessageSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleRequestSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GroupPartitionIdPair;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GroupPartitionIdPairSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtDemandedPartitionsMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtDemandedPartitionsMapSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtPartitionHistorySuppliersMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtPartitionHistorySuppliersMapSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtPartitionsToReloadMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtPartitionsToReloadMapSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.latch.LatchAckMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.latch.LatchAckMessageSerializer;
import org.apache.ignite.internal.processors.cache.distributed.near.CacheVersionedValue;
import org.apache.ignite.internal.processors.cache.distributed.near.CacheVersionedValueSerializer;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetRequestSerializer;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetResponseSerializer;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockRequestSerializer;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockResponseSerializer;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetRequestSerializer;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetResponseSerializer;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequestSerializer;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponseSerializer;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequestSerializer;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponseSerializer;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.DataStreamerUpdatesHandlerResult;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.DataStreamerUpdatesHandlerResultSerializer;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IncrementalSnapshotAwareMessage;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IncrementalSnapshotAwareMessageSerializer;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IncrementalSnapshotVerifyResult;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IncrementalSnapshotVerifyResultMarshallableSerializer;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotCheckHandlersResponse;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotCheckHandlersResponseSerializer;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotCheckPartitionHashesResponse;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotCheckPartitionHashesResponseMarshallableSerializer;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotCheckResponse;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotCheckResponseSerializer;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotFilesFailureMessage;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotFilesFailureMessageSerializer;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotFilesRequestMessage;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotFilesRequestMessageSerializer;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotHandlerResult;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotHandlerResultSerializer;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotMetadataResponse;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotMetadataResponseMarshallableSerializer;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotOperationResponse;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotOperationResponseSerializer;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotPartitionsVerifyHandlerResponse;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotPartitionsVerifyHandlerResponseMarshallableSerializer;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotRestoreOperationResponse;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotRestoreOperationResponseMarshallableSerializer;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryRequest;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryRequestSerializer;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryResponse;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryResponseSerializer;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuerySerializer;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryBatchAck;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryBatchAckSerializer;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryEntry;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryEntryMarshallableSerializer;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntrySerializer;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKeySerializer;
import org.apache.ignite.internal.processors.cache.transactions.TxEntryValueHolder;
import org.apache.ignite.internal.processors.cache.transactions.TxEntryValueHolderSerializer;
import org.apache.ignite.internal.processors.cache.transactions.TxLock;
import org.apache.ignite.internal.processors.cache.transactions.TxLockSerializer;
import org.apache.ignite.internal.processors.cache.transactions.TxLocksRequest;
import org.apache.ignite.internal.processors.cache.transactions.TxLocksRequestSerializer;
import org.apache.ignite.internal.processors.cache.transactions.TxLocksResponse;
import org.apache.ignite.internal.processors.cache.transactions.TxLocksResponseSerializer;
import org.apache.ignite.internal.processors.cache.version.GridCacheRawVersionedEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheRawVersionedEntrySerializer;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionEx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionExSerializer;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionSerializer;
import org.apache.ignite.internal.processors.cluster.CacheMetricsMessage;
import org.apache.ignite.internal.processors.cluster.CacheMetricsMessageSerializer;
import org.apache.ignite.internal.processors.cluster.ClusterMetricsUpdateMessage;
import org.apache.ignite.internal.processors.cluster.ClusterMetricsUpdateMessageSerializer;
import org.apache.ignite.internal.processors.cluster.NodeFullMetricsMessage;
import org.apache.ignite.internal.processors.cluster.NodeFullMetricsMessageSerializer;
import org.apache.ignite.internal.processors.cluster.NodeMetricsMessage;
import org.apache.ignite.internal.processors.cluster.NodeMetricsMessageSerializer;
import org.apache.ignite.internal.processors.continuous.ContinuousRoutineStartResultMessage;
import org.apache.ignite.internal.processors.continuous.ContinuousRoutineStartResultMessageSerializer;
import org.apache.ignite.internal.processors.continuous.GridContinuousMessage;
import org.apache.ignite.internal.processors.continuous.GridContinuousMessageSerializer;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerEntry;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerEntrySerializer;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerRequest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerRequestSerializer;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerResponse;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerResponseSerializer;
import org.apache.ignite.internal.processors.marshaller.MissingMappingRequestMessage;
import org.apache.ignite.internal.processors.marshaller.MissingMappingRequestMessageSerializer;
import org.apache.ignite.internal.processors.marshaller.MissingMappingResponseMessage;
import org.apache.ignite.internal.processors.marshaller.MissingMappingResponseMessageSerializer;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryCancelRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryCancelRequestSerializer;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryFailResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryFailResponseSerializer;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryNextPageRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryNextPageRequestSerializer;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryNextPageResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryNextPageResponseSerializer;
import org.apache.ignite.internal.processors.query.messages.GridQueryKillRequest;
import org.apache.ignite.internal.processors.query.messages.GridQueryKillRequestSerializer;
import org.apache.ignite.internal.processors.query.messages.GridQueryKillResponse;
import org.apache.ignite.internal.processors.query.messages.GridQueryKillResponseSerializer;
import org.apache.ignite.internal.processors.query.schema.message.SchemaOperationStatusMessage;
import org.apache.ignite.internal.processors.query.schema.message.SchemaOperationStatusMessageSerializer;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsColumnData;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsColumnDataSerializer;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsDecimalMessage;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsDecimalMessageSerializer;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsKeyMessage;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsKeyMessageSerializer;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsObjectData;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsObjectDataSerializer;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsRequest;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsRequestSerializer;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsResponse;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsResponseSerializer;
import org.apache.ignite.internal.processors.rest.handlers.task.GridTaskResultRequest;
import org.apache.ignite.internal.processors.rest.handlers.task.GridTaskResultRequestSerializer;
import org.apache.ignite.internal.processors.rest.handlers.task.GridTaskResultResponse;
import org.apache.ignite.internal.processors.rest.handlers.task.GridTaskResultResponseSerializer;
import org.apache.ignite.internal.processors.service.ServiceDeploymentProcessId;
import org.apache.ignite.internal.processors.service.ServiceDeploymentProcessIdSerializer;
import org.apache.ignite.internal.processors.service.ServiceSingleNodeDeploymentResult;
import org.apache.ignite.internal.processors.service.ServiceSingleNodeDeploymentResultBatch;
import org.apache.ignite.internal.processors.service.ServiceSingleNodeDeploymentResultBatchSerializer;
import org.apache.ignite.internal.processors.service.ServiceSingleNodeDeploymentResultSerializer;
import org.apache.ignite.internal.util.GridByteArrayList;
import org.apache.ignite.internal.util.GridByteArrayListSerializer;
import org.apache.ignite.internal.util.GridPartitionStateMap;
import org.apache.ignite.internal.util.GridPartitionStateMapSerializer;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.internal.util.distributed.SingleNodeMessageSerializer;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.spi.collision.jobstealing.JobStealingRequest;
import org.apache.ignite.spi.collision.jobstealing.JobStealingRequestSerializer;
import org.apache.ignite.spi.communication.tcp.internal.TcpInverseConnectionResponseMessage;
import org.apache.ignite.spi.communication.tcp.internal.TcpInverseConnectionResponseMessageSerializer;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeMessage;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeMessageSerializer;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeWaitMessage;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeWaitMessageSerializer;
import org.apache.ignite.spi.communication.tcp.messages.NodeIdMessage;
import org.apache.ignite.spi.communication.tcp.messages.NodeIdMessageSerializer;
import org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessage;
import org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessageSerializer;

/**
 * Message factory implementation.
 */
public class GridIoMessageFactory implements MessageFactoryProvider {
    /** Custom data marshaller. */
    private final Marshaller marsh;

    /** Class loader for the custom data marshalling. */
    private final ClassLoader clsLdr;

    /**
     * @param marsh Custom data marshaller.
     * @param clsLdr Class loader for the custom data marshalling.
     */
    public GridIoMessageFactory(Marshaller marsh, ClassLoader clsLdr) {
        this.marsh = marsh;
        this.clsLdr = clsLdr;
    }

    /** {@inheritDoc} */
    @Override public void registerAll(MessageFactory factory) {
        // -54 is reserved for SQL.
        // We don't use the code‑generated serializer for CompressedMessage - serialization is highly customized.
        factory.register(-67, CompressedMessage::new);
        factory.register(-66, ErrorMessage::new, new ErrorMessageMarshallableSerializer(marsh, clsLdr));
        factory.register(-65, TxInfo::new, new TxInfoSerializer());
        factory.register(-64, TxEntriesInfo::new, new TxEntriesInfoSerializer());
        factory.register(-63, ExchangeInfo::new, new ExchangeInfoSerializer());
        factory.register(-62, IgniteDiagnosticResponse::new, new IgniteDiagnosticResponseSerializer());
        factory.register(-61, IgniteDiagnosticRequest::new, new IgniteDiagnosticRequestSerializer());
        factory.register(-53, SchemaOperationStatusMessage::new, new SchemaOperationStatusMessageSerializer());
        factory.register(-51, NearCacheUpdates::new, new NearCacheUpdatesSerializer());
        factory.register(-50, GridNearAtomicCheckUpdateRequest::new, new GridNearAtomicCheckUpdateRequestSerializer());
        factory.register(-49, UpdateErrors::new, new UpdateErrorsSerializer());
        factory.register(-48, GridDhtAtomicNearResponse::new, new GridDhtAtomicNearResponseSerializer());
        factory.register(-45, GridChangeGlobalStateMessageResponse::new, new GridChangeGlobalStateMessageResponseSerializer());
        factory.register((short)-43, IgniteIoTestMessage::new);
        factory.register(-36, GridDhtAtomicSingleUpdateRequest::new, new GridDhtAtomicSingleUpdateRequestSerializer());
        factory.register(-27, GridDhtTxOnePhaseCommitAckRequest::new, new GridDhtTxOnePhaseCommitAckRequestSerializer());
        factory.register(-25, TxLock::new, new TxLockSerializer());
        factory.register(-24, TxLocksRequest::new, new TxLocksRequestSerializer());
        factory.register(-23, TxLocksResponse::new, new TxLocksResponseSerializer());
        factory.register(-1, NodeIdMessage::new, new NodeIdMessageSerializer());
        factory.register(-2, RecoveryLastReceivedMessage::new,
            new RecoveryLastReceivedMessageSerializer());
        factory.register(-3, HandshakeMessage::new, new HandshakeMessageSerializer());
        factory.register(-28, HandshakeWaitMessage::new, new HandshakeWaitMessageSerializer());
        factory.register(0, GridJobCancelRequest::new, new GridJobCancelRequestSerializer());
        factory.register(1, GridJobExecuteRequest::new, new GridJobExecuteRequestSerializer());
        factory.register(2, GridJobExecuteResponse::new, new GridJobExecuteResponseSerializer());
        factory.register(3, GridJobSiblingsRequest::new, new GridJobSiblingsRequestSerializer());
        factory.register(4, GridJobSiblingsResponse::new, new GridJobSiblingsResponseSerializer());
        factory.register(5, GridTaskCancelRequest::new, new GridTaskCancelRequestSerializer());
        factory.register(6, GridTaskSessionRequest::new, new GridTaskSessionRequestSerializer());
        factory.register(7, GridCheckpointRequest::new, new GridCheckpointRequestSerializer());
        factory.register(8, GridIoMessage::new, new GridIoMessageMarshallableSerializer(marsh, clsLdr));
        factory.register(9, GridIoUserMessage::new, new GridIoUserMessageSerializer());
        factory.register(10, GridDeploymentInfoBean::new, new GridDeploymentInfoBeanSerializer());
        factory.register(11, GridDeploymentRequest::new, new GridDeploymentRequestSerializer());
        factory.register(12, GridDeploymentResponse::new, new GridDeploymentResponseSerializer());
        factory.register(13, GridEventStorageMessage::new, new GridEventStorageMessageMarshallableSerializer(marsh, clsLdr));
        factory.register(16, GridCacheTxRecoveryRequest::new, new GridCacheTxRecoveryRequestSerializer());
        factory.register(17, GridCacheTxRecoveryResponse::new, new GridCacheTxRecoveryResponseSerializer());
        factory.register(18, IndexQueryResultMeta::new, new IndexQueryResultMetaSerializer());
        factory.register(19, IndexKeyTypeSettings::new, new IndexKeyTypeSettingsSerializer());
        factory.register(20, GridCacheTtlUpdateRequest::new, new GridCacheTtlUpdateRequestSerializer());
        factory.register(21, GridDistributedLockRequest::new, new GridDistributedLockRequestSerializer());
        factory.register(22, GridDistributedLockResponse::new, new GridDistributedLockResponseSerializer());
        factory.register(23, GridDistributedTxFinishRequest::new, new GridDistributedTxFinishRequestSerializer());
        factory.register(24, GridDistributedTxFinishResponse::new, new GridDistributedTxFinishResponseSerializer());
        factory.register(25, GridDistributedTxPrepareRequest::new, new GridDistributedTxPrepareRequestSerializer());
        factory.register(26, GridDistributedTxPrepareResponse::new, new GridDistributedTxPrepareResponseSerializer());
        // Type 27 is former GridDistributedUnlockRequest
        factory.register(28, GridDhtAffinityAssignmentRequest::new, new GridDhtAffinityAssignmentRequestSerializer());
        factory.register(29, GridDhtAffinityAssignmentResponse::new, new GridDhtAffinityAssignmentResponseSerializer());
        factory.register(30, GridDhtLockRequest::new, new GridDhtLockRequestSerializer());
        factory.register(31, GridDhtLockResponse::new, new GridDhtLockResponseSerializer());
        factory.register(32, GridDhtTxFinishRequest::new, new GridDhtTxFinishRequestSerializer());
        factory.register(33, GridDhtTxFinishResponse::new, new GridDhtTxFinishResponseSerializer());
        factory.register(34, GridDhtTxPrepareRequest::new, new GridDhtTxPrepareRequestSerializer());
        factory.register(35, GridDhtTxPrepareResponse::new, new GridDhtTxPrepareResponseSerializer());
        factory.register(36, GridDhtUnlockRequest::new, new GridDhtUnlockRequestSerializer());
        factory.register(37, GridDhtAtomicDeferredUpdateResponse::new, new GridDhtAtomicDeferredUpdateResponseSerializer());
        factory.register(38, GridDhtAtomicUpdateRequest::new, new GridDhtAtomicUpdateRequestSerializer());
        factory.register(39, GridDhtAtomicUpdateResponse::new, new GridDhtAtomicUpdateResponseSerializer());
        factory.register(40, GridNearAtomicFullUpdateRequest::new, new GridNearAtomicFullUpdateRequestSerializer());
        factory.register(41, GridNearAtomicUpdateResponse::new, new GridNearAtomicUpdateResponseSerializer());
        factory.register(42, GridDhtForceKeysRequest::new, new GridDhtForceKeysRequestSerializer());
        factory.register(43, GridDhtForceKeysResponse::new, new GridDhtForceKeysResponseSerializer());
        factory.register(45, GridDhtPartitionDemandMessage::new, new GridDhtPartitionDemandMessageSerializer());
        factory.register(46, GridDhtPartitionsFullMessage::new, new GridDhtPartitionsFullMessageMarshallableSerializer(marsh, clsLdr));
        factory.register(47, GridDhtPartitionsSingleMessage::new, new GridDhtPartitionsSingleMessageSerializer());
        factory.register(48, GridDhtPartitionsSingleRequest::new, new GridDhtPartitionsSingleRequestSerializer());
        factory.register(49, GridNearGetRequest::new, new GridNearGetRequestSerializer());
        factory.register(50, GridNearGetResponse::new, new GridNearGetResponseSerializer());
        factory.register(51, GridNearLockRequest::new, new GridNearLockRequestSerializer());
        factory.register(52, GridNearLockResponse::new, new GridNearLockResponseSerializer());
        factory.register(53, GridNearTxFinishRequest::new, new GridNearTxFinishRequestSerializer());
        factory.register(54, GridNearTxFinishResponse::new, new GridNearTxFinishResponseSerializer());
        factory.register(55, GridNearTxPrepareRequest::new, new GridNearTxPrepareRequestSerializer());
        factory.register(56, GridNearTxPrepareResponse::new, new GridNearTxPrepareResponseSerializer());
        factory.register(57, GridNearUnlockRequest::new, new GridNearUnlockRequestSerializer());
        factory.register(58, GridCacheQueryRequest::new, new GridCacheQueryRequestSerializer());
        factory.register(59, GridCacheQueryResponse::new, new GridCacheQueryResponseSerializer());
        factory.register(61, GridContinuousMessage::new, new GridContinuousMessageSerializer());
        factory.register(62, DataStreamerRequest::new, new DataStreamerRequestSerializer());
        factory.register(63, DataStreamerResponse::new, new DataStreamerResponseSerializer());
        factory.register(76, GridTaskResultRequest::new, new GridTaskResultRequestSerializer());
        factory.register(77, GridTaskResultResponse::new, new GridTaskResultResponseSerializer());
        factory.register(78, MissingMappingRequestMessage::new, new MissingMappingRequestMessageSerializer());
        factory.register(79, MissingMappingResponseMessage::new, new MissingMappingResponseMessageSerializer());
        factory.register(80, MetadataRequestMessage::new, new MetadataRequestMessageSerializer());
        factory.register(81, MetadataResponseMessage::new, new MetadataResponseMessageSerializer());
        factory.register(82, JobStealingRequest::new, new JobStealingRequestSerializer());
        factory.register(84, GridByteArrayList::new, new GridByteArrayListSerializer());
        factory.register(86, GridCacheVersion::new, new GridCacheVersionSerializer());
        factory.register(87, GridDhtPartitionExchangeId::new, new GridDhtPartitionExchangeIdSerializer());
        factory.register(88, GridCacheReturn::new, new GridCacheReturnSerializer());
        factory.register(91, GridCacheEntryInfo::new, new GridCacheEntryInfoSerializer());
        factory.register(93, CacheInvokeDirectResult::new, new CacheInvokeDirectResultSerializer());
        factory.register(94, IgniteTxKey::new, new IgniteTxKeySerializer());
        factory.register(95, DataStreamerEntry::new, new DataStreamerEntrySerializer());
        factory.register(96, CacheContinuousQueryEntry::new, new CacheContinuousQueryEntryMarshallableSerializer(marsh, clsLdr));
        factory.register(97, CacheEvictionEntry::new, new CacheEvictionEntrySerializer());
        factory.register(98, CacheEntryPredicateAdapter::new, new CacheEntryPredicateAdapterSerializer());
        factory.register(100, IgniteTxEntry::new, new IgniteTxEntrySerializer());
        factory.register(101, TxEntryValueHolder::new, new TxEntryValueHolderSerializer());
        factory.register(102, CacheVersionedValue::new, new CacheVersionedValueSerializer());
        factory.register(103, GridCacheRawVersionedEntry::new, new GridCacheRawVersionedEntrySerializer());
        factory.register(104, GridCacheVersionEx::new, new GridCacheVersionExSerializer());
        factory.register(106, GridQueryCancelRequest::new, new GridQueryCancelRequestSerializer());
        factory.register(107, GridQueryFailResponse::new, new GridQueryFailResponseSerializer());
        factory.register(108, GridQueryNextPageRequest::new, new GridQueryNextPageRequestSerializer());
        factory.register(109, GridQueryNextPageResponse::new, new GridQueryNextPageResponseSerializer());
        factory.register(112, GridCacheSqlQuery::new, new GridCacheSqlQuerySerializer());
        factory.register(113, IndexKeyDefinition::new, new IndexKeyDefinitionSerializer());
        factory.register(114, GridDhtPartitionSupplyMessage::new, new GridDhtPartitionSupplyMessageSerializer());
        factory.register(116, GridNearSingleGetRequest::new, new GridNearSingleGetRequestSerializer());
        factory.register(117, GridNearSingleGetResponse::new, new GridNearSingleGetResponseSerializer());
        factory.register(118, CacheContinuousQueryBatchAck::new, new CacheContinuousQueryBatchAckSerializer());

        // [120..123] - DR
        factory.register(125, GridNearAtomicSingleUpdateRequest::new, new GridNearAtomicSingleUpdateRequestSerializer());
        factory.register(126, GridNearAtomicSingleUpdateInvokeRequest::new, new GridNearAtomicSingleUpdateInvokeRequestSerializer());
        factory.register(127, GridNearAtomicSingleUpdateFilterRequest::new, new GridNearAtomicSingleUpdateFilterRequestSerializer());
        factory.register(128, CacheGroupAffinityMessage::new, new CacheGroupAffinityMessageSerializer());
        factory.register(129, WalStateAckMessage::new, new WalStateAckMessageSerializer());
        factory.register(130, UserManagementOperationFinishedMessage::new, new UserManagementOperationFinishedMessageSerializer());
        factory.register(131, UserAuthenticateRequestMessage::new, new UserAuthenticateRequestMessageSerializer());
        factory.register(132, UserAuthenticateResponseMessage::new, new UserAuthenticateResponseMessageSerializer());
        factory.register(133, ClusterMetricsUpdateMessage::new,
            new ClusterMetricsUpdateMessageSerializer());
        factory.register(134, ContinuousRoutineStartResultMessage::new, new ContinuousRoutineStartResultMessageSerializer());
        factory.register(135, LatchAckMessage::new, new LatchAckMessageSerializer());
        factory.register(136, CacheMetricsMessage::new, new CacheMetricsMessageSerializer());
        factory.register(137, NodeMetricsMessage::new, new NodeMetricsMessageSerializer());
        factory.register(138, NodeFullMetricsMessage::new, new NodeFullMetricsMessageSerializer());
        factory.register(157, PartitionUpdateCountersMessage::new, new PartitionUpdateCountersMessageMarshallableSerializer(marsh, clsLdr));
        factory.register(162, GenerateEncryptionKeyRequest::new, new GenerateEncryptionKeyRequestSerializer());
        factory.register(163, GenerateEncryptionKeyResponse::new, new GenerateEncryptionKeyResponseSerializer());
        factory.register(167, ServiceDeploymentProcessId::new, new ServiceDeploymentProcessIdSerializer());
        factory.register(168, ServiceSingleNodeDeploymentResultBatch::new, new ServiceSingleNodeDeploymentResultBatchSerializer());
        factory.register(169, ServiceSingleNodeDeploymentResult::new, new ServiceSingleNodeDeploymentResultSerializer());
        factory.register(170, GridQueryKillRequest::new, new GridQueryKillRequestSerializer());
        factory.register(171, GridQueryKillResponse::new, new GridQueryKillResponseSerializer());
        factory.register(174, GridIoSecurityAwareMessage::new,
            new GridIoSecurityAwareMessageMarshallableSerializer(marsh, clsLdr));
        factory.register(175, SessionChannelMessage::new, new SessionChannelMessageSerializer());
        factory.register(176, SingleNodeMessage::new, new SingleNodeMessageSerializer());
        factory.register(177, TcpInverseConnectionResponseMessage::new, new TcpInverseConnectionResponseMessageSerializer());
        factory.register(178, SnapshotFilesRequestMessage::new,
            new SnapshotFilesRequestMessageSerializer());
        factory.register(179, SnapshotFilesFailureMessage::new,
            new SnapshotFilesFailureMessageSerializer());
        factory.register(180, AtomicApplicationAttributesAwareRequest::new, new AtomicApplicationAttributesAwareRequestSerializer());
        factory.register(181, TransactionAttributesAwareRequest::new, new TransactionAttributesAwareRequestSerializer());

        // Incremental snapshot.
        factory.register(182, IncrementalSnapshotAwareMessage::new,
            new IncrementalSnapshotAwareMessageSerializer());

        // Index statistics.
        factory.register(183, StatisticsKeyMessage::new, new StatisticsKeyMessageSerializer());
        factory.register(184, StatisticsDecimalMessage::new, new StatisticsDecimalMessageSerializer());
        factory.register(185, StatisticsObjectData::new, new StatisticsObjectDataSerializer());
        factory.register(186, StatisticsColumnData::new, new StatisticsColumnDataSerializer());
        factory.register(187, StatisticsRequest::new, new StatisticsRequestSerializer());
        factory.register(188, StatisticsResponse::new, new StatisticsResponseSerializer());

        factory.register(500, CachePartitionPartialCountersMap::new,
            new CachePartitionPartialCountersMapSerializer());
        factory.register(501, IgniteDhtDemandedPartitionsMap::new,
            new IgniteDhtDemandedPartitionsMapSerializer());
        factory.register(505, BinaryMetadataVersionInfo::new,
            new BinaryMetadataVersionInfoSerializer());
        factory.register(506, CachePartitionFullCountersMap::new,
            new CachePartitionFullCountersMapSerializer());
        factory.register(508, GroupPartitionIdPair::new, new GroupPartitionIdPairSerializer());
        factory.register(510, IgniteDhtPartitionHistorySuppliersMap::new,
            new IgniteDhtPartitionHistorySuppliersMapSerializer());
        factory.register(513, IgniteDhtPartitionsToReloadMap::new,
            new IgniteDhtPartitionsToReloadMapSerializer());
        factory.register(517, GridPartitionStateMap::new, new GridPartitionStateMapSerializer());
        factory.register(518, GridDhtPartitionMap::new, new GridDhtPartitionMapSerializer());
        factory.register(519, GridDhtPartitionFullMap::new, new GridDhtPartitionFullMapSerializer());
        factory.register(520, SnapshotOperationResponse::new, new SnapshotOperationResponseSerializer());
        factory.register(521, SnapshotHandlerResult::new, new SnapshotHandlerResultSerializer());
        factory.register(522, DataStreamerUpdatesHandlerResult::new, new DataStreamerUpdatesHandlerResultSerializer());
        factory.register(523, SnapshotCheckResponse::new, new SnapshotCheckResponseSerializer());
        factory.register(524, IncrementalSnapshotVerifyResult::new,
            new IncrementalSnapshotVerifyResultMarshallableSerializer(marsh, clsLdr));
        factory.register(525, SnapshotRestoreOperationResponse::new,
            new SnapshotRestoreOperationResponseMarshallableSerializer(marsh, clsLdr));
        factory.register(526, SnapshotMetadataResponse::new,
            new SnapshotMetadataResponseMarshallableSerializer(marsh, clsLdr));
        factory.register(527, SnapshotCheckPartitionHashesResponse::new,
            new SnapshotCheckPartitionHashesResponseMarshallableSerializer(marsh, clsLdr));
        factory.register(528, SnapshotCheckHandlersResponse::new, new SnapshotCheckHandlersResponseSerializer());
        factory.register(530, SnapshotPartitionsVerifyHandlerResponse::new,
            new SnapshotPartitionsVerifyHandlerResponseMarshallableSerializer(marsh, clsLdr));

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
