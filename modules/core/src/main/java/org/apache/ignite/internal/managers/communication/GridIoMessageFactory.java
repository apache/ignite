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
import org.apache.ignite.internal.processors.cache.CacheEntryPredicateAdapterMarshallableSerializer;
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
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtPartitionCountersMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtPartitionCountersMapSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtPartitionHistorySuppliersMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtPartitionHistorySuppliersMapSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtPartitionsToReloadMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtPartitionsToReloadMapSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IntLongMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IntLongMapSerializer;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionReservationsMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionReservationsMapSerializer;
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
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotCheckHandlersNodeResponse;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotCheckHandlersNodeResponseSerializer;
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
import org.apache.ignite.internal.processors.cache.transactions.TxLockList;
import org.apache.ignite.internal.processors.cache.transactions.TxLockListSerializer;
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
        factory.register(CompressedMessage.TYPE_CODE, CompressedMessage::new);
        factory.register(ErrorMessage::new, new ErrorMessageMarshallableSerializer(marsh, clsLdr));
        factory.register(TxInfo::new, new TxInfoSerializer());
        factory.register(TxEntriesInfo::new, new TxEntriesInfoSerializer());
        factory.register(ExchangeInfo::new, new ExchangeInfoSerializer());
        factory.register(IgniteDiagnosticResponse::new, new IgniteDiagnosticResponseSerializer());
        factory.register(IgniteDiagnosticRequest::new, new IgniteDiagnosticRequestSerializer());
        factory.register(SchemaOperationStatusMessage::new, new SchemaOperationStatusMessageSerializer());
        factory.register(NearCacheUpdates::new, new NearCacheUpdatesSerializer());
        factory.register(GridNearAtomicCheckUpdateRequest::new, new GridNearAtomicCheckUpdateRequestSerializer());
        factory.register(UpdateErrors::new, new UpdateErrorsSerializer());
        factory.register(GridDhtAtomicNearResponse::new, new GridDhtAtomicNearResponseSerializer());
        factory.register(GridChangeGlobalStateMessageResponse::new, new GridChangeGlobalStateMessageResponseSerializer());
        factory.register((short)-43, IgniteIoTestMessage::new);
        factory.register(GridDhtAtomicSingleUpdateRequest::new, new GridDhtAtomicSingleUpdateRequestSerializer());
        factory.register(GridDhtTxOnePhaseCommitAckRequest::new, new GridDhtTxOnePhaseCommitAckRequestSerializer());
        factory.register(TxLockList::new, new TxLockListSerializer());
        factory.register(TxLock::new, new TxLockSerializer());
        factory.register(TxLocksRequest::new, new TxLocksRequestSerializer());
        factory.register(TxLocksResponse::new, new TxLocksResponseSerializer());
        factory.register(NodeIdMessage::new, new NodeIdMessageSerializer());
        factory.register(RecoveryLastReceivedMessage::new,
            new RecoveryLastReceivedMessageSerializer());
        factory.register(HandshakeMessage::new, new HandshakeMessageSerializer());
        factory.register(HandshakeWaitMessage::new, new HandshakeWaitMessageSerializer());
        factory.register(GridJobCancelRequest::new, new GridJobCancelRequestSerializer());
        factory.register(GridJobExecuteRequest::new, new GridJobExecuteRequestSerializer());
        factory.register(GridJobExecuteResponse::new, new GridJobExecuteResponseSerializer());
        factory.register(GridJobSiblingsRequest::new, new GridJobSiblingsRequestSerializer());
        factory.register(GridJobSiblingsResponse::new, new GridJobSiblingsResponseSerializer());
        factory.register(GridTaskCancelRequest::new, new GridTaskCancelRequestSerializer());
        factory.register(GridTaskSessionRequest::new, new GridTaskSessionRequestSerializer());
        factory.register(GridCheckpointRequest::new, new GridCheckpointRequestSerializer());
        factory.register(GridIoMessage::new, new GridIoMessageMarshallableSerializer(marsh, clsLdr));
        factory.register(GridIoUserMessage::new, new GridIoUserMessageSerializer());
        factory.register(GridDeploymentInfoBean::new, new GridDeploymentInfoBeanSerializer());
        factory.register(GridDeploymentRequest::new, new GridDeploymentRequestSerializer());
        factory.register(GridDeploymentResponse::new, new GridDeploymentResponseSerializer());
        factory.register(GridEventStorageMessage::new, new GridEventStorageMessageMarshallableSerializer(marsh, clsLdr));
        factory.register(GridCacheTxRecoveryRequest::new, new GridCacheTxRecoveryRequestSerializer());
        factory.register(GridCacheTxRecoveryResponse::new, new GridCacheTxRecoveryResponseSerializer());
        factory.register(IndexQueryResultMeta::new, new IndexQueryResultMetaSerializer());
        factory.register(IndexKeyTypeSettings::new, new IndexKeyTypeSettingsSerializer());
        factory.register(GridCacheTtlUpdateRequest::new, new GridCacheTtlUpdateRequestSerializer());
        factory.register(GridDistributedLockRequest::new, new GridDistributedLockRequestSerializer());
        factory.register(GridDistributedLockResponse::new, new GridDistributedLockResponseSerializer());
        factory.register(GridDistributedTxFinishRequest::new, new GridDistributedTxFinishRequestSerializer());
        factory.register(GridDistributedTxFinishResponse::new, new GridDistributedTxFinishResponseSerializer());
        factory.register(GridDistributedTxPrepareRequest::new, new GridDistributedTxPrepareRequestSerializer());
        factory.register(GridDistributedTxPrepareResponse::new, new GridDistributedTxPrepareResponseSerializer());
        // Type 27 is former GridDistributedUnlockRequest
        factory.register(GridDhtAffinityAssignmentRequest::new, new GridDhtAffinityAssignmentRequestSerializer());
        factory.register(GridDhtAffinityAssignmentResponse::new, new GridDhtAffinityAssignmentResponseSerializer());
        factory.register(GridDhtLockRequest::new, new GridDhtLockRequestSerializer());
        factory.register(GridDhtLockResponse::new, new GridDhtLockResponseSerializer());
        factory.register(GridDhtTxFinishRequest::new, new GridDhtTxFinishRequestSerializer());
        factory.register(GridDhtTxFinishResponse::new, new GridDhtTxFinishResponseSerializer());
        factory.register(GridDhtTxPrepareRequest::new, new GridDhtTxPrepareRequestSerializer());
        factory.register(GridDhtTxPrepareResponse::new, new GridDhtTxPrepareResponseSerializer());
        factory.register(GridDhtUnlockRequest::new, new GridDhtUnlockRequestSerializer());
        factory.register(GridDhtAtomicDeferredUpdateResponse::new, new GridDhtAtomicDeferredUpdateResponseSerializer());
        factory.register(GridDhtAtomicUpdateRequest::new, new GridDhtAtomicUpdateRequestSerializer());
        factory.register(GridDhtAtomicUpdateResponse::new, new GridDhtAtomicUpdateResponseSerializer());
        factory.register(GridNearAtomicFullUpdateRequest::new, new GridNearAtomicFullUpdateRequestSerializer());
        factory.register(GridNearAtomicUpdateResponse::new, new GridNearAtomicUpdateResponseSerializer());
        factory.register(GridDhtForceKeysRequest::new, new GridDhtForceKeysRequestSerializer());
        factory.register(GridDhtForceKeysResponse::new, new GridDhtForceKeysResponseSerializer());
        factory.register(GridDhtPartitionDemandMessage::new, new GridDhtPartitionDemandMessageSerializer());
        factory.register(GridDhtPartitionsFullMessage::new, new GridDhtPartitionsFullMessageMarshallableSerializer(marsh, clsLdr));
        factory.register(GridDhtPartitionsSingleMessage::new, new GridDhtPartitionsSingleMessageSerializer());
        factory.register(GridDhtPartitionsSingleRequest::new, new GridDhtPartitionsSingleRequestSerializer());
        factory.register(GridNearGetRequest::new, new GridNearGetRequestSerializer());
        factory.register(GridNearGetResponse::new, new GridNearGetResponseSerializer());
        factory.register(GridNearLockRequest::new, new GridNearLockRequestSerializer());
        factory.register(GridNearLockResponse::new, new GridNearLockResponseSerializer());
        factory.register(GridNearTxFinishRequest::new, new GridNearTxFinishRequestSerializer());
        factory.register(GridNearTxFinishResponse::new, new GridNearTxFinishResponseSerializer());
        factory.register(GridNearTxPrepareRequest::new, new GridNearTxPrepareRequestSerializer());
        factory.register(GridNearTxPrepareResponse::new, new GridNearTxPrepareResponseSerializer());
        factory.register(GridNearUnlockRequest::new, new GridNearUnlockRequestSerializer());
        factory.register(GridCacheQueryRequest::new, new GridCacheQueryRequestSerializer());
        factory.register(GridCacheQueryResponse::new, new GridCacheQueryResponseSerializer());
        factory.register(GridContinuousMessage::new, new GridContinuousMessageSerializer());
        factory.register(DataStreamerRequest::new, new DataStreamerRequestSerializer());
        factory.register(DataStreamerResponse::new, new DataStreamerResponseSerializer());
        factory.register(GridTaskResultRequest::new, new GridTaskResultRequestSerializer());
        factory.register(GridTaskResultResponse::new, new GridTaskResultResponseSerializer());
        factory.register(MissingMappingRequestMessage::new, new MissingMappingRequestMessageSerializer());
        factory.register(MissingMappingResponseMessage::new, new MissingMappingResponseMessageSerializer());
        factory.register(MetadataRequestMessage::new, new MetadataRequestMessageSerializer());
        factory.register(MetadataResponseMessage::new, new MetadataResponseMessageSerializer());
        factory.register(JobStealingRequest::new, new JobStealingRequestSerializer());
        factory.register(GridByteArrayList::new, new GridByteArrayListSerializer());
        factory.register(GridCacheVersion::new, new GridCacheVersionSerializer());
        factory.register(GridDhtPartitionExchangeId::new, new GridDhtPartitionExchangeIdSerializer());
        factory.register(GridCacheReturn::new, new GridCacheReturnSerializer());
        factory.register(GridCacheEntryInfo::new, new GridCacheEntryInfoSerializer());
        factory.register(CacheInvokeDirectResult::new, new CacheInvokeDirectResultSerializer());
        factory.register(IgniteTxKey::new, new IgniteTxKeySerializer());
        factory.register(DataStreamerEntry::new, new DataStreamerEntrySerializer());
        factory.register(CacheContinuousQueryEntry::new, new CacheContinuousQueryEntryMarshallableSerializer(marsh, clsLdr));
        factory.register(CacheEvictionEntry::new, new CacheEvictionEntrySerializer());
        factory.register(CacheEntryPredicateAdapter::new, new CacheEntryPredicateAdapterMarshallableSerializer(marsh, clsLdr));
        factory.register(IgniteTxEntry::new, new IgniteTxEntrySerializer());
        factory.register(TxEntryValueHolder::new, new TxEntryValueHolderSerializer());
        factory.register(CacheVersionedValue::new, new CacheVersionedValueSerializer());
        factory.register(GridCacheRawVersionedEntry::new, new GridCacheRawVersionedEntrySerializer());
        factory.register(GridCacheVersionEx::new, new GridCacheVersionExSerializer());
        factory.register(GridQueryCancelRequest::new, new GridQueryCancelRequestSerializer());
        factory.register(GridQueryFailResponse::new, new GridQueryFailResponseSerializer());
        factory.register(GridQueryNextPageRequest::new, new GridQueryNextPageRequestSerializer());
        factory.register(GridQueryNextPageResponse::new, new GridQueryNextPageResponseSerializer());
        factory.register(GridCacheSqlQuery::new, new GridCacheSqlQuerySerializer());
        factory.register(IndexKeyDefinition::new, new IndexKeyDefinitionSerializer());
        factory.register(GridDhtPartitionSupplyMessage::new, new GridDhtPartitionSupplyMessageSerializer());
        factory.register(GridNearSingleGetRequest::new, new GridNearSingleGetRequestSerializer());
        factory.register(GridNearSingleGetResponse::new, new GridNearSingleGetResponseSerializer());
        factory.register(CacheContinuousQueryBatchAck::new, new CacheContinuousQueryBatchAckSerializer());

        // [120..123] - DR
        factory.register(GridNearAtomicSingleUpdateRequest::new, new GridNearAtomicSingleUpdateRequestSerializer());
        factory.register(GridNearAtomicSingleUpdateInvokeRequest::new, new GridNearAtomicSingleUpdateInvokeRequestSerializer());
        factory.register(GridNearAtomicSingleUpdateFilterRequest::new, new GridNearAtomicSingleUpdateFilterRequestSerializer());
        factory.register(CacheGroupAffinityMessage::new, new CacheGroupAffinityMessageSerializer());
        factory.register(WalStateAckMessage::new, new WalStateAckMessageSerializer());
        factory.register(UserManagementOperationFinishedMessage::new, new UserManagementOperationFinishedMessageSerializer());
        factory.register(UserAuthenticateRequestMessage::new, new UserAuthenticateRequestMessageSerializer());
        factory.register(UserAuthenticateResponseMessage::new, new UserAuthenticateResponseMessageSerializer());
        factory.register(ClusterMetricsUpdateMessage::new,
            new ClusterMetricsUpdateMessageSerializer());
        factory.register(ContinuousRoutineStartResultMessage::new, new ContinuousRoutineStartResultMessageSerializer());
        factory.register(LatchAckMessage::new, new LatchAckMessageSerializer());
        factory.register(CacheMetricsMessage::new, new CacheMetricsMessageSerializer());
        factory.register(NodeMetricsMessage::new, new NodeMetricsMessageSerializer());
        factory.register(NodeFullMetricsMessage::new, new NodeFullMetricsMessageSerializer());
        factory.register(PartitionUpdateCountersMessage::new, new PartitionUpdateCountersMessageMarshallableSerializer(marsh, clsLdr));
        factory.register(GenerateEncryptionKeyRequest::new, new GenerateEncryptionKeyRequestSerializer());
        factory.register(GenerateEncryptionKeyResponse::new, new GenerateEncryptionKeyResponseSerializer());
        factory.register(ServiceDeploymentProcessId::new, new ServiceDeploymentProcessIdSerializer());
        factory.register(ServiceSingleNodeDeploymentResultBatch::new, new ServiceSingleNodeDeploymentResultBatchSerializer());
        factory.register(ServiceSingleNodeDeploymentResult::new, new ServiceSingleNodeDeploymentResultSerializer());
        factory.register(GridQueryKillRequest::new, new GridQueryKillRequestSerializer());
        factory.register(GridQueryKillResponse::new, new GridQueryKillResponseSerializer());
        factory.register(GridIoSecurityAwareMessage::new,
            new GridIoSecurityAwareMessageMarshallableSerializer(marsh, clsLdr));
        factory.register(SessionChannelMessage::new, new SessionChannelMessageSerializer());
        factory.register(SingleNodeMessage::new, new SingleNodeMessageSerializer());
        factory.register(TcpInverseConnectionResponseMessage::new, new TcpInverseConnectionResponseMessageSerializer());
        factory.register(SnapshotFilesRequestMessage::new,
            new SnapshotFilesRequestMessageSerializer());
        factory.register(SnapshotFilesFailureMessage::new,
            new SnapshotFilesFailureMessageSerializer());
        factory.register(AtomicApplicationAttributesAwareRequest::new, new AtomicApplicationAttributesAwareRequestSerializer());
        factory.register(TransactionAttributesAwareRequest::new, new TransactionAttributesAwareRequestSerializer());

        // Incremental snapshot.
        factory.register(IncrementalSnapshotAwareMessage::new,
            new IncrementalSnapshotAwareMessageSerializer());

        // Index statistics.
        factory.register(StatisticsKeyMessage::new, new StatisticsKeyMessageSerializer());
        factory.register(StatisticsDecimalMessage::new, new StatisticsDecimalMessageSerializer());
        factory.register(StatisticsObjectData::new, new StatisticsObjectDataSerializer());
        factory.register(StatisticsColumnData::new, new StatisticsColumnDataSerializer());
        factory.register(StatisticsRequest::new, new StatisticsRequestSerializer());
        factory.register(StatisticsResponse::new, new StatisticsResponseSerializer());

        factory.register(CachePartitionPartialCountersMap::new,
            new CachePartitionPartialCountersMapSerializer());
        factory.register(IgniteDhtDemandedPartitionsMap::new,
            new IgniteDhtDemandedPartitionsMapSerializer());
        factory.register(BinaryMetadataVersionInfo::new,
            new BinaryMetadataVersionInfoSerializer());
        factory.register(CachePartitionFullCountersMap::new,
            new CachePartitionFullCountersMapSerializer());
        factory.register(IgniteDhtPartitionCountersMap::new,
            new IgniteDhtPartitionCountersMapSerializer());
        factory.register(GroupPartitionIdPair::new, new GroupPartitionIdPairSerializer());
        factory.register(PartitionReservationsMap::new, new PartitionReservationsMapSerializer());
        factory.register(IgniteDhtPartitionHistorySuppliersMap::new,
            new IgniteDhtPartitionHistorySuppliersMapSerializer());
        factory.register(IgniteDhtPartitionsToReloadMap::new,
            new IgniteDhtPartitionsToReloadMapSerializer());
        factory.register(IntLongMap::new, new IntLongMapSerializer());
        factory.register(GridPartitionStateMap::new, new GridPartitionStateMapSerializer());
        factory.register(GridDhtPartitionMap::new, new GridDhtPartitionMapSerializer());
        factory.register(GridDhtPartitionFullMap::new, new GridDhtPartitionFullMapSerializer());
        factory.register(SnapshotOperationResponse::new, new SnapshotOperationResponseSerializer());
        factory.register(SnapshotHandlerResult::new, new SnapshotHandlerResultSerializer());
        factory.register(DataStreamerUpdatesHandlerResult::new, new DataStreamerUpdatesHandlerResultSerializer());
        factory.register(SnapshotCheckResponse::new, new SnapshotCheckResponseSerializer());
        factory.register(IncrementalSnapshotVerifyResult::new,
            new IncrementalSnapshotVerifyResultMarshallableSerializer(marsh, clsLdr));
        factory.register(SnapshotRestoreOperationResponse::new,
            new SnapshotRestoreOperationResponseMarshallableSerializer(marsh, clsLdr));
        factory.register(SnapshotMetadataResponse::new,
            new SnapshotMetadataResponseMarshallableSerializer(marsh, clsLdr));
        factory.register(SnapshotCheckPartitionHashesResponse::new,
            new SnapshotCheckPartitionHashesResponseMarshallableSerializer(marsh, clsLdr));
        factory.register(SnapshotCheckHandlersResponse::new, new SnapshotCheckHandlersResponseSerializer());
        factory.register(SnapshotCheckHandlersNodeResponse::new, new SnapshotCheckHandlersNodeResponseSerializer());
        factory.register(SnapshotPartitionsVerifyHandlerResponse::new,
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
