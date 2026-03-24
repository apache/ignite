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
import org.apache.ignite.internal.cache.query.index.IndexKeyTypeMessage;
import org.apache.ignite.internal.cache.query.index.IndexKeyTypeMessageMarshallableSerializer;
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
import org.apache.ignite.internal.processors.cache.CacheEntryInfoCollection;
import org.apache.ignite.internal.processors.cache.CacheEntryInfoCollectionSerializer;
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
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionsToReloadMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionsToReloadMapSerializer;
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
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsToReload;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsToReloadSerializer;
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
import org.apache.ignite.internal.util.UUIDCollectionMessage;
import org.apache.ignite.internal.util.UUIDCollectionMessageSerializer;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.internal.util.distributed.SingleNodeMessageSerializer;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.spi.collision.jobstealing.JobStealingRequest;
import org.apache.ignite.spi.collision.jobstealing.JobStealingRequestSerializer;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
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
        factory.register(ErrorMessage::new, new ErrorMessageMarshallableSerializer(marsh, clsLdr, -66));
        factory.register(TxInfo::new, new TxInfoSerializer(-65));
        factory.register(TxEntriesInfo::new, new TxEntriesInfoSerializer(-64));
        factory.register(ExchangeInfo::new, new ExchangeInfoSerializer(-63));
        factory.register(IgniteDiagnosticResponse::new, new IgniteDiagnosticResponseSerializer(-62));
        factory.register(IgniteDiagnosticRequest::new, new IgniteDiagnosticRequestSerializer(-61));
        factory.register(SchemaOperationStatusMessage::new, new SchemaOperationStatusMessageSerializer(-53));
        factory.register(NearCacheUpdates::new, new NearCacheUpdatesSerializer(-51));
        factory.register(GridNearAtomicCheckUpdateRequest::new, new GridNearAtomicCheckUpdateRequestSerializer(-50));
        factory.register(UpdateErrors::new, new UpdateErrorsSerializer(-49));
        factory.register(GridDhtAtomicNearResponse::new, new GridDhtAtomicNearResponseSerializer(-48));
        factory.register(GridChangeGlobalStateMessageResponse::new, new GridChangeGlobalStateMessageResponseSerializer(-45));
        factory.register((short)-43, IgniteIoTestMessage::new);
        factory.register(GridDhtAtomicSingleUpdateRequest::new, new GridDhtAtomicSingleUpdateRequestSerializer(-36));
        factory.register(GridDhtTxOnePhaseCommitAckRequest::new, new GridDhtTxOnePhaseCommitAckRequestSerializer(-27));
        factory.register(TxLockList::new, new TxLockListSerializer(-26));
        factory.register(TxLock::new, new TxLockSerializer(-25));
        factory.register(TxLocksRequest::new, new TxLocksRequestSerializer(-24));
        factory.register(TxLocksResponse::new, new TxLocksResponseSerializer(-23));
        factory.register(NodeIdMessage::new, new NodeIdMessageSerializer(TcpCommunicationSpi.NODE_ID_MSG_TYPE));
        factory.register(RecoveryLastReceivedMessage::new,
            new RecoveryLastReceivedMessageSerializer(TcpCommunicationSpi.RECOVERY_LAST_ID_MSG_TYPE));
        factory.register(HandshakeMessage::new, new HandshakeMessageSerializer(TcpCommunicationSpi.HANDSHAKE_MSG_TYPE));
        factory.register(HandshakeWaitMessage::new, new HandshakeWaitMessageSerializer(TcpCommunicationSpi.HANDSHAKE_WAIT_MSG_TYPE));
        factory.register(GridJobCancelRequest::new, new GridJobCancelRequestSerializer(0));
        factory.register(GridJobExecuteRequest::new, new GridJobExecuteRequestSerializer(1));
        factory.register(GridJobExecuteResponse::new, new GridJobExecuteResponseSerializer(2));
        factory.register(GridJobSiblingsRequest::new, new GridJobSiblingsRequestSerializer(3));
        factory.register(GridJobSiblingsResponse::new, new GridJobSiblingsResponseSerializer(4));
        factory.register(GridTaskCancelRequest::new, new GridTaskCancelRequestSerializer(5));
        factory.register(GridTaskSessionRequest::new, new GridTaskSessionRequestSerializer(6));
        factory.register(GridCheckpointRequest::new, new GridCheckpointRequestSerializer(7));
        factory.register(GridIoMessage::new, new GridIoMessageMarshallableSerializer(marsh, clsLdr, 8));
        factory.register(GridIoUserMessage::new, new GridIoUserMessageSerializer(9));
        factory.register(GridDeploymentInfoBean::new, new GridDeploymentInfoBeanSerializer(10));
        factory.register(GridDeploymentRequest::new, new GridDeploymentRequestSerializer(11));
        factory.register(GridDeploymentResponse::new, new GridDeploymentResponseSerializer(12));
        factory.register(GridEventStorageMessage::new, new GridEventStorageMessageMarshallableSerializer(marsh, clsLdr, 13));
        factory.register(GridCacheTxRecoveryRequest::new, new GridCacheTxRecoveryRequestSerializer(16));
        factory.register(GridCacheTxRecoveryResponse::new, new GridCacheTxRecoveryResponseSerializer(17));
        factory.register(IndexQueryResultMeta::new, new IndexQueryResultMetaSerializer(18));
        factory.register(IndexKeyTypeSettings::new, new IndexKeyTypeSettingsSerializer(19));
        factory.register(GridCacheTtlUpdateRequest::new, new GridCacheTtlUpdateRequestSerializer(20));
        factory.register(GridDistributedLockRequest::new, new GridDistributedLockRequestSerializer(21));
        factory.register(GridDistributedLockResponse::new, new GridDistributedLockResponseSerializer(22));
        factory.register(GridDistributedTxFinishRequest::new, new GridDistributedTxFinishRequestSerializer(23));
        factory.register(GridDistributedTxFinishResponse::new, new GridDistributedTxFinishResponseSerializer(24));
        factory.register(GridDistributedTxPrepareRequest::new, new GridDistributedTxPrepareRequestSerializer(25));
        factory.register(GridDistributedTxPrepareResponse::new, new GridDistributedTxPrepareResponseSerializer(26));
        // Type 27 is former GridDistributedUnlockRequest
        factory.register(GridDhtAffinityAssignmentRequest::new, new GridDhtAffinityAssignmentRequestSerializer(28));
        factory.register(GridDhtAffinityAssignmentResponse::new, new GridDhtAffinityAssignmentResponseSerializer(29));
        factory.register(GridDhtLockRequest::new, new GridDhtLockRequestSerializer(30));
        factory.register(GridDhtLockResponse::new, new GridDhtLockResponseSerializer(31));
        factory.register(GridDhtTxFinishRequest::new, new GridDhtTxFinishRequestSerializer(32));
        factory.register(GridDhtTxFinishResponse::new, new GridDhtTxFinishResponseSerializer(33));
        factory.register(GridDhtTxPrepareRequest::new, new GridDhtTxPrepareRequestSerializer(34));
        factory.register(GridDhtTxPrepareResponse::new, new GridDhtTxPrepareResponseSerializer(35));
        factory.register(GridDhtUnlockRequest::new, new GridDhtUnlockRequestSerializer(36));
        factory.register(GridDhtAtomicDeferredUpdateResponse::new, new GridDhtAtomicDeferredUpdateResponseSerializer(37));
        factory.register(GridDhtAtomicUpdateRequest::new, new GridDhtAtomicUpdateRequestSerializer(38));
        factory.register(GridDhtAtomicUpdateResponse::new, new GridDhtAtomicUpdateResponseSerializer(39));
        factory.register(GridNearAtomicFullUpdateRequest::new, new GridNearAtomicFullUpdateRequestSerializer(40));
        factory.register(GridNearAtomicUpdateResponse::new, new GridNearAtomicUpdateResponseSerializer(41));
        factory.register(GridDhtForceKeysRequest::new, new GridDhtForceKeysRequestSerializer(42));
        factory.register(GridDhtForceKeysResponse::new, new GridDhtForceKeysResponseSerializer(43));
        factory.register(GridDhtPartitionDemandMessage::new, new GridDhtPartitionDemandMessageSerializer(45));
        factory.register(GridDhtPartitionsFullMessage::new, new GridDhtPartitionsFullMessageMarshallableSerializer(marsh, clsLdr, 46));
        factory.register(GridDhtPartitionsSingleMessage::new, new GridDhtPartitionsSingleMessageSerializer(47));
        factory.register(GridDhtPartitionsSingleRequest::new, new GridDhtPartitionsSingleRequestSerializer(48));
        factory.register(GridNearGetRequest::new, new GridNearGetRequestSerializer(49));
        factory.register(GridNearGetResponse::new, new GridNearGetResponseSerializer(50));
        factory.register(GridNearLockRequest::new, new GridNearLockRequestSerializer(51));
        factory.register(GridNearLockResponse::new, new GridNearLockResponseSerializer(52));
        factory.register(GridNearTxFinishRequest::new, new GridNearTxFinishRequestSerializer(53));
        factory.register(GridNearTxFinishResponse::new, new GridNearTxFinishResponseSerializer(54));
        factory.register(GridNearTxPrepareRequest::new, new GridNearTxPrepareRequestSerializer(55));
        factory.register(GridNearTxPrepareResponse::new, new GridNearTxPrepareResponseSerializer(56));
        factory.register(GridNearUnlockRequest::new, new GridNearUnlockRequestSerializer(57));
        factory.register(GridCacheQueryRequest::new, new GridCacheQueryRequestSerializer(58));
        factory.register(GridCacheQueryResponse::new, new GridCacheQueryResponseSerializer(59));
        factory.register(GridContinuousMessage::new, new GridContinuousMessageSerializer(61));
        factory.register(DataStreamerRequest::new, new DataStreamerRequestSerializer(62));
        factory.register(DataStreamerResponse::new, new DataStreamerResponseSerializer(63));
        factory.register(GridTaskResultRequest::new, new GridTaskResultRequestSerializer(76));
        factory.register(GridTaskResultResponse::new, new GridTaskResultResponseSerializer(77));
        factory.register(MissingMappingRequestMessage::new, new MissingMappingRequestMessageSerializer(78));
        factory.register(MissingMappingResponseMessage::new, new MissingMappingResponseMessageSerializer(79));
        factory.register(MetadataRequestMessage::new, new MetadataRequestMessageSerializer(80));
        factory.register(MetadataResponseMessage::new, new MetadataResponseMessageSerializer(81));
        factory.register(JobStealingRequest::new, new JobStealingRequestSerializer(82));
        factory.register(GridByteArrayList::new, new GridByteArrayListSerializer(84));
        factory.register(GridCacheVersion::new, new GridCacheVersionSerializer(86));
        factory.register(GridDhtPartitionExchangeId::new, new GridDhtPartitionExchangeIdSerializer(87));
        factory.register(GridCacheReturn::new, new GridCacheReturnSerializer(88));
        factory.register(GridCacheEntryInfo::new, new GridCacheEntryInfoSerializer(91));
        factory.register(CacheEntryInfoCollection::new, new CacheEntryInfoCollectionSerializer(92));
        factory.register(CacheInvokeDirectResult::new, new CacheInvokeDirectResultSerializer(93));
        factory.register(IgniteTxKey::new, new IgniteTxKeySerializer(94));
        factory.register(DataStreamerEntry::new, new DataStreamerEntrySerializer(95));
        factory.register(CacheContinuousQueryEntry::new, new CacheContinuousQueryEntryMarshallableSerializer(marsh, clsLdr, 96));
        factory.register(CacheEvictionEntry::new, new CacheEvictionEntrySerializer(97));
        factory.register(CacheEntryPredicateAdapter::new, new CacheEntryPredicateAdapterMarshallableSerializer(marsh, clsLdr, 98));
        factory.register(IgniteTxEntry::new, new IgniteTxEntrySerializer(100));
        factory.register(TxEntryValueHolder::new, new TxEntryValueHolderSerializer(101));
        factory.register(CacheVersionedValue::new, new CacheVersionedValueSerializer(102));
        factory.register(GridCacheRawVersionedEntry::new, new GridCacheRawVersionedEntrySerializer(103));
        factory.register(GridCacheVersionEx::new, new GridCacheVersionExSerializer(104));
        factory.register(GridQueryCancelRequest::new, new GridQueryCancelRequestSerializer(106));
        factory.register(GridQueryFailResponse::new, new GridQueryFailResponseSerializer(107));
        factory.register(GridQueryNextPageRequest::new, new GridQueryNextPageRequestSerializer(108));
        factory.register(GridQueryNextPageResponse::new, new GridQueryNextPageResponseSerializer(109));
        factory.register(GridCacheSqlQuery::new, new GridCacheSqlQuerySerializer(112));
        factory.register(IndexKeyDefinition::new, new IndexKeyDefinitionSerializer(113));
        factory.register(GridDhtPartitionSupplyMessage::new, new GridDhtPartitionSupplyMessageSerializer(114));
        factory.register(UUIDCollectionMessage::new, new UUIDCollectionMessageSerializer(115));
        factory.register(GridNearSingleGetRequest::new, new GridNearSingleGetRequestSerializer(116));
        factory.register(GridNearSingleGetResponse::new, new GridNearSingleGetResponseSerializer(117));
        factory.register(CacheContinuousQueryBatchAck::new, new CacheContinuousQueryBatchAckSerializer(118));

        // [120..123] - DR
        factory.register(GridNearAtomicSingleUpdateRequest::new, new GridNearAtomicSingleUpdateRequestSerializer(125));
        factory.register(GridNearAtomicSingleUpdateInvokeRequest::new, new GridNearAtomicSingleUpdateInvokeRequestSerializer(126));
        factory.register(GridNearAtomicSingleUpdateFilterRequest::new, new GridNearAtomicSingleUpdateFilterRequestSerializer(127));
        factory.register(CacheGroupAffinityMessage::new, new CacheGroupAffinityMessageSerializer(128));
        factory.register(WalStateAckMessage::new, new WalStateAckMessageSerializer(129));
        factory.register(UserManagementOperationFinishedMessage::new, new UserManagementOperationFinishedMessageSerializer(130));
        factory.register(UserAuthenticateRequestMessage::new, new UserAuthenticateRequestMessageSerializer(131));
        factory.register(UserAuthenticateResponseMessage::new, new UserAuthenticateResponseMessageSerializer(132));
        factory.register(ClusterMetricsUpdateMessage::new,
            new ClusterMetricsUpdateMessageSerializer(ClusterMetricsUpdateMessage.TYPE_CODE));
        factory.register(ContinuousRoutineStartResultMessage::new, new ContinuousRoutineStartResultMessageSerializer(134));
        factory.register(LatchAckMessage::new, new LatchAckMessageSerializer(135));
        factory.register(CacheMetricsMessage::new, new CacheMetricsMessageSerializer(CacheMetricsMessage.TYPE_CODE));
        factory.register(NodeMetricsMessage::new, new NodeMetricsMessageSerializer(NodeMetricsMessage.TYPE_CODE));
        factory.register(NodeFullMetricsMessage::new, new NodeFullMetricsMessageSerializer(NodeFullMetricsMessage.TYPE_CODE));
        factory.register(PartitionUpdateCountersMessage::new, new PartitionUpdateCountersMessageMarshallableSerializer(marsh, clsLdr, 157));
        factory.register(GenerateEncryptionKeyRequest::new, new GenerateEncryptionKeyRequestSerializer(162));
        factory.register(GenerateEncryptionKeyResponse::new, new GenerateEncryptionKeyResponseSerializer(163));
        factory.register(ServiceDeploymentProcessId::new, new ServiceDeploymentProcessIdSerializer(167));
        factory.register(ServiceSingleNodeDeploymentResultBatch::new, new ServiceSingleNodeDeploymentResultBatchSerializer(168));
        factory.register(ServiceSingleNodeDeploymentResult::new, new ServiceSingleNodeDeploymentResultSerializer(169));
        factory.register(GridQueryKillRequest::new, new GridQueryKillRequestSerializer(GridQueryKillRequest.TYPE_CODE));
        factory.register(GridQueryKillResponse::new, new GridQueryKillResponseSerializer(GridQueryKillResponse.TYPE_CODE));
        factory.register(GridIoSecurityAwareMessage::new,
            new GridIoSecurityAwareMessageMarshallableSerializer(marsh, clsLdr, GridIoSecurityAwareMessage.TYPE_CODE));
        factory.register(SessionChannelMessage::new, new SessionChannelMessageSerializer(SessionChannelMessage.TYPE_CODE));
        factory.register(SingleNodeMessage::new, new SingleNodeMessageSerializer(SingleNodeMessage.TYPE_CODE));
        factory.register(TcpInverseConnectionResponseMessage::new, new TcpInverseConnectionResponseMessageSerializer(177));
        factory.register(SnapshotFilesRequestMessage::new,
            new SnapshotFilesRequestMessageSerializer(SnapshotFilesRequestMessage.TYPE_CODE));
        factory.register(SnapshotFilesFailureMessage::new,
            new SnapshotFilesFailureMessageSerializer(SnapshotFilesFailureMessage.TYPE_CODE));
        factory.register(AtomicApplicationAttributesAwareRequest::new, new AtomicApplicationAttributesAwareRequestSerializer(180));
        factory.register(TransactionAttributesAwareRequest::new, new TransactionAttributesAwareRequestSerializer(181));

        // Incremental snapshot.
        factory.register(IncrementalSnapshotAwareMessage::new,
            new IncrementalSnapshotAwareMessageSerializer(IncrementalSnapshotAwareMessage.TYPE_CODE));

        // Index statistics.
        factory.register(StatisticsKeyMessage::new, new StatisticsKeyMessageSerializer(StatisticsKeyMessage.TYPE_CODE));
        factory.register(StatisticsDecimalMessage::new, new StatisticsDecimalMessageSerializer(StatisticsDecimalMessage.TYPE_CODE));
        factory.register(StatisticsObjectData::new, new StatisticsObjectDataSerializer(StatisticsObjectData.TYPE_CODE));
        factory.register(StatisticsColumnData::new, new StatisticsColumnDataSerializer(StatisticsColumnData.TYPE_CODE));
        factory.register(StatisticsRequest::new, new StatisticsRequestSerializer(StatisticsRequest.TYPE_CODE));
        factory.register(StatisticsResponse::new, new StatisticsResponseSerializer(StatisticsResponse.TYPE_CODE));

        factory.register(CachePartitionPartialCountersMap::new,
            new CachePartitionPartialCountersMapSerializer(CachePartitionPartialCountersMap.TYPE_CODE));
        factory.register(IgniteDhtDemandedPartitionsMap::new,
            new IgniteDhtDemandedPartitionsMapSerializer(IgniteDhtDemandedPartitionsMap.TYPE_CODE));
        factory.register(BinaryMetadataVersionInfo::new,
            new BinaryMetadataVersionInfoSerializer(BinaryMetadataVersionInfo.TYPE_CODE));
        factory.register(CachePartitionFullCountersMap::new,
            new CachePartitionFullCountersMapSerializer(CachePartitionFullCountersMap.TYPE_CODE));
        factory.register(IgniteDhtPartitionCountersMap::new,
            new IgniteDhtPartitionCountersMapSerializer(IgniteDhtPartitionCountersMap.TYPE_CODE));
        factory.register(GroupPartitionIdPair::new, new GroupPartitionIdPairSerializer(GroupPartitionIdPair.TYPE_CODE));
        factory.register(PartitionReservationsMap::new, new PartitionReservationsMapSerializer(PartitionReservationsMap.TYPE_CODE));
        factory.register(IgniteDhtPartitionHistorySuppliersMap::new,
            new IgniteDhtPartitionHistorySuppliersMapSerializer(IgniteDhtPartitionHistorySuppliersMap.TYPE_CODE));
        factory.register(PartitionsToReload::new, new PartitionsToReloadSerializer(PartitionsToReload.TYPE_CODE));
        factory.register(CachePartitionsToReloadMap::new,
            new CachePartitionsToReloadMapSerializer(CachePartitionsToReloadMap.TYPE_CODE));
        factory.register(IgniteDhtPartitionsToReloadMap::new,
            new IgniteDhtPartitionsToReloadMapSerializer(IgniteDhtPartitionsToReloadMap.TYPE_CODE));
        factory.register(IntLongMap::new, new IntLongMapSerializer(IntLongMap.TYPE_CODE));
        factory.register(IndexKeyTypeMessage::new,
            new IndexKeyTypeMessageMarshallableSerializer(marsh, clsLdr, IndexKeyTypeMessage.TYPE_CODE));
        factory.register(GridPartitionStateMap::new, new GridPartitionStateMapSerializer(GridPartitionStateMap.TYPE_CODE));
        factory.register(GridDhtPartitionMap::new, new GridDhtPartitionMapSerializer(GridDhtPartitionMap.TYPE_CODE));
        factory.register(GridDhtPartitionFullMap::new, new GridDhtPartitionFullMapSerializer(GridDhtPartitionFullMap.TYPE_CODE));
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
