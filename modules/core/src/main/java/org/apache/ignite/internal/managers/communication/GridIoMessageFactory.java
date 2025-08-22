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

import org.apache.ignite.internal.GridJobCancelRequest;
import org.apache.ignite.internal.GridJobExecuteRequest;
import org.apache.ignite.internal.GridJobExecuteResponse;
import org.apache.ignite.internal.GridJobSiblingsRequest;
import org.apache.ignite.internal.GridJobSiblingsResponse;
import org.apache.ignite.internal.GridTaskCancelRequest;
import org.apache.ignite.internal.GridTaskSessionRequest;
import org.apache.ignite.internal.IgniteDiagnosticMessage;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.codegen.CacheEvictionEntrySerializer;
import org.apache.ignite.internal.codegen.CacheVersionedValueSerializer;
import org.apache.ignite.internal.codegen.GenerateEncryptionKeyRequestSerializer;
import org.apache.ignite.internal.codegen.GridCacheEntryInfoSerializer;
import org.apache.ignite.internal.codegen.GridCacheVersionExSerializer;
import org.apache.ignite.internal.codegen.GridCacheVersionSerializer;
import org.apache.ignite.internal.codegen.GridCheckpointRequestSerializer;
import org.apache.ignite.internal.codegen.GridDhtPartitionExchangeIdSerializer;
import org.apache.ignite.internal.codegen.GridIntListSerializer;
import org.apache.ignite.internal.codegen.GridJobCancelRequestSerializer;
import org.apache.ignite.internal.codegen.GridJobSiblingsRequestSerializer;
import org.apache.ignite.internal.codegen.GridQueryKillRequestSerializer;
import org.apache.ignite.internal.codegen.GridQueryKillResponseSerializer;
import org.apache.ignite.internal.codegen.GridQueryNextPageRequestSerializer;
import org.apache.ignite.internal.codegen.GridTaskCancelRequestSerializer;
import org.apache.ignite.internal.codegen.GridTaskResultRequestSerializer;
import org.apache.ignite.internal.codegen.IgniteTxKeySerializer;
import org.apache.ignite.internal.codegen.JobStealingRequestSerializer;
import org.apache.ignite.internal.codegen.LatchAckMessageSerializer;
import org.apache.ignite.internal.codegen.MetadataRequestMessageSerializer;
import org.apache.ignite.internal.codegen.MissingMappingRequestMessageSerializer;
import org.apache.ignite.internal.codegen.MissingMappingResponseMessageSerializer;
import org.apache.ignite.internal.codegen.NearCacheUpdatesSerializer;
import org.apache.ignite.internal.codegen.SessionChannelMessageSerializer;
import org.apache.ignite.internal.codegen.TcpInverseConnectionResponseMessageSerializer;
import org.apache.ignite.internal.codegen.TxLockSerializer;
import org.apache.ignite.internal.codegen.TxLocksRequestSerializer;
import org.apache.ignite.internal.codegen.TxLocksResponseSerializer;
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
import org.apache.ignite.internal.processors.cache.CacheEntryPredicateContainsValue;
import org.apache.ignite.internal.processors.cache.CacheEntrySerializablePredicate;
import org.apache.ignite.internal.processors.cache.CacheEvictionEntry;
import org.apache.ignite.internal.processors.cache.CacheInvokeDirectResult;
import org.apache.ignite.internal.processors.cache.CacheObjectByteArrayImpl;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridChangeGlobalStateMessageResponse;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.WalStateAckMessage;
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
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedUnlockRequest;
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
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtForceKeysRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtForceKeysResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyErrorMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleRequest;
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
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearUnlockRequest;
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
import org.apache.ignite.internal.processors.cluster.ClusterMetricsUpdateMessage;
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
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.GridMessageCollection;
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
        // -46 ... -51 - snapshot messages.
        factory.register((short)-61, IgniteDiagnosticMessage::new);
        factory.register((short)-53, SchemaOperationStatusMessage::new);
        factory.register((short)-52, GridIntList::new, new GridIntListSerializer());
        factory.register((short)-51, NearCacheUpdates::new, new NearCacheUpdatesSerializer());
        factory.register((short)-50, GridNearAtomicCheckUpdateRequest::new);
        factory.register((short)-49, UpdateErrors::new);
        factory.register((short)-48, GridDhtAtomicNearResponse::new);
        factory.register((short)-45, GridChangeGlobalStateMessageResponse::new);
        factory.register((short)-43, IgniteIoTestMessage::new);
        factory.register((short)-36, GridDhtAtomicSingleUpdateRequest::new);
        factory.register((short)-27, GridDhtTxOnePhaseCommitAckRequest::new);
        factory.register((short)-26, TxLockList::new);
        factory.register((short)-25, TxLock::new, new TxLockSerializer());
        factory.register((short)-24, TxLocksRequest::new, new TxLocksRequestSerializer());
        factory.register((short)-23, TxLocksResponse::new, new TxLocksResponseSerializer());
        factory.register(TcpCommunicationSpi.NODE_ID_MSG_TYPE, NodeIdMessage::new);
        factory.register(TcpCommunicationSpi.RECOVERY_LAST_ID_MSG_TYPE, RecoveryLastReceivedMessage::new);
        factory.register(TcpCommunicationSpi.HANDSHAKE_MSG_TYPE, HandshakeMessage::new);
        factory.register(TcpCommunicationSpi.HANDSHAKE_WAIT_MSG_TYPE, HandshakeWaitMessage::new);
        factory.register((short)0, GridJobCancelRequest::new, new GridJobCancelRequestSerializer());
        factory.register((short)1, GridJobExecuteRequest::new);
        factory.register((short)2, GridJobExecuteResponse::new);
        factory.register((short)3, GridJobSiblingsRequest::new, new GridJobSiblingsRequestSerializer());
        factory.register((short)4, GridJobSiblingsResponse::new);
        factory.register((short)5, GridTaskCancelRequest::new, new GridTaskCancelRequestSerializer());
        factory.register((short)6, GridTaskSessionRequest::new);
        factory.register((short)7, GridCheckpointRequest::new, new GridCheckpointRequestSerializer());
        factory.register((short)8, GridIoMessage::new);
        factory.register((short)9, GridIoUserMessage::new);
        factory.register((short)10, GridDeploymentInfoBean::new);
        factory.register((short)11, GridDeploymentRequest::new);
        factory.register((short)12, GridDeploymentResponse::new);
        factory.register((short)13, GridEventStorageMessage::new);
        factory.register((short)16, GridCacheTxRecoveryRequest::new);
        factory.register((short)17, GridCacheTxRecoveryResponse::new);
        factory.register((short)20, GridCacheTtlUpdateRequest::new);
        factory.register((short)21, GridDistributedLockRequest::new);
        factory.register((short)22, GridDistributedLockResponse::new);
        factory.register((short)23, GridDistributedTxFinishRequest::new);
        factory.register((short)24, GridDistributedTxFinishResponse::new);
        factory.register((short)25, GridDistributedTxPrepareRequest::new);
        factory.register((short)26, GridDistributedTxPrepareResponse::new);
        factory.register((short)27, GridDistributedUnlockRequest::new);
        factory.register((short)28, GridDhtAffinityAssignmentRequest::new);
        factory.register((short)29, GridDhtAffinityAssignmentResponse::new);
        factory.register((short)30, GridDhtLockRequest::new);
        factory.register((short)31, GridDhtLockResponse::new);
        factory.register((short)32, GridDhtTxFinishRequest::new);
        factory.register((short)33, GridDhtTxFinishResponse::new);
        factory.register((short)34, GridDhtTxPrepareRequest::new);
        factory.register((short)35, GridDhtTxPrepareResponse::new);
        factory.register((short)36, GridDhtUnlockRequest::new);
        factory.register((short)37, GridDhtAtomicDeferredUpdateResponse::new);
        factory.register((short)38, GridDhtAtomicUpdateRequest::new);
        factory.register((short)39, GridDhtAtomicUpdateResponse::new);
        factory.register((short)40, GridNearAtomicFullUpdateRequest::new);
        factory.register((short)41, GridNearAtomicUpdateResponse::new);
        factory.register((short)42, GridDhtForceKeysRequest::new);
        factory.register((short)43, GridDhtForceKeysResponse::new);
        factory.register((short)45, GridDhtPartitionDemandMessage::new);
        factory.register((short)46, GridDhtPartitionsFullMessage::new);
        factory.register((short)47, GridDhtPartitionsSingleMessage::new);
        factory.register((short)48, GridDhtPartitionsSingleRequest::new);
        factory.register((short)49, GridNearGetRequest::new);
        factory.register((short)50, GridNearGetResponse::new);
        factory.register((short)51, GridNearLockRequest::new);
        factory.register((short)52, GridNearLockResponse::new);
        factory.register((short)53, GridNearTxFinishRequest::new);
        factory.register((short)54, GridNearTxFinishResponse::new);
        factory.register((short)55, GridNearTxPrepareRequest::new);
        factory.register((short)56, GridNearTxPrepareResponse::new);
        factory.register((short)57, GridNearUnlockRequest::new);
        factory.register((short)58, GridCacheQueryRequest::new);
        factory.register((short)59, GridCacheQueryResponse::new);
        factory.register((short)61, GridContinuousMessage::new);
        factory.register((short)62, DataStreamerRequest::new);
        factory.register((short)63, DataStreamerResponse::new);
        factory.register((short)76, GridTaskResultRequest::new, new GridTaskResultRequestSerializer());
        factory.register((short)77, GridTaskResultResponse::new);
        factory.register((short)78, MissingMappingRequestMessage::new, new MissingMappingRequestMessageSerializer());
        factory.register((short)79, MissingMappingResponseMessage::new, new MissingMappingResponseMessageSerializer());
        factory.register((short)80, MetadataRequestMessage::new, new MetadataRequestMessageSerializer());
        factory.register((short)81, MetadataResponseMessage::new);
        factory.register((short)82, JobStealingRequest::new, new JobStealingRequestSerializer());
        factory.register((short)84, GridByteArrayList::new);
        factory.register((short)85, GridLongList::new);
        factory.register((short)86, GridCacheVersion::new, new GridCacheVersionSerializer());
        factory.register((short)87, GridDhtPartitionExchangeId::new, new GridDhtPartitionExchangeIdSerializer());
        factory.register((short)88, GridCacheReturn::new);
        factory.register((short)89, CacheObjectImpl::new);
        factory.register((short)90, KeyCacheObjectImpl::new);
        factory.register((short)91, GridCacheEntryInfo::new, new GridCacheEntryInfoSerializer());
        factory.register((short)92, CacheEntryInfoCollection::new);
        factory.register((short)93, CacheInvokeDirectResult::new);
        factory.register((short)94, IgniteTxKey::new, new IgniteTxKeySerializer());
        factory.register((short)95, DataStreamerEntry::new);
        factory.register((short)96, CacheContinuousQueryEntry::new);
        factory.register((short)97, CacheEvictionEntry::new, new CacheEvictionEntrySerializer());
        factory.register((short)98, CacheEntryPredicateContainsValue::new);
        factory.register((short)99, CacheEntrySerializablePredicate::new);
        factory.register((short)100, IgniteTxEntry::new);
        factory.register((short)101, TxEntryValueHolder::new);
        factory.register((short)102, CacheVersionedValue::new, new CacheVersionedValueSerializer());
        factory.register((short)103, GridCacheRawVersionedEntry::new);
        factory.register((short)104, GridCacheVersionEx::new, new GridCacheVersionExSerializer());
        factory.register((short)105, CacheObjectByteArrayImpl::new);
        factory.register((short)106, GridQueryCancelRequest::new);
        factory.register((short)107, GridQueryFailResponse::new);
        factory.register((short)108, GridQueryNextPageRequest::new, new GridQueryNextPageRequestSerializer());
        factory.register((short)109, GridQueryNextPageResponse::new);
        factory.register((short)112, GridCacheSqlQuery::new);
        // 113 - BinaryObjectImpl
        factory.register((short)114, GridDhtPartitionSupplyMessage::new);
        factory.register((short)115, UUIDCollectionMessage::new);
        factory.register((short)116, GridNearSingleGetRequest::new);
        factory.register((short)117, GridNearSingleGetResponse::new);
        factory.register((short)118, CacheContinuousQueryBatchAck::new);
        // 119 - BinaryEnumObjectImpl
        BinaryUtils.registerMessages(factory::register);

        // [120..123] - DR
        factory.register((short)124, GridMessageCollection::new);
        factory.register((short)125, GridNearAtomicSingleUpdateRequest::new);
        factory.register((short)126, GridNearAtomicSingleUpdateInvokeRequest::new);
        factory.register((short)127, GridNearAtomicSingleUpdateFilterRequest::new);
        factory.register((short)128, CacheGroupAffinityMessage::new);
        factory.register((short)129, WalStateAckMessage::new, new WalStateAckMessageSerializer());
        factory.register((short)130, UserManagementOperationFinishedMessage::new, new UserManagementOperationFinishedMessageSerializer());
        factory.register((short)131, UserAuthenticateRequestMessage::new, new UserAuthenticateRequestMessageSerializer());
        factory.register((short)132, UserAuthenticateResponseMessage::new, new UserAuthenticateResponseMessageSerializer());
        factory.register((short)133, ClusterMetricsUpdateMessage::new);
        factory.register((short)134, ContinuousRoutineStartResultMessage::new);
        factory.register((short)135, LatchAckMessage::new, new LatchAckMessageSerializer());
        factory.register((short)157, PartitionUpdateCountersMessage::new);
        factory.register((short)158, GridDhtPartitionSupplyErrorMessage::new);
        factory.register((short)162, GenerateEncryptionKeyRequest::new, new GenerateEncryptionKeyRequestSerializer());
        factory.register((short)163, GenerateEncryptionKeyResponse::new);
        factory.register((short)167, ServiceDeploymentProcessId::new);
        factory.register((short)168, ServiceSingleNodeDeploymentResultBatch::new);
        factory.register((short)169, ServiceSingleNodeDeploymentResult::new);
        factory.register(GridQueryKillRequest.TYPE_CODE, GridQueryKillRequest::new, new GridQueryKillRequestSerializer());
        factory.register(GridQueryKillResponse.TYPE_CODE, GridQueryKillResponse::new, new GridQueryKillResponseSerializer());
        factory.register(GridIoSecurityAwareMessage.TYPE_CODE, GridIoSecurityAwareMessage::new);
        factory.register(SessionChannelMessage.TYPE_CODE, SessionChannelMessage::new, new SessionChannelMessageSerializer());
        factory.register(SingleNodeMessage.TYPE_CODE, SingleNodeMessage::new);
        factory.register((short)177, TcpInverseConnectionResponseMessage::new, new TcpInverseConnectionResponseMessageSerializer());
        factory.register(SnapshotFilesRequestMessage.TYPE_CODE, SnapshotFilesRequestMessage::new);
        factory.register(SnapshotFilesFailureMessage.TYPE_CODE, SnapshotFilesFailureMessage::new);
        factory.register((short)180, AtomicApplicationAttributesAwareRequest::new);
        factory.register((short)181, TransactionAttributesAwareRequest::new);

        // Incremental snapshot.
        factory.register(IncrementalSnapshotAwareMessage.TYPE_CODE, IncrementalSnapshotAwareMessage::new);

        // Index statistics.
        factory.register(StatisticsKeyMessage.TYPE_CODE, StatisticsKeyMessage::new);
        factory.register(StatisticsDecimalMessage.TYPE_CODE, StatisticsDecimalMessage::new);
        factory.register(StatisticsObjectData.TYPE_CODE, StatisticsObjectData::new);
        factory.register(StatisticsColumnData.TYPE_CODE, StatisticsColumnData::new);
        factory.register(StatisticsRequest.TYPE_CODE, StatisticsRequest::new);
        factory.register(StatisticsResponse.TYPE_CODE, StatisticsResponse::new);

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
