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
import org.apache.ignite.internal.binary.BinaryEnumObjectImpl;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.managers.checkpoint.GridCheckpointRequest;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfoBean;
import org.apache.ignite.internal.managers.deployment.GridDeploymentRequest;
import org.apache.ignite.internal.managers.deployment.GridDeploymentResponse;
import org.apache.ignite.internal.managers.encryption.GenerateEncryptionKeyRequest;
import org.apache.ignite.internal.managers.encryption.GenerateEncryptionKeyResponse;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
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
import org.apache.ignite.internal.processors.cache.GridCacheMvccEntryInfo;
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
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxQueryEnlistRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxQueryEnlistResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxQueryFirstEnlistRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtUnlockRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridInvokeValue;
import org.apache.ignite.internal.processors.cache.distributed.dht.PartitionUpdateCountersMessage;
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
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandLegacyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessageV2;
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
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxEnlistRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxEnlistResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxQueryEnlistRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxQueryEnlistResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxQueryResultsEnlistRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxQueryResultsEnlistResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearUnlockRequest;
import org.apache.ignite.internal.processors.cache.mvcc.DeadlockProbe;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshotWithoutTxs;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersionImpl;
import org.apache.ignite.internal.processors.cache.mvcc.ProbedTx;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccAckRequestQueryCntr;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccAckRequestQueryId;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccAckRequestTx;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccAckRequestTxAndQueryCntr;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccAckRequestTxAndQueryId;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccActiveQueriesMessage;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccFutureResponse;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccQuerySnapshotRequest;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccRecoveryFinishedMessage;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccSnapshotResponse;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccTxSnapshotRequest;
import org.apache.ignite.internal.processors.cache.mvcc.msg.PartitionCountersNeighborcastRequest;
import org.apache.ignite.internal.processors.cache.mvcc.msg.PartitionCountersNeighborcastResponse;
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
import org.apache.ignite.plugin.extensions.communication.IgniteMessageFactory;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.spi.collision.jobstealing.JobStealingRequest;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.communication.tcp.internal.TcpInverseConnectionResponseMessage;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeMessage;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeMessage2;
import org.apache.ignite.spi.communication.tcp.messages.HandshakeWaitMessage;
import org.apache.ignite.spi.communication.tcp.messages.NodeIdMessage;
import org.apache.ignite.spi.communication.tcp.messages.RecoveryLastReceivedMessage;

/**
 * Message factory implementation.
 */
public class GridIoMessageFactory implements MessageFactoryProvider {
    /** {@inheritDoc} */
    @Override public void registerAll(IgniteMessageFactory factory) {
        // -54 is reserved for SQL.
        // -46 ... -51 - snapshot messages.
        factory.register((short)-61, IgniteDiagnosticMessage::new);
        factory.register((short)-53, SchemaOperationStatusMessage::new);
        factory.register((short)-52, GridIntList::new);
        factory.register((short)-51, NearCacheUpdates::new);
        factory.register((short)-50, GridNearAtomicCheckUpdateRequest::new);
        factory.register((short)-49, UpdateErrors::new);
        factory.register((short)-48, GridDhtAtomicNearResponse::new);
        factory.register((short)-45, GridChangeGlobalStateMessageResponse::new);
        factory.register((short)-44, HandshakeMessage2::new);
        factory.register((short)-43, IgniteIoTestMessage::new);
        factory.register((short)-36, GridDhtAtomicSingleUpdateRequest::new);
        factory.register((short)-27, GridDhtTxOnePhaseCommitAckRequest::new);
        factory.register((short)-26, TxLockList::new);
        factory.register((short)-25, TxLock::new);
        factory.register((short)-24, TxLocksRequest::new);
        factory.register((short)-23, TxLocksResponse::new);
        factory.register(TcpCommunicationSpi.NODE_ID_MSG_TYPE, NodeIdMessage::new);
        factory.register(TcpCommunicationSpi.RECOVERY_LAST_ID_MSG_TYPE, RecoveryLastReceivedMessage::new);
        factory.register(TcpCommunicationSpi.HANDSHAKE_MSG_TYPE, HandshakeMessage::new);
        factory.register(TcpCommunicationSpi.HANDSHAKE_WAIT_MSG_TYPE, HandshakeWaitMessage::new);
        factory.register((short)0, GridJobCancelRequest::new);
        factory.register((short)1, GridJobExecuteRequest::new);
        factory.register((short)2, GridJobExecuteResponse::new);
        factory.register((short)3, GridJobSiblingsRequest::new);
        factory.register((short)4, GridJobSiblingsResponse::new);
        factory.register((short)5, GridTaskCancelRequest::new);
        factory.register((short)6, GridTaskSessionRequest::new);
        factory.register((short)7, GridCheckpointRequest::new);
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
        factory.register((short)44, GridDhtPartitionDemandLegacyMessage::new);
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
        factory.register((short)76, GridTaskResultRequest::new);
        factory.register((short)77, GridTaskResultResponse::new);
        factory.register((short)78, MissingMappingRequestMessage::new);
        factory.register((short)79, MissingMappingResponseMessage::new);
        factory.register((short)80, MetadataRequestMessage::new);
        factory.register((short)81, MetadataResponseMessage::new);
        factory.register((short)82, JobStealingRequest::new);
        factory.register((short)84, GridByteArrayList::new);
        factory.register((short)85, GridLongList::new);
        factory.register((short)86, GridCacheVersion::new);
        factory.register((short)87, GridDhtPartitionExchangeId::new);
        factory.register((short)88, GridCacheReturn::new);
        factory.register((short)89, CacheObjectImpl::new);
        factory.register((short)90, KeyCacheObjectImpl::new);
        factory.register((short)91, GridCacheEntryInfo::new);
        factory.register((short)92, CacheEntryInfoCollection::new);
        factory.register((short)93, CacheInvokeDirectResult::new);
        factory.register((short)94, IgniteTxKey::new);
        factory.register((short)95, DataStreamerEntry::new);
        factory.register((short)96, CacheContinuousQueryEntry::new);
        factory.register((short)97, CacheEvictionEntry::new);
        factory.register((short)98, CacheEntryPredicateContainsValue::new);
        factory.register((short)99, CacheEntrySerializablePredicate::new);
        factory.register((short)100, IgniteTxEntry::new);
        factory.register((short)101, TxEntryValueHolder::new);
        factory.register((short)102, CacheVersionedValue::new);
        factory.register((short)103, GridCacheRawVersionedEntry::new);
        factory.register((short)104, GridCacheVersionEx::new);
        factory.register((short)105, CacheObjectByteArrayImpl::new);
        factory.register((short)106, GridQueryCancelRequest::new);
        factory.register((short)107, GridQueryFailResponse::new);
        factory.register((short)108, GridQueryNextPageRequest::new);
        factory.register((short)109, GridQueryNextPageResponse::new);
        factory.register((short)111, AffinityTopologyVersion::new);
        factory.register((short)112, GridCacheSqlQuery::new);
        factory.register((short)113, BinaryObjectImpl::new);
        factory.register((short)114, GridDhtPartitionSupplyMessage::new);
        factory.register((short)115, UUIDCollectionMessage::new);
        factory.register((short)116, GridNearSingleGetRequest::new);
        factory.register((short)117, GridNearSingleGetResponse::new);
        factory.register((short)118, CacheContinuousQueryBatchAck::new);
        factory.register((short)119, BinaryEnumObjectImpl::new);

        // [120..123] - DR
        factory.register((short)124, GridMessageCollection::new);
        factory.register((short)125, GridNearAtomicSingleUpdateRequest::new);
        factory.register((short)126, GridNearAtomicSingleUpdateInvokeRequest::new);
        factory.register((short)127, GridNearAtomicSingleUpdateFilterRequest::new);
        factory.register((short)128, CacheGroupAffinityMessage::new);
        factory.register((short)129, WalStateAckMessage::new);
        factory.register((short)130, UserManagementOperationFinishedMessage::new);
        factory.register((short)131, UserAuthenticateRequestMessage::new);
        factory.register((short)132, UserAuthenticateResponseMessage::new);
        factory.register((short)133, ClusterMetricsUpdateMessage::new);
        factory.register((short)134, ContinuousRoutineStartResultMessage::new);
        factory.register((short)135, LatchAckMessage::new);
        factory.register((short)136, MvccTxSnapshotRequest::new);
        factory.register((short)137, MvccAckRequestTx::new);
        factory.register((short)138, MvccFutureResponse::new);
        factory.register((short)139, MvccQuerySnapshotRequest::new);
        factory.register((short)140, MvccAckRequestQueryCntr::new);
        factory.register((short)141, MvccSnapshotResponse::new);
        factory.register((short)143, GridCacheMvccEntryInfo::new);
        factory.register((short)144, GridDhtTxQueryEnlistResponse::new);
        factory.register((short)145, MvccAckRequestQueryId::new);
        factory.register((short)146, MvccAckRequestTxAndQueryCntr::new);
        factory.register((short)147, MvccAckRequestTxAndQueryId::new);
        factory.register((short)148, MvccVersionImpl::new);
        factory.register((short)149, MvccActiveQueriesMessage::new);
        factory.register((short)150, MvccSnapshotWithoutTxs::new);
        factory.register((short)151, GridNearTxQueryEnlistRequest::new);
        factory.register((short)152, GridNearTxQueryEnlistResponse::new);
        factory.register((short)153, GridNearTxQueryResultsEnlistRequest::new);
        factory.register((short)154, GridNearTxQueryResultsEnlistResponse::new);
        factory.register((short)155, GridDhtTxQueryEnlistRequest::new);
        factory.register((short)156, GridDhtTxQueryFirstEnlistRequest::new);
        factory.register((short)157, PartitionUpdateCountersMessage::new);
        factory.register((short)158, GridDhtPartitionSupplyMessageV2::new);
        factory.register((short)159, GridNearTxEnlistRequest::new);
        factory.register((short)160, GridNearTxEnlistResponse::new);
        factory.register((short)161, GridInvokeValue::new);
        factory.register((short)162, GenerateEncryptionKeyRequest::new);
        factory.register((short)163, GenerateEncryptionKeyResponse::new);
        factory.register((short)164, MvccRecoveryFinishedMessage::new);
        factory.register((short)165, PartitionCountersNeighborcastRequest::new);
        factory.register((short)166, PartitionCountersNeighborcastResponse::new);
        factory.register((short)167, ServiceDeploymentProcessId::new);
        factory.register((short)168, ServiceSingleNodeDeploymentResultBatch::new);
        factory.register((short)169, ServiceSingleNodeDeploymentResult::new);
        factory.register((short)170, DeadlockProbe::new);
        factory.register((short)171, ProbedTx::new);
        factory.register(GridQueryKillRequest.TYPE_CODE, GridQueryKillRequest::new);
        factory.register(GridQueryKillResponse.TYPE_CODE, GridQueryKillResponse::new);
        factory.register(GridIoSecurityAwareMessage.TYPE_CODE, GridIoSecurityAwareMessage::new);
        factory.register(SessionChannelMessage.TYPE_CODE, SessionChannelMessage::new);
        factory.register(SingleNodeMessage.TYPE_CODE, SingleNodeMessage::new);
        factory.register((short)177, TcpInverseConnectionResponseMessage::new);

        // [-3..119] [124..129] [-23..-28] [-36..-55] - this
        // [120..123] - DR
        // [-4..-22, -30..-35] - SQL
        // [2048..2053] - Snapshots
        // [-42..-37] - former hadoop.
        // [64..71] - former IGFS.
    }

    /** {@inheritDoc} */
    @Override public Message create(short type) {
        throw new UnsupportedOperationException();
    }
}
