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

import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridJobCancelRequest;
import org.apache.ignite.internal.GridJobExecuteRequest;
import org.apache.ignite.internal.GridJobExecuteResponse;
import org.apache.ignite.internal.GridJobSiblingsRequest;
import org.apache.ignite.internal.GridJobSiblingsResponse;
import org.apache.ignite.internal.GridTaskCancelRequest;
import org.apache.ignite.internal.GridTaskSessionRequest;
import org.apache.ignite.internal.binary.BinaryEnumObjectImpl;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.managers.checkpoint.GridCheckpointRequest;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfoBean;
import org.apache.ignite.internal.managers.deployment.GridDeploymentRequest;
import org.apache.ignite.internal.managers.deployment.GridDeploymentResponse;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryInfoCollection;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicateContainsValue;
import org.apache.ignite.internal.processors.cache.CacheEntrySerializablePredicate;
import org.apache.ignite.internal.processors.cache.CacheEvictionEntry;
import org.apache.ignite.internal.processors.cache.CacheInvokeDirectResult;
import org.apache.ignite.internal.processors.cache.CacheObjectByteArrayImpl;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheEvictionRequest;
import org.apache.ignite.internal.processors.cache.GridCacheEvictionResponse;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
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
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicDeferredUpdateResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicSingleUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicUpdateResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicFullUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateFilterRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateInvokeRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicUpdateResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtForceKeysRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtForceKeysResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessageV2;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleRequest;
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
import org.apache.ignite.internal.processors.clock.GridClockDeltaSnapshotMessage;
import org.apache.ignite.internal.processors.clock.GridClockDeltaVersion;
import org.apache.ignite.internal.processors.continuous.GridContinuousMessage;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerEntry;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerRequest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerResponse;
import org.apache.ignite.internal.processors.hadoop.HadoopJobId;
import org.apache.ignite.internal.processors.hadoop.shuffle.HadoopShuffleAck;
import org.apache.ignite.internal.processors.hadoop.shuffle.HadoopShuffleFinishRequest;
import org.apache.ignite.internal.processors.hadoop.shuffle.HadoopShuffleFinishResponse;
import org.apache.ignite.internal.processors.hadoop.shuffle.HadoopShuffleMessage;
import org.apache.ignite.internal.processors.hadoop.shuffle.HadoopDirectShuffleMessage;
import org.apache.ignite.internal.processors.igfs.IgfsAckMessage;
import org.apache.ignite.internal.processors.igfs.IgfsBlockKey;
import org.apache.ignite.internal.processors.igfs.IgfsBlocksMessage;
import org.apache.ignite.internal.processors.igfs.IgfsDeleteMessage;
import org.apache.ignite.internal.processors.igfs.IgfsFileAffinityRange;
import org.apache.ignite.internal.processors.igfs.IgfsFragmentizerRequest;
import org.apache.ignite.internal.processors.igfs.IgfsFragmentizerResponse;
import org.apache.ignite.internal.processors.igfs.IgfsSyncMessage;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryCancelRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryFailResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryNextPageRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryNextPageResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryRequest;
import org.apache.ignite.internal.processors.rest.handlers.task.GridTaskResultRequest;
import org.apache.ignite.internal.processors.rest.handlers.task.GridTaskResultResponse;
import org.apache.ignite.internal.util.GridByteArrayList;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.GridMessageCollection;
import org.apache.ignite.internal.util.UUIDCollectionMessage;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.spi.collision.jobstealing.JobStealingRequest;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.jsr166.ConcurrentHashMap8;

/**
 * Message factory implementation.
 */
public class GridIoMessageFactory implements MessageFactory {
    /** Custom messages registry. Used for test purposes. */
    private static final Map<Byte, IgniteOutClosure<Message>> CUSTOM = new ConcurrentHashMap8<>();

    /** Extensions. */
    private final MessageFactory[] ext;

    /**
     * @param ext Extensions.
     */
    public GridIoMessageFactory(MessageFactory[] ext) {
        this.ext = ext;
    }

    /** {@inheritDoc} */
    @Override public Message create(byte type) {
        Message msg = null;

        switch (type) {
            case -44:
                msg = new TcpCommunicationSpi.HandshakeMessage2();

                break;

            case -43:
                msg = new IgniteIoTestMessage();

                break;

            case -42:
                msg = new HadoopDirectShuffleMessage();

                break;

            case -41:
                msg = new HadoopShuffleFinishResponse();

                break;

            case -40:
                msg = new HadoopShuffleFinishRequest();

                break;

            case -39:
                msg = new HadoopJobId();

                break;

            case -38:
                msg = new HadoopShuffleAck();

                break;

            case -37:
                msg = new HadoopShuffleMessage();

                break;

            case -36:
                msg = new GridDhtAtomicSingleUpdateRequest();

                break;

            case -27:
                msg = new GridDhtTxOnePhaseCommitAckRequest();

                break;

            case -26:
                msg = new TxLockList();

                break;

            case -25:
                msg = new TxLock();

                break;

            case -24:
                msg = new TxLocksRequest();

                break;

            case -23:
                msg = new TxLocksResponse();

                break;

            case TcpCommunicationSpi.NODE_ID_MSG_TYPE:
                msg = new TcpCommunicationSpi.NodeIdMessage();

                break;

            case TcpCommunicationSpi.RECOVERY_LAST_ID_MSG_TYPE:
                msg = new TcpCommunicationSpi.RecoveryLastReceivedMessage();

                break;

            case TcpCommunicationSpi.HANDSHAKE_MSG_TYPE:
                msg = new TcpCommunicationSpi.HandshakeMessage();

                break;

            case 0:
                msg = new GridJobCancelRequest();

                break;

            case 1:
                msg = new GridJobExecuteRequest();

                break;

            case 2:
                msg = new GridJobExecuteResponse();

                break;

            case 3:
                msg = new GridJobSiblingsRequest();

                break;

            case 4:
                msg = new GridJobSiblingsResponse();

                break;

            case 5:
                msg = new GridTaskCancelRequest();

                break;

            case 6:
                msg = new GridTaskSessionRequest();

                break;

            case 7:
                msg = new GridCheckpointRequest();

                break;

            case 8:
                msg = new GridIoMessage();

                break;

            case 9:
                msg = new GridIoUserMessage();

                break;

            case 10:
                msg = new GridDeploymentInfoBean();

                break;

            case 11:
                msg = new GridDeploymentRequest();

                break;

            case 12:
                msg = new GridDeploymentResponse();

                break;

            case 13:
                msg = new GridEventStorageMessage();

                break;

            case 14:
                msg = new GridCacheEvictionRequest();

                break;

            case 15:
                msg = new GridCacheEvictionResponse();

                break;

            case 16:
                msg = new GridCacheTxRecoveryRequest();

                break;

            case 17:
                msg = new GridCacheTxRecoveryResponse();

                break;

            case 20:
                msg = new GridCacheTtlUpdateRequest();

                break;

            case 21:
                msg = new GridDistributedLockRequest();

                break;

            case 22:
                msg = new GridDistributedLockResponse();

                break;

            case 23:
                msg = new GridDistributedTxFinishRequest();

                break;

            case 24:
                msg = new GridDistributedTxFinishResponse();

                break;

            case 25:
                msg = new GridDistributedTxPrepareRequest();

                break;

            case 26:
                msg = new GridDistributedTxPrepareResponse();

                break;

            case 27:
                msg = new GridDistributedUnlockRequest();

                break;

            case 28:
                msg = new GridDhtAffinityAssignmentRequest();

                break;

            case 29:
                msg = new GridDhtAffinityAssignmentResponse();

                break;

            case 30:
                msg = new GridDhtLockRequest();

                break;

            case 31:
                msg = new GridDhtLockResponse();

                break;

            case 32:
                msg = new GridDhtTxFinishRequest();

                break;

            case 33:
                msg = new GridDhtTxFinishResponse();

                break;

            case 34:
                msg = new GridDhtTxPrepareRequest();

                break;

            case 35:
                msg = new GridDhtTxPrepareResponse();

                break;

            case 36:
                msg = new GridDhtUnlockRequest();

                break;

            case 37:
                msg = new GridDhtAtomicDeferredUpdateResponse();

                break;

            case 38:
                msg = new GridDhtAtomicUpdateRequest();

                break;

            case 39:
                msg = new GridDhtAtomicUpdateResponse();

                break;

            case 40:
                msg = new GridNearAtomicFullUpdateRequest();

                break;

            case 41:
                msg = new GridNearAtomicUpdateResponse();

                break;

            case 42:
                msg = new GridDhtForceKeysRequest();

                break;

            case 43:
                msg = new GridDhtForceKeysResponse();

                break;

            case 44:
                msg = new GridDhtPartitionDemandMessage();

                break;

            case 45:
                msg = new GridDhtPartitionSupplyMessage();

                break;

            case 46:
                msg = new GridDhtPartitionsFullMessage();

                break;

            case 47:
                msg = new GridDhtPartitionsSingleMessage();

                break;

            case 48:
                msg = new GridDhtPartitionsSingleRequest();

                break;

            case 49:
                msg = new GridNearGetRequest();

                break;

            case 50:
                msg = new GridNearGetResponse();

                break;

            case 51:
                msg = new GridNearLockRequest();

                break;

            case 52:
                msg = new GridNearLockResponse();

                break;

            case 53:
                msg = new GridNearTxFinishRequest();

                break;

            case 54:
                msg = new GridNearTxFinishResponse();

                break;

            case 55:
                msg = new GridNearTxPrepareRequest();

                break;

            case 56:
                msg = new GridNearTxPrepareResponse();

                break;

            case 57:
                msg = new GridNearUnlockRequest();

                break;

            case 58:
                msg = new GridCacheQueryRequest();

                break;

            case 59:
                msg = new GridCacheQueryResponse();

                break;

            case 60:
                msg = new GridClockDeltaSnapshotMessage();

                break;

            case 61:
                msg = new GridContinuousMessage();

                break;

            case 62:
                msg = new DataStreamerRequest();

                break;

            case 63:
                msg = new DataStreamerResponse();

                break;

            case 64:
                msg = new IgfsAckMessage();

                break;

            case 65:
                msg = new IgfsBlockKey();

                break;

            case 66:
                msg = new IgfsBlocksMessage();

                break;

            case 67:
                msg = new IgfsDeleteMessage();

                break;

            case 68:
                msg = new IgfsFileAffinityRange();

                break;

            case 69:
                msg = new IgfsFragmentizerRequest();

                break;

            case 70:
                msg = new IgfsFragmentizerResponse();

                break;

            case 71:
                msg = new IgfsSyncMessage();

                break;

            case 76:
                msg = new GridTaskResultRequest();

                break;

            case 77:
                msg = new GridTaskResultResponse();

                break;

            case 82:
                msg = new JobStealingRequest();

                break;

            case 83:
                msg = new GridClockDeltaVersion();

                break;

            case 84:
                msg = new GridByteArrayList();

                break;

            case 85:
                msg = new GridLongList();

                break;

            case 86:
                msg = new GridCacheVersion();

                break;

            case 87:
                msg = new GridDhtPartitionExchangeId();

                break;

            case 88:
                msg = new GridCacheReturn();

                break;

            case 89:
                msg = new CacheObjectImpl();

                break;

            case 90:
                msg = new KeyCacheObjectImpl();

                break;

            case 91:
                msg = new GridCacheEntryInfo();

                break;

            case 92:
                msg = new CacheEntryInfoCollection();

                break;

            case 93:
                msg = new CacheInvokeDirectResult();

                break;

            case 94:
                msg = new IgniteTxKey();

                break;

            case 95:
                msg = new DataStreamerEntry();

                break;

            case 96:
                msg = new CacheContinuousQueryEntry();

                break;

            case 97:
                msg = new CacheEvictionEntry();

                break;

            case 98:
                msg = new CacheEntryPredicateContainsValue();

                break;

            case 99:
                msg = new CacheEntrySerializablePredicate();

                break;

            case 100:
                msg = new IgniteTxEntry();

                break;

            case 101:
                msg = new TxEntryValueHolder();

                break;

            case 102:
                msg = new CacheVersionedValue();

                break;

            case 103:
                msg = new GridCacheRawVersionedEntry<>();

                break;

            case 104:
                msg = new GridCacheVersionEx();

                break;

            case 105:
                msg = new CacheObjectByteArrayImpl();

                break;

            case 106:
                msg = new GridQueryCancelRequest();

                break;

            case 107:
                msg = new GridQueryFailResponse();

                break;

            case 108:
                msg = new GridQueryNextPageRequest();

                break;

            case 109:
                msg = new GridQueryNextPageResponse();

                break;

            case 110:
                msg = new GridQueryRequest();

                break;

            case 111:
                msg = new AffinityTopologyVersion();

                break;

            case 112:
                msg = new GridCacheSqlQuery();

                break;

            case 113:
                msg = new BinaryObjectImpl();

                break;

            case 114:
                msg = new GridDhtPartitionSupplyMessageV2();

                break;

            case 115:
                msg = new UUIDCollectionMessage();

                break;

            case 116:
                msg = new GridNearSingleGetRequest();

                break;

            case 117:
                msg = new GridNearSingleGetResponse();

                break;

            case 118:
                msg = new CacheContinuousQueryBatchAck();

                break;

            case 119:
                msg = new BinaryEnumObjectImpl();

                break;

            case 124:
                msg = new GridMessageCollection<>();

                break;

            case 125:
                msg = new GridNearAtomicSingleUpdateRequest();

                break;

            case 126:
                msg = new GridNearAtomicSingleUpdateInvokeRequest();

                break;

            case 127:
                msg = new GridNearAtomicSingleUpdateFilterRequest();

                break;

            // [-3..119] [124..127] [-36..-44]- this
            // [120..123] - DR
            // [-4..-22, -30..-35] - SQL
            default:
                if (ext != null) {
                    for (MessageFactory factory : ext) {
                        msg = factory.create(type);

                        if (msg != null)
                            break;
                    }
                }

                if (msg == null) {
                    IgniteOutClosure<Message> c = CUSTOM.get(type);

                    if (c != null)
                        msg = c.apply();
                }
        }

        if (msg == null)
            throw new IgniteException("Invalid message type: " + type);

        return msg;
    }

    /**
     * Registers factory for custom message. Used for test purposes.
     *
     * @param type Message type.
     * @param c Message producer.
     */
    public static void registerCustom(byte type, IgniteOutClosure<Message> c) {
        assert c != null;

        CUSTOM.put(type, c);
    }
}
