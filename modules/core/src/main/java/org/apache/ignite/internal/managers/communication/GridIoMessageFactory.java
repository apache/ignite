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

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.managers.checkpoint.*;
import org.apache.ignite.internal.managers.deployment.*;
import org.apache.ignite.internal.managers.eventstorage.*;
import org.apache.ignite.internal.portable.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.processors.cache.query.continuous.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.processors.clock.*;
import org.apache.ignite.internal.processors.continuous.*;
import org.apache.ignite.internal.processors.datastreamer.*;
import org.apache.ignite.internal.processors.igfs.*;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.*;
import org.apache.ignite.internal.processors.rest.handlers.task.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.apache.ignite.spi.collision.jobstealing.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.jsr166.*;

import java.util.*;

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
                msg = new GridNearAtomicUpdateRequest();

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
                msg = new PortableObjectImpl();

                break;

            // [-3..112] - this
            // [120..123] - DR
            // [-4..-22] - SQL
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
