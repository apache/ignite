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

package org.apache.ignite.internal.direct;

import org.apache.ignite.internal.*;
import org.apache.ignite.internal.managers.checkpoint.*;
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.internal.managers.deployment.*;
import org.apache.ignite.internal.managers.eventstorage.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.processors.clock.*;
import org.apache.ignite.internal.processors.continuous.*;
import org.apache.ignite.internal.processors.dataload.*;
import org.apache.ignite.internal.processors.fs.*;
import org.apache.ignite.internal.processors.rest.client.message.*;
import org.apache.ignite.internal.processors.rest.handlers.task.*;
import org.apache.ignite.internal.processors.rest.protocols.tcp.*;
import org.apache.ignite.internal.processors.streamer.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.apache.ignite.spi.collision.jobstealing.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.jdk8.backport.*;

import java.util.*;

/**
 * Communication message factory.
 */
public class GridTcpCommunicationMessageFactory {
    /** Common message producers. */
    private static final GridTcpCommunicationMessageProducer[] COMMON = new GridTcpCommunicationMessageProducer[83];

    /**
     * Custom messages registry. Used for test purposes.
     */
    private static final Map<Byte, GridTcpCommunicationMessageProducer> CUSTOM = new ConcurrentHashMap8<>();

    /** */
    public static final int MAX_COMMON_TYPE = 88;

    static {
        registerCommon(new GridTcpCommunicationMessageProducer() {
            @Override public MessageAdapter create(byte type) {
                switch (type) {
                    case 0:
                        return new GridJobCancelRequest();

                    case 1:
                        return new GridJobExecuteRequest();

                    case 2:
                        return new GridJobExecuteResponse();

                    case 3:
                        return new GridJobSiblingsRequest();

                    case 4:
                        return new GridJobSiblingsResponse();

                    case 5:
                        return new GridTaskCancelRequest();

                    case 6:
                        return new GridTaskSessionRequest();

                    case 7:
                        return new GridCheckpointRequest();

                    case 8:
                        return new GridIoMessage();

                    case 9:
                        return new GridIoUserMessage();

                    case 10:
                        return new GridDeploymentInfoBean();

                    case 11:
                        return new GridDeploymentRequest();

                    case 12:
                        return new GridDeploymentResponse();

                    case 13:
                        return new GridEventStorageMessage();

                    case 14:
                        return new GridCacheEvictionRequest();

                    case 15:
                        return new GridCacheEvictionResponse();

                    case 16:
                        return new GridCacheOptimisticCheckPreparedTxRequest();

                    case 17:
                        return new GridCacheOptimisticCheckPreparedTxResponse();

                    case 18:
                        return new GridCachePessimisticCheckCommittedTxRequest();

                    case 19:
                        return new GridCachePessimisticCheckCommittedTxResponse();

                    case 20:
                        return new GridCacheTtlUpdateRequest();

                    case 21:
                        return new GridDistributedLockRequest();

                    case 22:
                        return new GridDistributedLockResponse();

                    case 23:
                        return new GridDistributedTxFinishRequest();

                    case 24:
                        return new GridDistributedTxFinishResponse();

                    case 25:
                        return new GridDistributedTxPrepareRequest();

                    case 26:
                        return new GridDistributedTxPrepareResponse();

                    case 27:
                        return new GridDistributedUnlockRequest();

                    case 28:
                        return new GridDhtAffinityAssignmentRequest();

                    case 29:
                        return new GridDhtAffinityAssignmentResponse();

                    case 30:
                        return new GridDhtLockRequest();

                    case 31:
                        return new GridDhtLockResponse();

                    case 32:
                        return new GridDhtTxFinishRequest();

                    case 33:
                        return new GridDhtTxFinishResponse();

                    case 34:
                        return new GridDhtTxPrepareRequest();

                    case 35:
                        return new GridDhtTxPrepareResponse();

                    case 36:
                        return new GridDhtUnlockRequest();

                    case 37:
                        return new GridDhtAtomicDeferredUpdateResponse();

                    case 38:
                        return new GridDhtAtomicUpdateRequest();

                    case 39:
                        return new GridDhtAtomicUpdateResponse();

                    case 40:
                        return new GridNearAtomicUpdateRequest();

                    case 41:
                        return new GridNearAtomicUpdateResponse();

                    case 42:
                        return new GridDhtForceKeysRequest();

                    case 43:
                        return new GridDhtForceKeysResponse();

                    case 44:
                        return new GridDhtPartitionDemandMessage();

                    case 45:
                        return new GridDhtPartitionSupplyMessage();

                    case 46:
                        return new GridDhtPartitionsFullMessage();

                    case 47:
                        return new GridDhtPartitionsSingleMessage();

                    case 48:
                        return new GridDhtPartitionsSingleRequest();

                    case 49:
                        return new GridNearGetRequest();

                    case 50:
                        return new GridNearGetResponse();

                    case 51:
                        return new GridNearLockRequest();

                    case 52:
                        return new GridNearLockResponse();

                    case 53:
                        return new GridNearTxFinishRequest();

                    case 54:
                        return new GridNearTxFinishResponse();

                    case 55:
                        return new GridNearTxPrepareRequest();

                    case 56:
                        return new GridNearTxPrepareResponse();

                    case 57:
                        return new GridNearUnlockRequest();

                    case 58:
                        return new GridCacheQueryRequest();

                    case 59:
                        return new GridCacheQueryResponse();

                    case 60:
                        return new GridClockDeltaSnapshotMessage();

                    case 61:
                        return new GridContinuousMessage();

                    case 62:
                        return new GridDataLoadRequest();

                    case 63:
                        return new GridDataLoadResponse();

                    case 64:
                        return new GridGgfsAckMessage();

                    case 65:
                        return new GridGgfsBlockKey();

                    case 66:
                        return new GridGgfsBlocksMessage();

                    case 67:
                        return new GridGgfsDeleteMessage();

                    case 68:
                        return new GridGgfsFileAffinityRange();

                    case 69:
                        return new GridGgfsFragmentizerRequest();

                    case 70:
                        return new GridGgfsFragmentizerResponse();

                    case 71:
                        return new GridGgfsSyncMessage();

                    case 72:
                        return new GridClientHandshakeRequestWrapper();

                    case 73:
                        return new GridClientHandshakeResponseWrapper();

                    case 74:
                        return new GridClientMessageWrapper();

                    case 75:
                        return new GridClientPingPacketWrapper();

                    case 76:
                        return new GridTaskResultRequest();

                    case 77:
                        return new GridTaskResultResponse();

                    case 78:
                        return new GridMemcachedMessageWrapper();

                    case 79:
                        return new GridStreamerCancelRequest();

                    case 80:
                        return new GridStreamerExecutionRequest();

                    case 81:
                        return new GridStreamerResponse();

                    case 82:
                        return new JobStealingRequest();

                    case 83:
                        return new GridClockDeltaVersion();

                    case 84:
                        return new GridByteArrayList();

                    case 85:
                        return new GridLongList();

                    case 86:
                        return new GridCacheVersion();

                    case 87:
                        return new GridDhtPartitionExchangeId();

                    case 88:
                        return new GridCacheValueBytes();

                    default:
                        assert false : "Invalid message type.";

                        return null;
                }
            }
        },  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
           20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
           40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59,
           60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79,
           80, 81, 82, 83, 84, 85, 86, 87, 88);
    }

    /**
     * @param type Message type.
     * @return New message.
     */
    public static MessageAdapter create(byte type) {
        if (type == TcpCommunicationSpi.NODE_ID_MSG_TYPE)
            return new TcpCommunicationSpi.NodeIdMessage();
        else if (type == TcpCommunicationSpi.RECOVERY_LAST_ID_MSG_TYPE)
            return new TcpCommunicationSpi.RecoveryLastReceivedMessage();
        else if (type == TcpCommunicationSpi.HANDSHAKE_MSG_TYPE)
            return new TcpCommunicationSpi.HandshakeMessage();
        else
            return create0(type);
    }

    /**
     * @param type Message type.
     * @return New message.
     */
    private static MessageAdapter create0(byte type) {
        if (type >= 0 && type < COMMON.length) {
            GridTcpCommunicationMessageProducer producer = COMMON[type];

            if (producer != null)
                return producer.create(type);
            else
                throw new IllegalStateException("Common message type producer is not registered: " + type);
        }
        else {
            GridTcpCommunicationMessageProducer c = CUSTOM.get(type);

            if (c != null)
                return c.create(type);
            else
                throw new IllegalStateException("Custom message type producer is not registered: " + type);
        }
    }

    /**
     * Register message producer for common message type.
     *
     * @param producer Producer.
     * @param types Types applicable for this producer.
     */
    public static void registerCommon(GridTcpCommunicationMessageProducer producer, int... types) {
        for (int type : types) {
            assert type >= 0 && type < COMMON.length : "Common type being registered is out of common messages " +
                "array length: " + type;

            COMMON[type] = producer;
        }
    }

    /**
     * Registers factory for custom message. Used for test purposes.
     *
     * @param producer Message producer.
     * @param type Message type.
     */
    public static void registerCustom(GridTcpCommunicationMessageProducer producer, byte type) {
        assert producer != null;

        CUSTOM.put(type, producer);
    }

    /**
     * @return Common message producers.
     */
    public static GridTcpCommunicationMessageProducer[] commonProducers() {
        return COMMON;
    }
}
