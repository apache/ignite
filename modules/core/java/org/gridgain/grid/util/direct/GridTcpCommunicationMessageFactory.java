// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.direct;

import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.checkpoint.*;
import org.gridgain.grid.kernal.managers.communication.*;
import org.gridgain.grid.kernal.managers.deployment.*;
import org.gridgain.grid.kernal.managers.eventstorage.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.atomic.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.preloader.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.kernal.processors.cache.distributed.replicated.preloader.*;
import org.gridgain.grid.kernal.processors.cache.query.*;
import org.gridgain.grid.kernal.processors.clock.*;
import org.gridgain.grid.kernal.processors.continuous.*;
import org.gridgain.grid.kernal.processors.dataload.*;
import org.gridgain.grid.kernal.processors.dr.messages.internal.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.gridgain.grid.kernal.processors.rest.handlers.task.*;
import org.gridgain.grid.kernal.processors.streamer.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.spi.collision.jobstealing.*;
import org.gridgain.grid.spi.communication.tcp.*;
import org.gridgain.grid.util.*;

import java.util.*;

/**
 * Communication message factory.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridTcpCommunicationMessageFactory {
    /**
     * Custom messages registry. Used for test purposes.
     */
    private static final Map<Byte, GridOutClosure<GridTcpCommunicationMessageAdapter>> CUSTOM =
        new ConcurrentHashMap8<>();

    /**
     * @param type Message type.
     * @return New message.
     */
    public static GridTcpCommunicationMessageAdapter create(byte type) {
        return type == GridTcpCommunicationSpi.NODE_ID_MSG_TYPE ? new GridTcpCommunicationSpi.NodeIdMessage() :
            create0(type);
    }

    /**
     * @param type Message type.
     * @return New message.
     */
    private static GridTcpCommunicationMessageAdapter create0(byte type) {
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
                return new GridDhtAtomicDeferredUpdateResponse();

            case 15:
                return new GridDhtAtomicUpdateRequest();

            case 16:
                return new GridDhtAtomicUpdateResponse();

            case 17:
                return new GridNearAtomicUpdateRequest();

            case 18:
                return new GridNearAtomicUpdateResponse();

            case 19:
                return new GridDhtLockRequest();

            case 20:
                return new GridDhtLockResponse();

            case 21:
                return new GridDhtTxFinishRequest();

            case 22:
                return new GridDhtTxFinishResponse();

            case 23:
                return new GridDhtTxPrepareRequest();

            case 24:
                return new GridDhtTxPrepareResponse();

            case 25:
                return new GridDhtUnlockRequest();

            case 26:
                return new GridDhtForceKeysRequest();

            case 27:
                return new GridDhtForceKeysResponse();

            case 28:
                return new GridDhtPartitionDemandMessage();

            case 29:
                return new GridDhtPartitionsFullMessage();

            case 30:
                return new GridDhtPartitionsSingleMessage();

            case 31:
                return new GridDhtPartitionsSingleRequest();

            case 32:
                return new GridDhtPartitionSupplyMessage();

            case 33:
                return new GridCacheOptimisticCheckPreparedTxRequest();

            case 34:
                return new GridCacheOptimisticCheckPreparedTxResponse();

            case 35:
                return new GridCachePessimisticCheckCommittedTxRequest();

            case 36:
                return new GridCachePessimisticCheckCommittedTxResponse();

            case 37:
                return new GridDistributedLockRequest();

            case 38:
                return new GridDistributedLockResponse();

            case 39:
                return new GridDistributedTxFinishRequest();

            case 40:
                return new GridDistributedTxFinishResponse();

            case 41:
                return new GridDistributedTxPrepareRequest();

            case 42:
                return new GridDistributedTxPrepareResponse();

            case 43:
                return new GridDistributedUnlockRequest();

            case 44:
                return new GridNearGetRequest();

            case 45:
                return new GridNearGetResponse();

            case 46:
                return new GridNearLockRequest();

            case 47:
                return new GridNearLockResponse();

            case 48:
                return new GridNearTxFinishRequest();

            case 49:
                return new GridNearTxFinishResponse();

            case 50:
                return new GridNearTxPrepareRequest();

            case 51:
                return new GridNearTxPrepareResponse();

            case 52:
                return new GridNearUnlockRequest();

            case 53:
                return new GridReplicatedForceKeysRequest();

            case 54:
                return new GridReplicatedForceKeysResponse();

            case 55:
                return new GridReplicatedPreloadDemandMessage();

            case 56:
                return new GridReplicatedPreloadSupplyMessage();

            case 57:
                return new GridCacheDgcRequest();

            case 58:
                return new GridCacheDgcResponse();

            case 59:
                return new GridCacheEvictionRequest();

            case 60:
                return new GridCacheEvictionResponse();

            case 61:
                return new GridCacheQueryRequest();

            case 62:
                return new GridCacheQueryResponse();

            case 63:
                return new GridClockDeltaSnapshotMessage();

            case 64:
                return new GridContinuousMessage();

            case 65:
                return new GridDataLoadRequest();

            case 66:
                return new GridDataLoadResponse();

            case 69:
                return new GridDrInternalRequest();

            case 70:
                return new GridDrInternalResponse();

            case 71:
                return new GridGgfsAckMessage();

            case 72:
                return new GridGgfsBlockKey();

            case 73:
                return new GridGgfsBlocksMessage();

            case 74:
                return new GridGgfsDeleteMessage();

            case 75:
                return new GridGgfsFileAffinityRange();

            case 76:
                return new GridGgfsFragmentizerRequest();

            case 77:
                return new GridGgfsFragmentizerResponse();

            case 78:
                return new GridGgfsSyncMessage();

            case 79:
                return new GridTaskResultRequest();

            case 80:
                return new GridTaskResultResponse();

            case 81:
                return new GridStreamerCancelRequest();

            case 82:
                return new GridStreamerExecutionRequest();

            case 83:
                return new GridStreamerResponse();

            case 84:
                return new GridJobStealingRequest();

            default:
                return createCustom(type);
        }
    }

    /**
     * Registers factory for custom message. Used for test purposes.
     *
     * @param type Message type.
     * @param factory Message factory.
     */
    public static void registerCustom(byte type, GridOutClosure<GridTcpCommunicationMessageAdapter> factory) {
        assert factory != null;

        CUSTOM.put(type, factory);
    }

    /**
     * @param type Custom message type.
     * @return New custom message.
     */
    private static GridTcpCommunicationMessageAdapter createCustom(byte type) {
        GridOutClosure<GridTcpCommunicationMessageAdapter> c = CUSTOM.get(type);

        if (c == null)
            throw new IllegalStateException("Invalid message type: " + type);

        return c.apply();
    }
}
