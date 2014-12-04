/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.apache.ignite.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.timeout.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Committed tx buffer which should be used in synchronous commit mode.
 */
public class GridCachePerThreadTxCommitBuffer<K, V> implements GridCacheTxCommitBuffer<K, V> {
    /** Logger. */
    private IgniteLogger log;

    /** Cache context. */
    private GridCacheSharedContext<K, V> cctx;

    /** Store map. */
    private Map<StoreKey, GridCacheCommittedTxInfo<K, V>> infoMap;

    /**
     * @param cctx Cache context.
     */
    public GridCachePerThreadTxCommitBuffer(GridCacheSharedContext<K, V> cctx) {
        this.cctx = cctx;

        log = cctx.logger(GridCachePerThreadTxCommitBuffer.class);

        int logSize = cctx.txConfig().getPessimisticTxLogSize();

        infoMap = logSize > 0 ?
            new GridBoundedConcurrentLinkedHashMap<StoreKey, GridCacheCommittedTxInfo<K, V>>(logSize) :
            new ConcurrentHashMap8<StoreKey, GridCacheCommittedTxInfo<K, V>>();
    }

    /** {@inheritDoc} */
    @Override public void addCommittedTx(GridCacheTxEx<K, V> tx) {
        long threadId = tx.threadId();

        StoreKey key = new StoreKey(tx.eventNodeId(), threadId);

        if (log.isDebugEnabled())
            log.debug("Adding committed transaction [locNodeId=" + cctx.localNodeId() + ", key=" + key +
                ", tx=" + tx + ']');

        infoMap.put(key, new GridCacheCommittedTxInfo<>(tx));
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheCommittedTxInfo<K, V> committedTx(GridCacheVersion originatingTxVer,
        UUID nodeId, long threadId) {
        assert originatingTxVer != null;

        StoreKey key = new StoreKey(nodeId, threadId);

        GridCacheCommittedTxInfo<K, V> txInfo = infoMap.get(key);

        if (log.isDebugEnabled())
            log.debug("Got committed transaction info by key [locNodeId=" + cctx.localNodeId() +
                ", key=" + key + ", originatingTxVer=" + originatingTxVer + ", txInfo=" + txInfo + ']');

        if (txInfo == null || !originatingTxVer.equals(txInfo.originatingTxId()))
            return null;

        return txInfo;
    }

    /**
     * @param nodeId Left node ID.
     */
    @Override public void onNodeLeft(UUID nodeId) {
        // Clear all node's records after clear interval.
        cctx.kernalContext().timeout().addTimeoutObject(
            new NodeLeftTimeoutObject(cctx.txConfig().getPessimisticTxLogLinger(), nodeId));
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return infoMap.size();
    }

    /**
     * Store key.
     */
    private static class StoreKey {
        /** Node ID which started transaction. */
        private UUID nodeId;

        /** Thread ID which started transaction. */
        private long threadId;

        /**
         * @param nodeId Node ID.
         * @param threadId Thread ID.
         */
        private StoreKey(UUID nodeId, long threadId) {
            this.nodeId = nodeId;
            this.threadId = threadId;
        }

        /**
         * @return Node ID.
         */
        public UUID nodeId() {
            return nodeId;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            StoreKey storeKey = (StoreKey)o;

            return threadId == storeKey.threadId && nodeId.equals(storeKey.nodeId);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = nodeId.hashCode();

            res = 31 * res + (int)(threadId ^ (threadId >>> 32));

            return res;
        }

        /** {@inheritDoc} */
        public String toString() {
            return S.toString(StoreKey.class, this);
        }
    }

    /**
     * Node left timeout object which will clear all committed records from left node.
     */
    private class NodeLeftTimeoutObject extends GridTimeoutObjectAdapter {
        /** Left node ID. */
        private UUID leftNodeId;

        /**
         * @param timeout Timeout.
         * @param leftNodeId Left node ID.
         */
        protected NodeLeftTimeoutObject(long timeout, UUID leftNodeId) {
            super(timeout);

            this.leftNodeId = leftNodeId;
        }

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            Iterator<StoreKey> it = infoMap.keySet().iterator();

            while (it.hasNext()) {
                StoreKey key = it.next();

                if (leftNodeId.equals(key.nodeId()))
                    it.remove();
            }
        }
    }
}
