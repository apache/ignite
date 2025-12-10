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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.communication.TransactionIsolationMessage;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

/**
 * Lock request message.
 */
public class GridDistributedLockRequest extends GridDistributedBaseMessage {
    /** Skip store flag bit mask. */
    private static final int SKIP_STORE_FLAG_MASK = 0x01;

    /** Keep binary flag. */
    private static final int KEEP_BINARY_FLAG_MASK = 0x02;

    /** */
    private static final int STORE_USED_FLAG_MASK = 0x04;

    /** */
    private static final int SKIP_READ_THROUGH_FLAG_MASK = 0x08;

    /** Sender node ID. */
    @Order(7)
    private UUID nodeId;

    /** Near transaction version. */
    @Order(value = 8, method = "nearXidVersion")
    private GridCacheVersion nearXidVer;

    /** Thread ID. */
    @Order(9)
    private long threadId;

    /** Future ID. */
    @Order(value = 10, method = "futureId")
    private IgniteUuid futId;

    /** Max wait timeout. */
    @Order(11)
    private long timeout;

    /** Indicates whether lock is obtained within a scope of transaction. */
    @Order(value = 12, method = "inTx")
    private boolean isInTx;

    /** Invalidate flag for transactions. */
    @Order(13)
    private boolean isInvalidate;

    /** Indicates whether implicit lock so for read or write operation. */
    @Order(value = 14, method = "txRead")
    private boolean isRead;

    /** Transaction isolation message. */
    @Order(15)
    private TransactionIsolationMessage isolation;

    /** Key bytes for keys to lock. */
    @Order(16)
    private List<KeyCacheObject> keys;

    /** Array indicating whether value should be returned for a key. */
    @Order(value = 17, method = "returnValues")
    @GridToStringInclude
    private boolean[] retVals;

    /** Key-bytes index. */
    protected int idx;

    /** Key count. */
    @Order(18)
    private int txSize;

    /** Additional flags. */
    @Order(19)
    private byte flags;

    /**
     * Empty constructor.
     */
    public GridDistributedLockRequest() {
        /* No-op. */
    }

    /**
     * @param cacheId Cache ID.
     * @param nodeId Node ID.
     * @param nearXidVer Near transaction ID.
     * @param threadId Thread ID.
     * @param futId Future ID.
     * @param lockVer Cache version.
     * @param isInTx {@code True} if implicit transaction lock.
     * @param isRead Indicates whether implicit lock is for read or write operation.
     * @param isolation Transaction isolation.
     * @param isInvalidate Invalidation flag.
     * @param timeout Lock timeout.
     * @param keyCnt Number of keys.
     * @param txSize Expected transaction size.
     * @param skipStore Skip store flag.
     */
    public GridDistributedLockRequest(
        int cacheId,
        UUID nodeId,
        @Nullable GridCacheVersion nearXidVer,
        long threadId,
        IgniteUuid futId,
        GridCacheVersion lockVer,
        boolean isInTx,
        boolean isRead,
        TransactionIsolation isolation,
        boolean isInvalidate,
        long timeout,
        int keyCnt,
        int txSize,
        boolean skipStore,
        boolean skipReadThrough,
        boolean keepBinary
    ) {
        super(lockVer, keyCnt, false);

        assert keyCnt > 0;
        assert futId != null;
        assert !isInTx || isolation != null;

        this.cacheId = cacheId;
        this.nodeId = nodeId;
        this.nearXidVer = nearXidVer;
        this.threadId = threadId;
        this.futId = futId;
        this.isInTx = isInTx;
        this.isRead = isRead;
        this.isolation = new TransactionIsolationMessage(isolation);
        this.isInvalidate = isInvalidate;
        this.timeout = timeout;
        this.txSize = txSize;

        retVals = new boolean[keyCnt];

        skipStore(skipStore);
        skipReadThrough(skipReadThrough);
        keepBinary(keepBinary);
    }

    /**
     * @return Node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @param nodeId Node ID.
     */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @return Near transaction ID.
     */
    public GridCacheVersion nearXidVersion() {
        return nearXidVer;
    }

    /**
     * @param nearXidVer Near transaction ID.
     */
    public void nearXidVersion(GridCacheVersion nearXidVer) {
        this.nearXidVer = nearXidVer;
    }

    /**
     * @return Owner node thread ID.
     */
    public long threadId() {
        return threadId;
    }

    /**
     * @param threadId Owner node thread ID.
     */
    public void threadId(long threadId) {
        this.threadId = threadId;
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @param futId Future ID.
     */
    public void futureId(IgniteUuid futId) {
        this.futId = futId;
    }

    /**
     * @return {@code True} if implicit transaction lock.
     */
    public boolean inTx() {
        return isInTx;
    }

    /**
     * @param isInTx {@code True} if implicit transaction lock.
     */
    public void inTx(boolean isInTx) {
        this.isInTx = isInTx;
    }

    /**
     * @return Invalidate flag.
     */
    public boolean isInvalidate() {
        return isInvalidate;
    }

    /**
     * @param isInvalidate Invalidate flag.
     */
    public void isInvalidate(boolean isInvalidate) {
        this.isInvalidate = isInvalidate;
    }

    /**
     * @return {@code True} if lock is implicit and for a read operation.
     */
    public boolean txRead() {
        return isRead;
    }

    /**
     * @param isRead {@code True} if lock is implicit and for a read operation.
     */
    public void txRead(boolean isRead) {
        this.isRead = isRead;
    }

    /**
     * @param idx Key index.
     * @return Flag indicating whether a value should be returned.
     */
    public boolean returnValue(int idx) {
        return retVals[idx];
    }

    /**
     * @return Array indicating whether value should be returned for a key.
     */
    public boolean[] returnValues() {
        return retVals;
    }

    /**
     * @param retVals Array indicating whether value should be returned for a key.
     */
    public void returnValues(boolean[] retVals) {
        this.retVals = retVals;
    }

    /**
     * Sets skip store flag value.
     *
     * @param skipStore Skip store flag.
     */
    private void skipStore(boolean skipStore) {
        flags = skipStore ? (byte)(flags | SKIP_STORE_FLAG_MASK) : (byte)(flags & ~SKIP_STORE_FLAG_MASK);
    }

    /**
     * @return Skip store flag.
     */
    public boolean skipStore() {
        return (flags & SKIP_STORE_FLAG_MASK) == 1;
    }

    /**
     * Sets skip store flag value.
     *
     * @param skipReadThrough Skip read-through cache store flag.
     */
    private void skipReadThrough(boolean skipReadThrough) {
        flags = skipReadThrough ? (byte)(flags | SKIP_READ_THROUGH_FLAG_MASK) : (byte)(flags & ~SKIP_READ_THROUGH_FLAG_MASK);
    }

    /**
     * @return Skip store flag.
     */
    public boolean skipReadThrough() {
        return (flags & SKIP_READ_THROUGH_FLAG_MASK) != 0;
    }

    /**
     * @param keepBinary Keep binary flag.
     */
    private void keepBinary(boolean keepBinary) {
        flags = keepBinary ? (byte)(flags | KEEP_BINARY_FLAG_MASK) : (byte)(flags & ~KEEP_BINARY_FLAG_MASK);
    }

    /**
     * @return Keep binary.
     */
    public boolean keepBinary() {
        return (flags & KEEP_BINARY_FLAG_MASK) != 0;
    }

    /**
     * @return Flag indicating whether transaction use cache store.
     */
    public boolean storeUsed() {
        return (flags & STORE_USED_FLAG_MASK) != 0;
    }

    /**
     * @param storeUsed Store used value.
     */
    public void storeUsed(boolean storeUsed) {
        if (storeUsed)
            flags |= STORE_USED_FLAG_MASK;
        else
            flags &= ~STORE_USED_FLAG_MASK;
    }

    /**
     * @return Transaction isolation message.
     */
    public TransactionIsolationMessage isolation() {
        return isolation;
    }

    /**
     * @param isolation Transaction isolation message.
     */
    public void isolation(TransactionIsolationMessage isolation) {
        this.isolation = isolation;
    }

    /**
     * @return Tx size.
     */
    public int txSize() {
        return txSize;
    }

    /**
     * @param txSize Tx size.
     */
    public void txSize(int txSize) {
        this.txSize = txSize;
    }

    /**
     * Adds a key.
     *
     * @param key Key.
     * @param retVal Flag indicating whether value should be returned.
     */
    public void addKeyBytes(KeyCacheObject key, boolean retVal) {
        if (keys == null)
            keys = new ArrayList<>(keysCount());

        keys.add(key);

        retVals[idx] = retVal;

        idx++;
    }

    /**
     * @return Unmarshalled keys.
     */
    public List<KeyCacheObject> keys() {
        return keys;
    }

    /**
     * @param keys Unmarshalled keys.
     */
    public void keys(List<KeyCacheObject> keys) {
        this.keys = keys;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return keys != null && !keys.isEmpty() ? keys.get(0).partition() : -1;
    }

    /**
     * @return Max lock wait time.
     */
    public long timeout() {
        return timeout;
    }

    /**
     * @param timeout Max lock wait time.
     */
    public void timeout(long timeout) {
        this.timeout = timeout;
    }

    /**
     * @return Flags.
     */
    public byte flags() {
        return flags;
    }

    /**
     * @param flags Flags.
     */
    public void flags(byte flags) {
        this.flags = flags;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger messageLogger(GridCacheSharedContext<?, ?> ctx) {
        return ctx.txLockMessageLogger();
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext<?, ?> cctx = ctx.cacheContext(cacheId);

        prepareMarshalCacheObjects(keys, cctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<?, ?> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext<?, ?> cctx = ctx.cacheContext(cacheId);

        finishUnmarshalCacheObjects(keys, cctx, ldr);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 21;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDistributedLockRequest.class, this, "keysCnt", retVals.length,
            "super", super.toString());
    }
}
