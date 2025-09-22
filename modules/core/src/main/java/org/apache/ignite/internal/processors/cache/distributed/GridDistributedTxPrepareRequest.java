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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxState;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxStateAware;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.UUIDCollectionMessage;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

/**
 * Transaction prepare request for optimistic and eventually consistent
 * transactions.
 */
public class GridDistributedTxPrepareRequest extends GridDistributedBaseMessage implements IgniteTxStateAware {
    /** */
    private static final int NEED_RETURN_VALUE_FLAG_MASK = 0x01;

    /** */
    private static final int INVALIDATE_FLAG_MASK = 0x02;

    /** */
    private static final int ONE_PHASE_COMMIT_FLAG_MASK = 0x04;

    /** */
    private static final int LAST_REQ_FLAG_MASK = 0x08;

    /** */
    private static final int SYSTEM_TX_FLAG_MASK = 0x10;

    /** */
    public static final int STORE_WRITE_THROUGH_FLAG_MASK = 0x20;

    /** Collection to message converter. */
    private static final C1<Collection<UUID>, UUIDCollectionMessage> COL_TO_MSG = UUIDCollectionMessage::new;

    /** Message to collection converter. */
    private static final C1<UUIDCollectionMessage, Collection<UUID>> MSG_TO_COL = UUIDCollectionMessage::uuids;

    /** Thread ID. */
    @Order(7)
    @GridToStringInclude
    private long threadId;

    /** Transaction concurrency. */
    @GridToStringInclude
    private TransactionConcurrency concurrency;

    /** Transaction concurrency ordinal. */
    @Order(value = 8, method = "concurrencyOrdinal")
    private byte concurrencyOrd;

    /** Transaction isolation. */
    @GridToStringInclude
    private TransactionIsolation isolation;

    /** Transaction isolation ordinal. */
    @Order(value = 9, method = "isolationOrdinal")
    private byte isolationOrd;

    /** Commit version for EC transactions. */
    @Order(value = 10, method = "writeVersion")
    @GridToStringInclude
    private GridCacheVersion writeVer;

    /** Transaction timeout. */
    @Order(11)
    @GridToStringInclude
    private long timeout;

    /** Transaction read set. */
    @Order(12)
    @GridToStringInclude
    private Collection<IgniteTxEntry> reads;

    /** Transaction write entries. */
    @Order(13)
    @GridToStringInclude
    private Collection<IgniteTxEntry> writes;

    /** DHT versions to verify. */
    @GridToStringInclude
    private Map<IgniteTxKey, GridCacheVersion> dhtVers;

    /** */
    @Order(value = 14, method = "dhtVersionKeys")
    private Collection<IgniteTxKey> dhtVerKeys;

    /** */
    @Order(value = 15, method = "dhtVersionValues")
    private Collection<GridCacheVersion> dhtVerVals;

    /** Expected transaction size. */
    @Order(16)
    private int txSize;

    /** Transaction nodes mapping (primary node -> related backup nodes). */
    private Map<UUID, Collection<UUID>> txNodes;

    /** Tx nodes direct marshallable message. */
    @Order(value = 17, method = "txNodesMessages")
    private Map<UUID, UUIDCollectionMessage> txNodesMsg;

    /** IO policy. */
    @Order(value = 18, method = "policy")
    private byte plc;

    /** Transient TX state. */
    private IgniteTxState txState;

    /** */
    @Order(19)
    @GridToStringExclude
    private byte flags;

    /** Application attributes. */
    @GridToStringExclude
    private @Nullable Map<String, String> appAttrs;

    /**
     * Empty constructor.
     */
    public GridDistributedTxPrepareRequest() {
        /* No-op. */
    }

    /**
     * @param tx Cache transaction.
     * @param timeout Transactions timeout.
     * @param reads Read entries.
     * @param writes Write entries.
     * @param txNodes Transaction nodes mapping.
     * @param retVal Return value flag.
     * @param last Last request flag.
     * @param onePhaseCommit One phase commit flag.
     * @param addDepInfo Deployment info flag.
     */
    public GridDistributedTxPrepareRequest(
        IgniteInternalTx tx,
        long timeout,
        @Nullable Collection<IgniteTxEntry> reads,
        Collection<IgniteTxEntry> writes,
        Map<UUID, Collection<UUID>> txNodes,
        boolean retVal,
        boolean last,
        boolean onePhaseCommit,
        boolean addDepInfo
    ) {
        super(tx.xidVersion(), 0, addDepInfo);

        writeVer = tx.writeVersion();
        threadId = tx.threadId();
        concurrency = tx.concurrency();
        concurrencyOrd = concurrency != null ? (byte)concurrency.ordinal() : -1;
        isolation = tx.isolation();
        isolationOrd = isolation != null ? (byte)isolation.ordinal() : -1;
        txSize = tx.size();
        plc = tx.ioPolicy();

        this.timeout = timeout;
        this.reads = reads;
        this.writes = writes;
        this.txNodes = txNodes;

        setFlag(tx.system(), SYSTEM_TX_FLAG_MASK);
        setFlag(retVal, NEED_RETURN_VALUE_FLAG_MASK);
        setFlag(tx.isInvalidate(), INVALIDATE_FLAG_MASK);
        setFlag(onePhaseCommit, ONE_PHASE_COMMIT_FLAG_MASK);
        setFlag(last, LAST_REQ_FLAG_MASK);
    }

    /**
     * @return Flag indicating whether transaction needs return value.
     */
    public final boolean needReturnValue() {
        return isFlag(NEED_RETURN_VALUE_FLAG_MASK);
    }

    /**
     * @param retVal Need return value.
     */
    public final void needReturnValue(boolean retVal) {
        setFlag(retVal, NEED_RETURN_VALUE_FLAG_MASK);
    }

    /**
     * @return Transaction nodes mapping.
     */
    public Map<UUID, Collection<UUID>> transactionNodes() {
        return txNodes;
    }

    /**
     * @return System flag.
     */
    public final boolean system() {
        return isFlag(SYSTEM_TX_FLAG_MASK);
    }

    /**
     * @return Flag indicating whether transaction use cache store.
     */
    public boolean storeWriteThrough() {
        return (flags & STORE_WRITE_THROUGH_FLAG_MASK) != 0;
    }

    /**
     * @param storeWriteThrough Store write through value.
     */
    public void storeWriteThrough(boolean storeWriteThrough) {
        if (storeWriteThrough)
            flags |= STORE_WRITE_THROUGH_FLAG_MASK;
        else
            flags &= ~STORE_WRITE_THROUGH_FLAG_MASK;
    }

    /**
     * @return IO policy.
     */
    public byte policy() {
        return plc;
    }

    /**
     * @param plc IO policy.
     */
    public void policy(byte plc) {
        this.plc = plc;
    }

    /**
     * Adds version to be verified on remote node.
     *
     * @param key Key for which version is verified.
     * @param dhtVer DHT version to check.
     */
    public void addDhtVersion(IgniteTxKey key, @Nullable GridCacheVersion dhtVer) {
        if (dhtVers == null)
            dhtVers = new HashMap<>();

        dhtVers.put(key, dhtVer);
    }

    /**
     * @return Map of versions to be verified.
     */
    public Map<IgniteTxKey, GridCacheVersion> dhtVersions() {
        return dhtVers == null ? Collections.emptyMap() : dhtVers;
    }

    /**
     * @return Thread ID.
     */
    public long threadId() {
        return threadId;
    }

    /**
     * @param threadId Thread ID.
     */
    public void threadId(long threadId) {
        this.threadId = threadId;
    }

    /**
     * @return Commit version.
     */
    public GridCacheVersion writeVersion() {
        return writeVer;
    }

    /**
     * @param writeVer Commit version.
     */
    public void writeVersion(GridCacheVersion writeVer) {
        this.writeVer = writeVer;
    }

    /**
     * @return Invalidate flag.
     */
    public boolean isInvalidate() {
        return isFlag(INVALIDATE_FLAG_MASK);
    }

    /**
     * @return Transaction timeout.
     */
    public long timeout() {
        return timeout;
    }

    /**
     * @param timeout Transaction timeout.
     */
    public void timeout(long timeout) {
        this.timeout = timeout;
    }

    /**
     * @return Concurrency.
     */
    public TransactionConcurrency concurrency() {
        return concurrency;
    }

    /**
     * @return Concurrency ordinal.
     */
    public byte concurrencyOrdinal() {
        return concurrencyOrd;
    }

    /**
     * @param concurrencyOrd Concurrency ordinal.
     */
    public void concurrencyOrdinal(byte concurrencyOrd) {
        this.concurrencyOrd = concurrencyOrd;

        concurrency = TransactionConcurrency.fromOrdinal(concurrencyOrd);
    }

    /**
     * @return Isolation level.
     */
    public TransactionIsolation isolation() {
        return isolation;
    }

    /**
     * @return Isolation level ordinal.
     */
    public byte isolationOrdinal() {
        return isolationOrd;
    }

    /**
     * @param isolationOrd Isolation level ordinal.
     */
    public void isolationOrdinal(byte isolationOrd) {
        this.isolationOrd = isolationOrd;

        isolation = TransactionIsolation.fromOrdinal(isolationOrd);
    }

    /**
     * @return Read set.
     */
    public Collection<IgniteTxEntry> reads() {
        return reads;
    }

    /**
     * @return Write entries.
     */
    public Collection<IgniteTxEntry> writes() {
        return writes;
    }

    /**
     * @param reads Reads.
     */
    public void reads(Collection<IgniteTxEntry> reads) {
        this.reads = reads;
    }

    /**
     * @param writes Writes.
     */
    public void writes(Collection<IgniteTxEntry> writes) {
        this.writes = writes;
    }

    /**
     * @return DHT version keys.
     */
    public Collection<IgniteTxKey> dhtVersionKeys() {
        return dhtVerKeys;
    }

    /**
     * @param dhtVerKeys DHT version keys.
     */
    public void dhtVersionKeys(Collection<IgniteTxKey> dhtVerKeys) {
        this.dhtVerKeys = dhtVerKeys;
    }

    /**
     * @return DHT version values.
     */
    public Collection<GridCacheVersion> dhtVersionValues() {
        return dhtVerVals;
    }

    /**
     * @param dhtVerVals DHT version values.
     */
    public void dhtVersionValues(Collection<GridCacheVersion> dhtVerVals) {
        this.dhtVerVals = dhtVerVals;
    }

    /**
     * @return Expected transaction size.
     */
    public int txSize() {
        return txSize;
    }

    /**
     * @param txSize Expected transaction size.
     */
    public void txSize(int txSize) {
        this.txSize = txSize;
    }

    /**
     * @return Tx nodes direct marshallable message.
     */
    public Map<UUID, UUIDCollectionMessage> txNodesMessages() {
        return txNodesMsg;
    }

    /**
     * @param txNodesMsg Tx nodes direct marshallable message.
     */
    public void txNodesMessages(Map<UUID, UUIDCollectionMessage> txNodesMsg) {
        this.txNodesMsg = txNodesMsg;
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

    /**
     * @return One phase commit flag.
     */
    public boolean onePhaseCommit() {
        return isFlag(ONE_PHASE_COMMIT_FLAG_MASK);
    }

    /**
     * @return {@code True} if this is last prepare request for node.
     */
    public boolean last() {
        return isFlag(LAST_REQ_FLAG_MASK);
    }

    /**
     * @return Application attributes, or {@code null} if not set.
     */
    public Map<String, String> applicationAttributes() {
        return appAttrs;
    }

    /**
     * @param appAttrs Application attributes.
     */
    public void applicationAttributes(Map<String, String> appAttrs) {
        this.appAttrs = appAttrs;
    }

    /** {@inheritDoc} */
    @Override public IgniteTxState txState() {
        return txState;
    }

    /** {@inheritDoc} */
    @Override public void txState(IgniteTxState txState) {
        this.txState = txState;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (writes != null)
            marshalTx(writes, ctx);

        if (reads != null)
            marshalTx(reads, ctx);

        if (dhtVers != null && dhtVerKeys == null) {
            for (IgniteTxKey key : dhtVers.keySet()) {
                GridCacheContext<?, ?> cctx = ctx.cacheContext(key.cacheId());

                key.prepareMarshal(cctx);
            }

            dhtVerKeys = dhtVers.keySet();
            dhtVerVals = dhtVers.values();
        }

        if (txNodesMsg == null)
            txNodesMsg = F.viewReadOnly(txNodes, COL_TO_MSG);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<?, ?> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (writes != null)
            unmarshalTx(writes, ctx, ldr);

        if (reads != null)
            unmarshalTx(reads, ctx, ldr);

        if (dhtVerKeys != null && dhtVers == null) {
            assert dhtVerVals != null;
            assert dhtVerKeys.size() == dhtVerVals.size();

            Iterator<IgniteTxKey> keyIt = dhtVerKeys.iterator();
            Iterator<GridCacheVersion> verIt = dhtVerVals.iterator();

            dhtVers = U.newHashMap(dhtVerKeys.size());

            while (keyIt.hasNext()) {
                IgniteTxKey key = keyIt.next();

                key.finishUnmarshal(ctx.cacheContext(key.cacheId()), ldr);

                dhtVers.put(key, verIt.next());
            }
        }

        if (txNodesMsg != null)
            txNodes = F.viewReadOnly(txNodesMsg, MSG_TO_COL);
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo || forceAddDepInfo;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger messageLogger(GridCacheSharedContext<?, ?> ctx) {
        return ctx.txPrepareMessageLogger();
    }

    /**
     * Sets flag mask.
     *
     * @param flag Set or clear.
     * @param mask Mask.
     */
    private void setFlag(boolean flag, int mask) {
        flags = flag ? (byte)(flags | mask) : (byte)(flags & ~mask);
    }

    /**
     * Reags flag mask.
     *
     * @param mask Mask to read.
     * @return Flag value.
     */
    private boolean isFlag(int mask) {
        return (flags & mask) != 0;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 25;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder flags = new StringBuilder();

        if (needReturnValue())
            appendFlag(flags, "retVal");

        if (isInvalidate())
            appendFlag(flags, "invalidate");

        if (onePhaseCommit())
            appendFlag(flags, "onePhase");

        if (last())
            appendFlag(flags, "last");

        if (system())
            appendFlag(flags, "sys");

        return GridToStringBuilder.toString(GridDistributedTxPrepareRequest.class, this,
            "flags", flags.toString(),
            "super", super.toString());
    }
}
