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

package org.apache.ignite.internal.processors.cache.transactions;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.processors.cache.store.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.transactions.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;

import javax.cache.expiry.*;
import javax.cache.processor.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static org.apache.ignite.events.EventType.*;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.*;
import static org.apache.ignite.transactions.TransactionConcurrency.*;
import static org.apache.ignite.transactions.TransactionIsolation.*;
import static org.apache.ignite.transactions.TransactionState.*;

/**
 * Managed transaction adapter.
 */
public abstract class IgniteTxAdapter extends GridMetadataAwareAdapter
    implements IgniteInternalTx, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Static logger to avoid re-creation. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    protected static IgniteLogger log;

    /** Transaction ID. */
    @GridToStringInclude
    protected GridCacheVersion xidVer;

    /** Entries write version. */
    @GridToStringInclude
    protected GridCacheVersion writeVer;

    /** Implicit flag. */
    @GridToStringInclude
    protected boolean implicit;

    /** Implicit with one key flag. */
    @GridToStringInclude
    protected boolean implicitSingle;

    /** Local flag. */
    @GridToStringInclude
    protected boolean loc;

    /** Thread ID. */
    @GridToStringInclude
    protected long threadId;

    /** Transaction start time. */
    @GridToStringInclude
    protected long startTime = U.currentTimeMillis();

    /** Node ID. */
    @GridToStringInclude
    protected UUID nodeId;

    /** Transaction counter value at the start of transaction. */
    @GridToStringInclude
    protected GridCacheVersion startVer;

    /** Cache registry. */
    @GridToStringExclude
    protected GridCacheSharedContext<?, ?> cctx;

    /**
     * End version (a.k.a. <tt>'tnc'</tt> or <tt>'transaction number counter'</tt>)
     * assigned to this transaction at the end of write phase.
     */
    @GridToStringInclude
    protected GridCacheVersion endVer;

    /** Isolation. */
    @GridToStringInclude
    protected TransactionIsolation isolation = READ_COMMITTED;

    /** Concurrency. */
    @GridToStringInclude
    protected TransactionConcurrency concurrency = PESSIMISTIC;

    /** Transaction timeout. */
    @GridToStringInclude
    protected long timeout;

    /** Invalidate flag. */
    protected volatile boolean invalidate;

    /** Invalidation flag for system invalidations (not user-based ones). */
    private boolean sysInvalidate;

    /** Internal flag. */
    protected boolean internal;

    /** System transaction flag. */
    private boolean sys;

    /** IO policy. */
    private byte plc;

    /** */
    protected boolean onePhaseCommit;

    /** */
    protected boolean syncCommit;

    /** */
    protected boolean syncRollback;

    /** If this transaction contains transform entries. */
    protected boolean transform;

    /** Commit version. */
    private AtomicReference<GridCacheVersion> commitVer = new AtomicReference<>(null);

    /** Done marker. */
    protected final AtomicBoolean isDone = new AtomicBoolean(false);

    /** */
    private AtomicReference<FinalizationStatus> finalizing = new AtomicReference<>(FinalizationStatus.NONE);

    /** Preparing flag. */
    private AtomicBoolean preparing = new AtomicBoolean();

    /** */
    private Map<Integer, Set<Integer>> invalidParts = new HashMap<>(3);

    /**
     * Transaction state. Note that state is not protected, as we want to
     * always use {@link #state()} and {@link #state(TransactionState)}
     * methods.
     */
    @GridToStringInclude
    private volatile TransactionState state = ACTIVE;

    /** Timed out flag. */
    private volatile boolean timedOut;

    /** */
    protected int txSize;

    /** */
    @GridToStringExclude
    private AtomicReference<GridFutureAdapter<IgniteInternalTx>> finFut = new AtomicReference<>();

    /** Topology version. */
    protected AtomicReference<AffinityTopologyVersion> topVer = new AtomicReference<>(AffinityTopologyVersion.NONE);

    /** Mutex. */
    private final Lock lock = new ReentrantLock();

    /** Lock condition. */
    private final Condition cond = lock.newCondition();

    /** */
    protected Map<UUID, Collection<UUID>> txNodes;

    /** Subject ID initiated this transaction. */
    protected UUID subjId;

    /** Task name hash code. */
    protected int taskNameHash;

    /** Task name. */
    protected String taskName;

    /** Store used flag. */
    protected boolean storeEnabled = true;

    /** */
    @GridToStringExclude
    private TransactionProxyImpl proxy;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    protected IgniteTxAdapter() {
        // No-op.
    }

    /**
     * @param cctx Cache registry.
     * @param xidVer Transaction ID.
     * @param implicit Implicit flag.
     * @param implicitSingle Implicit with one key flag.
     * @param loc Local flag.
     * @param sys System transaction flag.
     * @param plc IO policy.
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @param timeout Timeout.
     * @param txSize Transaction size.
     */
    protected IgniteTxAdapter(
        GridCacheSharedContext<?, ?> cctx,
        GridCacheVersion xidVer,
        boolean implicit,
        boolean implicitSingle,
        boolean loc,
        boolean sys,
        byte plc,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation,
        long timeout,
        boolean invalidate,
        boolean storeEnabled,
        int txSize,
        @Nullable UUID subjId,
        int taskNameHash
    ) {
        assert xidVer != null;
        assert cctx != null;

        this.cctx = cctx;
        this.xidVer = xidVer;
        this.implicit = implicit;
        this.implicitSingle = implicitSingle;
        this.loc = loc;
        this.sys = sys;
        this.plc = plc;
        this.concurrency = concurrency;
        this.isolation = isolation;
        this.timeout = timeout;
        this.invalidate = invalidate;
        this.storeEnabled = storeEnabled;
        this.txSize = txSize;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;

        startVer = cctx.versions().last();

        nodeId = cctx.discovery().localNode().id();

        threadId = Thread.currentThread().getId();

        log = U.logger(cctx.kernalContext(), logRef, this);
    }

    /**
     * @param cctx Cache registry.
     * @param nodeId Node ID.
     * @param xidVer Transaction ID.
     * @param startVer Start version mark.
     * @param threadId Thread ID.
     * @param sys System transaction flag.
     * @param plc IO policy.
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @param timeout Timeout.
     * @param txSize Transaction size.
     */
    protected IgniteTxAdapter(
        GridCacheSharedContext<?, ?> cctx,
        UUID nodeId,
        GridCacheVersion xidVer,
        GridCacheVersion startVer,
        long threadId,
        boolean sys,
        byte plc,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation,
        long timeout,
        int txSize,
        @Nullable UUID subjId,
        int taskNameHash
    ) {
        this.cctx = cctx;
        this.nodeId = nodeId;
        this.threadId = threadId;
        this.xidVer = xidVer;
        this.startVer = startVer;
        this.sys = sys;
        this.plc = plc;
        this.concurrency = concurrency;
        this.isolation = isolation;
        this.timeout = timeout;
        this.txSize = txSize;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;

        implicit = false;
        implicitSingle = false;
        loc = false;

        log = U.logger(cctx.kernalContext(), logRef, this);
    }

    /** {@inheritDoc} */
    @Override public boolean localResult() {
        assert originatingNodeId() != null;

        return cctx.localNodeId().equals(originatingNodeId());
    }

    /**
     * Acquires lock.
     */
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
    protected final void lock() {
        lock.lock();
    }

    /**
     * Releases lock.
     */
    protected final void unlock() {
        lock.unlock();
    }

    /**
     * Signals all waiters.
     */
    protected final void signalAll() {
        cond.signalAll();
    }

    /**
     * Waits for signal.
     *
     * @throws InterruptedException If interrupted.
     */
    protected final void awaitSignal() throws InterruptedException {
        cond.await();
    }

    /**
     * Checks whether near cache should be updated.
     *
     * @return Flag indicating whether near cache should be updated.
     */
    protected boolean updateNearCache(
        GridCacheContext<?, ?> cacheCtx,
        KeyCacheObject key,
        AffinityTopologyVersion topVer
    ) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteTxEntry> optimisticLockEntries() {
        return writeEntries();
    }

    /** {@inheritDoc} */
    @Override public boolean storeEnabled() {
        return storeEnabled;
    }

    /**
     * @param storeEnabled Store enabled flag.
     */
    public void storeEnabled(boolean storeEnabled) {
        this.storeEnabled = storeEnabled;
    }

    /** {@inheritDoc} */
    @Override public boolean system() {
        return sys;
    }

    /** {@inheritDoc} */
    @Override public byte ioPolicy() {
        return plc;
    }

    /** {@inheritDoc} */
    @Override public boolean storeUsed() {
        if (!storeEnabled())
            return false;

        Collection<Integer> cacheIds = activeCacheIds();

        if (!cacheIds.isEmpty()) {
            for (int cacheId : cacheIds) {
                CacheStoreManager store = cctx.cacheContext(cacheId).store();

                if (store.configured())
                    return true;
            }
        }

        return false;
    }

    /**
     * Store manager for current transaction.
     *
     * @return Store manager.
     */
    protected Collection<CacheStoreManager> stores() {
        Collection<Integer> cacheIds = activeCacheIds();

        if (!cacheIds.isEmpty()) {
            Collection<CacheStoreManager> stores = new ArrayList<>(cacheIds.size());

            for (int cacheId : cacheIds) {
                CacheStoreManager store = cctx.cacheContext(cacheId).store();

                if (store.configured())
                    stores.add(store);
            }

            return stores;
        }

        return null;
    }

    /**
     * Uncommits transaction by invalidating all of its entries. Courtesy to minimize inconsistency.
     */
    @SuppressWarnings({"CatchGenericClass"})
    protected void uncommit() {
        for (IgniteTxEntry e : writeMap().values()) {
            try {
                GridCacheEntryEx Entry = e.cached();

                if (e.op() != NOOP)
                    Entry.invalidate(null, xidVer);
            }
            catch (Throwable t) {
                U.error(log, "Failed to invalidate transaction entries while reverting a commit.", t);

                if (t instanceof Error)
                    throw (Error)t;

                break;
            }
        }

        cctx.tm().uncommitTx(this);
    }

    /** {@inheritDoc} */
    @Override public UUID otherNodeId() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public UUID subjectId() {
        if (subjId != null)
            return subjId;

        return originatingNodeId();
    }

    /** {@inheritDoc} */
    @Override public int taskNameHash() {
        return taskNameHash;
    }

    /** {@inheritDoc} */
    @Override public AffinityTopologyVersion topologyVersion() {
        AffinityTopologyVersion res = topVer.get();

        if (res.equals(AffinityTopologyVersion.NONE))
            return cctx.exchange().topologyVersion();

        return res;
    }

    /** {@inheritDoc} */
    @Override public AffinityTopologyVersion topologyVersionSnapshot() {
        AffinityTopologyVersion ret = topVer.get();

        return AffinityTopologyVersion.NONE.equals(ret) ? null : ret;
    }

    /** {@inheritDoc} */
    @Override public AffinityTopologyVersion topologyVersion(AffinityTopologyVersion topVer) {
        this.topVer.compareAndSet(AffinityTopologyVersion.NONE, topVer);

        return this.topVer.get();
    }

    /** {@inheritDoc} */
    @Override public void onRemap(AffinityTopologyVersion topVer) {
        assert false : this;
    }

    /** {@inheritDoc} */
    @Override public boolean hasTransforms() {
        return transform;
    }

    /** {@inheritDoc} */
    @Override public boolean markPreparing() {
        return preparing.compareAndSet(false, true);
    }

    /**
     * @return {@code True} if marked.
     */
    @Override public boolean markFinalizing(FinalizationStatus status) {
        boolean res;

        switch (status) {
            case USER_FINISH:
                res = finalizing.compareAndSet(FinalizationStatus.NONE, FinalizationStatus.USER_FINISH);

                break;

            case RECOVERY_WAIT:
                finalizing.compareAndSet(FinalizationStatus.NONE, FinalizationStatus.RECOVERY_WAIT);

                FinalizationStatus cur = finalizing.get();

                res = cur == FinalizationStatus.RECOVERY_WAIT || cur == FinalizationStatus.RECOVERY_FINISH;

                break;

            case RECOVERY_FINISH:
                FinalizationStatus old = finalizing.get();

                res = old != FinalizationStatus.USER_FINISH && finalizing.compareAndSet(old, status);

                break;

            default:
                throw new IllegalArgumentException("Cannot set finalization status: " + status);

        }

        if (res) {
            if (log.isDebugEnabled())
                log.debug("Marked transaction as finalized: " + this);
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Transaction was not marked finalized: " + this);
        }

        return res;
    }

    /**
     * @return Finalization status.
     */
    protected FinalizationStatus finalizationStatus() {
        return finalizing.get();
    }

    /**
     * @return {@code True} if transaction has at least one key enlisted.
     */
    public abstract boolean isStarted();

    /** {@inheritDoc} */
    @Override public int size() {
        return txSize;
    }

    /**
     * @return Logger.
     */
    protected IgniteLogger log() {
        return log;
    }

    /** {@inheritDoc} */
    @Override public boolean near() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean implicit() {
        return implicit;
    }

    /** {@inheritDoc} */
    @Override public boolean implicitSingle() {
        return implicitSingle;
    }

    /** {@inheritDoc} */
    @Override public boolean local() {
        return loc;
    }

    /** {@inheritDoc} */
    @Override public final boolean user() {
        return !implicit() && local() && !dht() && !internal();
    }

    /** {@inheritDoc} */
    @Override public boolean dht() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean colocated() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean replicated() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean enforceSerializable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean syncCommit() {
        return syncCommit;
    }

    /** {@inheritDoc} */
    @Override public boolean syncRollback() {
        return syncRollback;
    }

    /**
     * @param syncCommit Synchronous commit flag.
     */
    public void syncCommit(boolean syncCommit) {
        this.syncCommit = syncCommit;
    }

    /**
     * @param syncRollback Synchronous rollback flag.
     */
    public void syncRollback(boolean syncRollback) {
        this.syncRollback = syncRollback;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid xid() {
        return xidVer.asGridUuid();
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, Set<Integer>> invalidPartitions() {
        return invalidParts;
    }

    /** {@inheritDoc} */
    @Override public void addInvalidPartition(GridCacheContext<?, ?> cacheCtx, int part) {
        Set<Integer> parts = invalidParts.get(cacheCtx.cacheId());

        if (parts == null) {
            parts = new GridLeanSet<>();

            invalidParts.put(cacheCtx.cacheId(), parts);
        }

        parts.add(part);

        if (log.isDebugEnabled())
            log.debug("Added invalid partition for transaction [cache=" + cacheCtx.name() + ", part=" + part +
                ", tx=" + this + ']');
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion ownedVersion(IgniteTxKey key) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public long startTime() {
        return startTime;
    }

    /**
     * Gets remaining allowed transaction time.
     *
     * @return Remaining transaction time.
     */
    @Override public long remainingTime() {
        if (timeout() <= 0)
            return -1;

        long timeLeft = timeout() - (U.currentTimeMillis() - startTime());

        if (timeLeft < 0)
            return 0;

        return timeLeft;
    }

    /**
     * @return Lock timeout.
     */
    protected long lockTimeout() {
        long timeout = remainingTime();

        return timeout < 0 ? 0 : timeout == 0 ? -1 : timeout;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion xidVersion() {
        return xidVer;
    }

    /** {@inheritDoc} */
    @Override public long threadId() {
        return threadId;
    }

    /** {@inheritDoc} */
    @Override public UUID nodeId() {
        return nodeId;
    }

    /** {@inheritDoc} */
    @Override public TransactionIsolation isolation() {
        return isolation;
    }

    /** {@inheritDoc} */
    @Override public TransactionConcurrency concurrency() {
        return concurrency;
    }

    /** {@inheritDoc} */
    @Override public long timeout() {
        return timeout;
    }

    /** {@inheritDoc} */
    @Override public long timeout(long timeout) {
        if (isStarted())
            throw new IllegalStateException("Cannot change timeout after transaction has started: " + this);

        long old = this.timeout;

        this.timeout = timeout;

        return old;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("SimplifiableIfStatement")
    @Override public boolean ownsLock(GridCacheEntryEx entry) throws GridCacheEntryRemovedException {
        GridCacheContext<?, ?> cacheCtx = entry.context();

        IgniteTxEntry txEntry = entry(entry.txKey());

        GridCacheVersion explicit = txEntry == null ? null : txEntry.explicitVersion();

        return local() && !cacheCtx.isDht() ?
            entry.lockedByThread(threadId()) || (explicit != null && entry.lockedBy(explicit)) :
            // If candidate is not there, then lock was explicit.
            // Otherwise, check if entry is owned by version.
            !entry.hasLockCandidate(xidVersion()) || entry.lockedBy(xidVersion());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("SimplifiableIfStatement")
    @Override public boolean ownsLockUnsafe(GridCacheEntryEx entry) {
        GridCacheContext cacheCtx = entry.context();

        IgniteTxEntry txEntry = entry(entry.txKey());

        GridCacheVersion explicit = txEntry == null ? null : txEntry.explicitVersion();

        return local() && !cacheCtx.isDht() ?
            entry.lockedByThreadUnsafe(threadId()) || (explicit != null && entry.lockedByUnsafe(explicit)) :
            // If candidate is not there, then lock was explicit.
            // Otherwise, check if entry is owned by version.
            !entry.hasLockCandidateUnsafe(xidVersion()) || entry.lockedByUnsafe(xidVersion());
    }

    /** {@inheritDoc} */
    @Override public TransactionState state() {
        return state;
    }

    /** {@inheritDoc} */
    @Override public boolean setRollbackOnly() {
        return state(MARKED_ROLLBACK);
    }

    /**
     * @return {@code True} if rollback only flag is set.
     */
    @Override public boolean isRollbackOnly() {
        return state == MARKED_ROLLBACK || state == ROLLING_BACK || state == ROLLED_BACK;
    }

    /** {@inheritDoc} */
    @Override public boolean done() {
        return isDone.get();
    }

    /**
     * @return Commit version.
     */
    @Override public GridCacheVersion commitVersion() {
        initCommitVersion();

        return commitVer.get();
    }

    /**
     * @param commitVer Commit version.
     * @return {@code True} if set to not null value.
     */
    @Override public boolean commitVersion(GridCacheVersion commitVer) {
        return commitVer != null && this.commitVer.compareAndSet(null, commitVer);
    }

    /**
     *
     */
    public void initCommitVersion() {
        if (commitVer.get() == null)
            commitVer.compareAndSet(null, xidVer);
    }

    /**
     *
     */
    @Override public void close() throws IgniteCheckedException {
        TransactionState state = state();

        if (state != ROLLING_BACK && state != ROLLED_BACK && state != COMMITTING && state != COMMITTED)
            rollback();

        awaitCompletion();
    }

    /** {@inheritDoc} */
    @Override public boolean needsCompletedVersions() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void completedVersions(GridCacheVersion base, Collection<GridCacheVersion> committed,
        Collection<GridCacheVersion> txs) {
        /* No-op. */
    }

    /**
     * Awaits transaction completion.
     *
     * @throws IgniteCheckedException If waiting failed.
     */
    protected void awaitCompletion() throws IgniteCheckedException {
        lock();

        try {
            while (!done())
                awaitSignal();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            if (!done())
                throw new IgniteCheckedException("Got interrupted while waiting for transaction to complete: " + this, e);
        }
        finally {
            unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean internal() {
        return internal;
    }

    /**
     * @param key Key.
     * @return {@code True} if key is internal.
     */
    protected boolean checkInternal(IgniteTxKey key) {
        if (key.key().internal()) {
            internal = true;

            return true;
        }

        return false;
    }

    /**
     * @param onePhaseCommit {@code True} if transaction commit should be performed in short-path way.
     */
    public void onePhaseCommit(boolean onePhaseCommit) {
        this.onePhaseCommit = onePhaseCommit;
    }

    /**
     * @return Fast commit flag.
     */
    @Override public boolean onePhaseCommit() {
        return onePhaseCommit;
    }

    /** {@inheritDoc} */
    @Override public boolean optimistic() {
        return concurrency == OPTIMISTIC;
    }

    /** {@inheritDoc} */
    @Override public boolean pessimistic() {
        return concurrency == PESSIMISTIC;
    }

    /** {@inheritDoc} */
    @Override public boolean serializable() {
        return isolation == SERIALIZABLE;
    }

    /** {@inheritDoc} */
    @Override public boolean repeatableRead() {
        return isolation == REPEATABLE_READ;
    }

    /** {@inheritDoc} */
    @Override public boolean readCommitted() {
        return isolation == READ_COMMITTED;
    }

    /** {@inheritDoc} */
    @Override public boolean state(TransactionState state) {
        return state(state, false);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ExternalizableWithoutPublicNoArgConstructor")
    @Override public IgniteInternalFuture<IgniteInternalTx> finishFuture() {
        GridFutureAdapter<IgniteInternalTx> fut = finFut.get();

        if (fut == null) {
            fut = new GridFutureAdapter<IgniteInternalTx>() {
                @Override public String toString() {
                    return S.toString(GridFutureAdapter.class, this, "tx", IgniteTxAdapter.this);
                }
            };

            if (!finFut.compareAndSet(null, fut))
                fut = finFut.get();
        }

        assert fut != null;

        if (isDone.get())
            fut.onDone(this);

        return fut;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteInternalFuture<?> currentPrepareFuture() {
        return null;
    }

    /**
     *
     * @param state State to set.
     * @param timedOut Timeout flag.
     * @return {@code True} if state changed.
     */
    @SuppressWarnings({"TooBroadScope"})
    private boolean state(TransactionState state, boolean timedOut) {
        boolean valid = false;

        TransactionState prev;

        boolean notify = false;

        lock();

        try {
            prev = this.state;

            switch (state) {
                case ACTIVE: {
                    valid = false;

                    break;
                } // Active is initial state and cannot be transitioned to.
                case PREPARING: {
                    valid = prev == ACTIVE;

                    break;
                }
                case PREPARED: {
                    valid = prev == PREPARING;

                    break;
                }
                case COMMITTING: {
                    valid = prev == PREPARED;

                    break;
                }

                case UNKNOWN: {
                    if (isDone.compareAndSet(false, true))
                        notify = true;

                    valid = prev == ROLLING_BACK || prev == COMMITTING;

                    break;
                }

                case COMMITTED: {
                    if (isDone.compareAndSet(false, true))
                        notify = true;

                    valid = prev == COMMITTING;

                    break;
                }

                case ROLLED_BACK: {
                    if (isDone.compareAndSet(false, true))
                        notify = true;

                    valid = prev == ROLLING_BACK;

                    break;
                }

                case MARKED_ROLLBACK: {
                    valid = prev == ACTIVE || prev == PREPARING || prev == PREPARED || prev == COMMITTING;

                    break;
                }

                case ROLLING_BACK: {
                    valid =
                        prev == ACTIVE || prev == MARKED_ROLLBACK || prev == PREPARING ||
                            prev == PREPARED || (prev == COMMITTING && local() && !dht());

                    break;
                }
            }

            if (valid) {
                this.state = state;
                this.timedOut = timedOut;

                if (log.isDebugEnabled())
                    log.debug("Changed transaction state [prev=" + prev + ", new=" + this.state + ", tx=" + this + ']');

                // Notify of state change.
                signalAll();
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("Invalid transaction state transition [invalid=" + state + ", cur=" + this.state +
                        ", tx=" + this + ']');
            }
        }
        finally {
            unlock();
        }

        if (notify) {
            GridFutureAdapter<IgniteInternalTx> fut = finFut.get();

            if (fut != null)
                fut.onDone(this);
        }

        if (valid) {
            // Seal transactions maps.
            if (state != ACTIVE)
                seal();
        }

        return valid;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion startVersion() {
        return startVer;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion endVersion() {
        return endVer;
    }

    /** {@inheritDoc} */
    @Override public void endVersion(GridCacheVersion endVer) {
        this.endVer = endVer;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion writeVersion() {
        return writeVer == null ? commitVersion() : writeVer;
    }

    /** {@inheritDoc} */
    @Override public void writeVersion(GridCacheVersion writeVer) {
        this.writeVer = writeVer;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid timeoutId() {
        return xidVer.asGridUuid();
    }

    /** {@inheritDoc} */
    @Override public long endTime() {
        long endTime = timeout == 0 ? Long.MAX_VALUE : startTime + timeout;

        return endTime > 0 ? endTime : endTime < 0 ? Long.MAX_VALUE : endTime;
    }

    /** {@inheritDoc} */
    @Override public void onTimeout() {
        if (local() && !dht())
            state(MARKED_ROLLBACK, true);
    }

    /** {@inheritDoc} */
    @Override public boolean timedOut() {
        return timedOut;
    }

    /** {@inheritDoc} */
    @Override public void invalidate(boolean invalidate) {
        if (isStarted() && !dht())
            throw new IllegalStateException("Cannot change invalidation flag after transaction has started: " + this);

        this.invalidate = invalidate;
    }

    /** {@inheritDoc} */
    @Override public boolean isInvalidate() {
        return invalidate;
    }

    /** {@inheritDoc} */
    @Override public boolean isSystemInvalidate() {
        return sysInvalidate;
    }

    /** {@inheritDoc} */
    @Override public void systemInvalidate(boolean sysInvalidate) {
        this.sysInvalidate = sysInvalidate;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Map<UUID, Collection<UUID>> transactionNodes() {
        return txNodes;
    }

    /**
     * @param txNodes Transaction nodes.
     */
    public void transactionNodes(Map<UUID, Collection<UUID>> txNodes) {
        this.txNodes = txNodes;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheVersion nearXidVersion() {
        return null;
    }

    /**
     * @param txEntry Entry to process.
     * @param metrics {@code True} if metrics should be updated.
     * @return Tuple containing transformation results.
     * @throws IgniteCheckedException If failed to get previous value for transform.
     * @throws GridCacheEntryRemovedException If entry was concurrently deleted.
     */
    protected IgniteBiTuple<GridCacheOperation, CacheObject> applyTransformClosures(
        IgniteTxEntry txEntry,
        boolean metrics) throws GridCacheEntryRemovedException, IgniteCheckedException {
        GridCacheContext cacheCtx = txEntry.context();

        assert cacheCtx != null;

        if (isSystemInvalidate())
            return F.t(cacheCtx.writeThrough() ? RELOAD : DELETE, null);

        if (F.isEmpty(txEntry.entryProcessors()))
            return F.t(txEntry.op(), txEntry.value());
        else {
            try {
                boolean recordEvt = cctx.gridEvents().isRecordable(EVT_CACHE_OBJECT_READ);

                CacheObject cacheVal = txEntry.hasValue() ? txEntry.value() :
                    txEntry.cached().innerGet(this,
                        /*swap*/false,
                        /*read through*/false,
                        /*fail fast*/true,
                        /*unmarshal*/true,
                        /*metrics*/metrics,
                        /*event*/recordEvt,
                        /*temporary*/true,
                        /*subjId*/subjId,
                        /**closure name */recordEvt ? F.first(txEntry.entryProcessors()).get1() : null,
                        resolveTaskName(),
                        null);

                boolean modified = false;

                Object val = null;

                Object key = null;

                GridCacheVersion ver;

                try {
                    ver = txEntry.cached().version();
                }
                catch (GridCacheEntryRemovedException e) {
                    assert optimistic() : txEntry;

                    if (log.isDebugEnabled())
                        log.debug("Failed to get entry version: [msg=" + e.getMessage() + ']');

                    ver = null;
                }

                for (T2<EntryProcessor<Object, Object, Object>, Object[]> t : txEntry.entryProcessors()) {
                    CacheInvokeEntry<Object, Object> invokeEntry = new CacheInvokeEntry(txEntry.context(),
                        txEntry.key(), key, cacheVal, val, ver);

                    try {
                        EntryProcessor<Object, Object, Object> processor = t.get1();

                        processor.process(invokeEntry, t.get2());

                        val = invokeEntry.getValue();

                        key = invokeEntry.key();
                    }
                    catch (Exception ignore) {
                        // No-op.
                    }

                    modified |= invokeEntry.modified();
                }

                if (modified)
                    cacheVal = cacheCtx.toCacheObject(cacheCtx.unwrapTemporary(val));

                GridCacheOperation op = modified ? (val == null ? DELETE : UPDATE) : NOOP;

                if (op == NOOP) {
                    ExpiryPolicy expiry = cacheCtx.expiryForTxEntry(txEntry);

                    if (expiry != null) {
                        long ttl = CU.toTtl(expiry.getExpiryForAccess());

                        txEntry.ttl(ttl);

                        if (ttl == CU.TTL_ZERO)
                            op = DELETE;
                    }
                }

                return F.t(op, cacheVal);
            }
            catch (GridCacheFilterFailedException e) {
                assert false : "Empty filter failed for innerGet: " + e;

                return null;
            }
        }
    }

    /**
     * @return Resolves task name.
     */
    public String resolveTaskName() {
        if (taskName != null)
            return taskName;

        return (taskName = cctx.kernalContext().task().resolveTaskName(taskNameHash));
    }

    /**
     * Resolve DR conflict.
     *
     * @param op Initially proposed operation.
     * @param txEntry TX entry being updated.
     * @param newVal New value.
     * @param newVer New version.
     * @param old Old entry.
     * @return Tuple with adjusted operation type and conflict context.
     * @throws IgniteCheckedException In case of eny exception.
     * @throws GridCacheEntryRemovedException If entry got removed.
     */
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    protected IgniteBiTuple<GridCacheOperation, GridCacheVersionConflictContext> conflictResolve(
        GridCacheOperation op,
        IgniteTxEntry txEntry,
        CacheObject newVal,
        GridCacheVersion newVer,
        GridCacheEntryEx old)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        assert newVer != null;

        // 1. Calculate TTL and expire time.
        long newTtl = txEntry.ttl();
        long newExpireTime = txEntry.conflictExpireTime();

        // 1.1. If TTL is not changed, then calculate it based on expiry.
        if (newTtl == CU.TTL_NOT_CHANGED) {
            ExpiryPolicy expiry = txEntry.context().expiryForTxEntry(txEntry);

            if (expiry != null) {
                if (op == CREATE)
                    newTtl = CU.toTtl(expiry.getExpiryForCreation());
                else if (op == UPDATE)
                    newTtl = CU.toTtl(expiry.getExpiryForUpdate());
            }
        }

        // 1.2. If TTL is set to zero, then mark operation as "DELETE".
        if (newTtl == CU.TTL_ZERO) {
            op = DELETE;

            newTtl = CU.TTL_ETERNAL;
        }

        // 1.3. If TTL is still not changed, then either use old entry TTL or set it to "ETERNAL".
        if (newTtl == CU.TTL_NOT_CHANGED) {
            if (old.isNewLocked())
                newTtl = CU.TTL_ETERNAL;
            else {
                newTtl = old.rawTtl();
                newExpireTime = old.rawExpireTime();
            }
        }

        // TTL must be resolved at this point.
        assert newTtl != CU.TTL_ZERO && newTtl != CU.TTL_NOT_CHANGED;

        // 1.4 If expire time was not set explicitly, then calculate it.
        if (newExpireTime == CU.EXPIRE_TIME_CALCULATE)
            newExpireTime = CU.toExpireTime(newTtl);

        // Expire time must be resolved at this point.
        assert newExpireTime != CU.EXPIRE_TIME_CALCULATE;

        // Construct old entry info.
        GridCacheVersionedEntryEx oldEntry = old.versionedEntry();

        // Construct new entry info.
        Object newVal0 = CU.value(newVal, txEntry.context(), false);

        GridCacheVersionedEntryEx newEntry = new GridCachePlainVersionedEntry(
            oldEntry.key(),
            newVal0,
            newTtl,
            newExpireTime,
            newVer);

        GridCacheVersionConflictContext ctx = old.context().conflictResolve(oldEntry, newEntry, false);

        if (ctx.isMerge()) {
            Object resVal = ctx.mergeValue();

            if ((op == CREATE || op == UPDATE) && resVal == null)
                op = DELETE;
            else if (op == DELETE && resVal != null)
                op = old.isNewLocked() ? CREATE : UPDATE;
        }

        return F.t(op, ctx);
    }

    /**
     * @param e Transaction entry.
     * @param primaryOnly Flag to include backups into check or not.
     * @return {@code True} if entry is locally mapped as a primary or back up node.
     */
    protected boolean isNearLocallyMapped(IgniteTxEntry e, boolean primaryOnly) {
        GridCacheContext cacheCtx = e.context();

        if (!cacheCtx.isNear())
            return false;

        // Try to take either entry-recorded primary node ID,
        // or transaction node ID from near-local transactions.
        UUID nodeId = e.nodeId() == null ? local() ? this.nodeId :  null : e.nodeId();

        if (nodeId != null && nodeId.equals(cctx.localNodeId()))
            return true;

        GridCacheEntryEx cached = e.cached();

        int part = cached != null ? cached.partition() : cacheCtx.affinity().partition(e.key());

        List<ClusterNode> affNodes = cacheCtx.affinity().nodes(part, topologyVersion());

        e.locallyMapped(F.contains(affNodes, cctx.localNode()));

        if (primaryOnly) {
            ClusterNode primary = F.first(affNodes);

            if (primary == null && !cacheCtx.affinityNode())
                return false;

            assert primary != null : "Primary node is null for affinity nodes: " + affNodes;

            return primary.isLocal();
        }
        else
            return e.locallyMapped();
    }

    /**
     * @param e Entry to evict if it qualifies for eviction.
     * @param primaryOnly Flag to try to evict only on primary node.
     * @return {@code True} if attempt was made to evict the entry.
     * @throws IgniteCheckedException If failed.
     */
    protected boolean evictNearEntry(IgniteTxEntry e, boolean primaryOnly) throws IgniteCheckedException {
        assert e != null;

        if (isNearLocallyMapped(e, primaryOnly)) {
            GridCacheEntryEx cached = e.cached();

            assert cached instanceof GridNearCacheEntry : "Invalid cache entry: " + e;

            if (log.isDebugEnabled())
                log.debug("Evicting dht-local entry from near cache [entry=" + cached + ", tx=" + this + ']');

            if (cached != null && cached.markObsolete(xidVer))
                return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        writeExternalMeta(out);

        out.writeObject(xidVer);
        out.writeBoolean(invalidate);
        out.writeLong(timeout);
        out.writeLong(threadId);
        out.writeLong(startTime);

        U.writeUuid(out, nodeId);

        out.write(isolation.ordinal());
        out.write(concurrency.ordinal());
        out.write(state().ordinal());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        readExternalMeta(in);

        xidVer = (GridCacheVersion)in.readObject();
        invalidate = in.readBoolean();
        timeout = in.readLong();
        threadId = in.readLong();
        startTime = in.readLong();

        nodeId = U.readUuid(in);

        isolation = TransactionIsolation.fromOrdinal(in.read());
        concurrency = TransactionConcurrency.fromOrdinal(in.read());

        state = TransactionState.fromOrdinal(in.read());
    }

    /**
     * Reconstructs object on unmarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of unmarshalling error.
     */
    protected Object readResolve() throws ObjectStreamException {
        return new TxShadow(
            xidVer.asGridUuid(),
            nodeId,
            threadId,
            startTime,
            isolation,
            concurrency,
            invalidate,
            implicit,
            timeout,
            state(),
            isRollbackOnly()
        );
    }

    /** {@inheritDoc} */
    @Override public TransactionProxy proxy() {
        if (proxy == null)
            proxy = new TransactionProxyImpl(this, cctx, false);

        return proxy;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        return o == this || (o instanceof IgniteTxAdapter && xidVer.equals(((IgniteTxAdapter)o).xidVer));
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return xidVer.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(IgniteTxAdapter.class, this,
            "duration", (U.currentTimeMillis() - startTime) + "ms",
            "onePhaseCommit", onePhaseCommit);
    }

    /**
     * Transaction shadow class to be used for deserialization.
     */
    private static class TxShadow implements IgniteInternalTx {
        /** Xid. */
        private final IgniteUuid xid;

        /** Node ID. */
        private final UUID nodeId;

        /** Thread ID. */
        private final long threadId;

        /** Start time. */
        private final long startTime;

        /** Transaction isolation. */
        private final TransactionIsolation isolation;

        /** Concurrency. */
        private final TransactionConcurrency concurrency;

        /** Invalidate flag. */
        private final boolean invalidate;

        /** Timeout. */
        private final long timeout;

        /** State. */
        private final TransactionState state;

        /** Rollback only flag. */
        private final boolean rollbackOnly;

        /** Implicit flag. */
        private final boolean implicit;

        /**
         * @param xid Xid.
         * @param nodeId Node ID.
         * @param threadId Thread ID.
         * @param startTime Start time.
         * @param isolation Isolation.
         * @param concurrency Concurrency.
         * @param invalidate Invalidate flag.
         * @param implicit Implicit flag.
         * @param timeout Transaction timeout.
         * @param state Transaction state.
         * @param rollbackOnly Rollback-only flag.
         */
        TxShadow(IgniteUuid xid, UUID nodeId, long threadId, long startTime, TransactionIsolation isolation,
            TransactionConcurrency concurrency, boolean invalidate, boolean implicit, long timeout,
            TransactionState state, boolean rollbackOnly) {
            this.xid = xid;
            this.nodeId = nodeId;
            this.threadId = threadId;
            this.startTime = startTime;
            this.isolation = isolation;
            this.concurrency = concurrency;
            this.invalidate = invalidate;
            this.implicit = implicit;
            this.timeout = timeout;
            this.state = state;
            this.rollbackOnly = rollbackOnly;
        }

        /** {@inheritDoc} */
        @Override public boolean localResult() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public IgniteUuid xid() {
            return xid;
        }

        /** {@inheritDoc} */
        @Override public UUID nodeId() {
            return nodeId;
        }

        /** {@inheritDoc} */
        @Override public long threadId() {
            return threadId;
        }

        /** {@inheritDoc} */
        @Override public long startTime() {
            return startTime;
        }

        /** {@inheritDoc} */
        @Override public TransactionIsolation isolation() {
            return isolation;
        }

        /** {@inheritDoc} */
        @Override public TransactionConcurrency concurrency() {
            return concurrency;
        }

        /** {@inheritDoc} */
        @Override public boolean isInvalidate() {
            return invalidate;
        }

        /** {@inheritDoc} */
        @Override public boolean implicit() {
            return implicit;
        }

        /** {@inheritDoc} */
        @Override public long timeout() {
            return timeout;
        }

        /** {@inheritDoc} */
        @Override public TransactionState state() {
            return state;
        }

        /** {@inheritDoc} */
        @Override public boolean isRollbackOnly() {
            return rollbackOnly;
        }

        /** {@inheritDoc} */
        @Override public long timeout(long timeout) {
            throw new IllegalStateException("Deserialized transaction can only be used as read-only.");
        }

        /** {@inheritDoc} */
        @Override public boolean setRollbackOnly() {
            throw new IllegalStateException("Deserialized transaction can only be used as read-only.");
        }

        /** {@inheritDoc} */
        @Override public void commit() {
            throw new IllegalStateException("Deserialized transaction can only be used as read-only.");
        }

        /** {@inheritDoc} */
        @Override public void close() {
            throw new IllegalStateException("Deserialized transaction can only be used as read-only.");
        }

        /** {@inheritDoc} */
        @Override public void rollback() {
            throw new IllegalStateException("Deserialized transaction can only be used as read-only.");
        }

        /** {@inheritDoc} */
        @Override public Collection<Integer> activeCacheIds() {
            throw new IllegalStateException("Deserialized transaction can only be used as read-only.");
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object addMeta(int key, Object val) {
            throw new IllegalStateException("Deserialized transaction can only be used as read-only.");
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object removeMeta(int key) {
            throw new IllegalStateException("Deserialized transaction can only be used as read-only.");
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object meta(int key) {
            throw new IllegalStateException("Deserialized transaction can only be used as read-only.");
        }

        /** {@inheritDoc} */
        @Override public int size() {
            throw new IllegalStateException("Deserialized transaction can only be used as read-only.");
        }

        /** {@inheritDoc} */
        @Override public boolean storeEnabled() {
            throw new IllegalStateException("Deserialized transaction can only be used as read-only.");
        }

        /** {@inheritDoc} */
        @Override public boolean storeUsed() {
            throw new IllegalStateException("Deserialized transaction can only be used as read-only.");
        }

        /** {@inheritDoc} */
        @Override public boolean system() {
            throw new IllegalStateException("Deserialized transaction can only be used as read-only.");
        }

        @Override public byte ioPolicy() {
            throw new IllegalStateException("Deserialized transaction can only be used as read-only.");
        }

        /** {@inheritDoc} */
        @Override public AffinityTopologyVersion topologyVersion() {
            throw new IllegalStateException("Deserialized transaction can only be used as read-only.");
        }

        /** {@inheritDoc} */
        @Override public AffinityTopologyVersion topologyVersionSnapshot() {
            throw new IllegalStateException("Deserialized transaction can only be used as read-only.");
        }

        /** {@inheritDoc} */
        @Override public boolean implicitSingle() {
            throw new IllegalStateException("Deserialized transaction can only be used as read-only.");
        }

        /** {@inheritDoc} */
        @Override public AffinityTopologyVersion topologyVersion(AffinityTopologyVersion topVer) {
            throw new IllegalStateException("Deserialized transaction can only be used as read-only.");
        }

        /** {@inheritDoc} */
        @Override public void onRemap(AffinityTopologyVersion topVer) {
            throw new IllegalStateException("Deserialized transaction can only be used as read-only.");
        }

        /** {@inheritDoc} */
        @Override public boolean empty() {
            throw new IllegalStateException("Deserialized transaction can only be used as read-only.");
        }

        /** {@inheritDoc} */
        @Override public boolean markPreparing() {
            throw new IllegalStateException("Deserialized transaction can only be used as read-only.");
        }

        /** {@inheritDoc} */
        @Override public boolean markFinalizing(FinalizationStatus status) {
            throw new IllegalStateException("Deserialized transaction can only be used as read-only.");
        }

        /** {@inheritDoc} */
        @Override public void addInvalidPartition(GridCacheContext cacheCtx, int part) {
            throw new IllegalStateException("Deserialized transaction can only be used as read-only.");
        }

        /** {@inheritDoc} */
        @Override public Map<Integer, Set<Integer>> invalidPartitions() {
            throw new IllegalStateException("Deserialized transaction can only be used as read-only.");
        }

        /** {@inheritDoc} */
        @Nullable @Override public GridCacheVersion ownedVersion(IgniteTxKey key) {
            throw new IllegalStateException("Deserialized transaction can only be used as read-only.");
        }

        /** {@inheritDoc} */
        @Nullable @Override public UUID otherNodeId() {
            throw new IllegalStateException("Deserialized transaction can only be used as read-only.");
        }

        /** {@inheritDoc} */
        @Override public UUID eventNodeId() {
            throw new IllegalStateException("Deserialized transaction can only be used as read-only.");
        }

        /** {@inheritDoc} */
        @Override public UUID originatingNodeId() {
            throw new IllegalStateException("Deserialized transaction can only be used as read-only.");
        }

        /** {@inheritDoc} */
        @Override public Collection<UUID> masterNodeIds() {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public GridCacheVersion nearXidVersion() {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Map<UUID, Collection<UUID>> transactionNodes() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean ownsLock(GridCacheEntryEx entry) throws GridCacheEntryRemovedException {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean ownsLockUnsafe(GridCacheEntryEx entry) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean enforceSerializable() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean near() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean dht() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean colocated() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean local() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean replicated() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public UUID subjectId() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public int taskNameHash() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public boolean user() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean syncCommit() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean syncRollback() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean hasWriteKey(IgniteTxKey key) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public Set<IgniteTxKey> readSet() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Set<IgniteTxKey> writeSet() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Collection<IgniteTxEntry> allEntries() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Collection<IgniteTxEntry> writeEntries() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Collection<IgniteTxEntry> readEntries() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Map<IgniteTxKey, IgniteTxEntry> writeMap() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Map<IgniteTxKey, IgniteTxEntry> readMap() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Collection<IgniteTxEntry> optimisticLockEntries() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void seal() {

        }

        /** {@inheritDoc} */
        @Nullable @Override public IgniteTxEntry entry(IgniteTxKey key) {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public GridTuple<CacheObject> peek(GridCacheContext ctx,
            boolean failFast,
            KeyCacheObject key,
            @Nullable CacheEntryPredicate[] filter) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public GridCacheVersion startVersion() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public GridCacheVersion xidVersion() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public GridCacheVersion commitVersion() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean commitVersion(GridCacheVersion commitVer) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public GridCacheVersion endVersion() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void prepare() throws IgniteCheckedException {

        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<IgniteInternalTx> prepareAsync() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void endVersion(GridCacheVersion endVer) {

        }

        /** {@inheritDoc} */
        @Override public GridCacheVersion writeVersion() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeVersion(GridCacheVersion ver) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<IgniteInternalTx> finishFuture() {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public IgniteInternalFuture<IgniteInternalTx> currentPrepareFuture() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean state(TransactionState state) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public void invalidate(boolean invalidate) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void systemInvalidate(boolean sysInvalidate) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean isSystemInvalidate() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<IgniteInternalTx> rollbackAsync() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<IgniteInternalTx> commitAsync() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean onOwnerChanged(GridCacheEntryEx entry, GridCacheMvccCandidate owner) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean timedOut() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean done() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean optimistic() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean pessimistic() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean readCommitted() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean repeatableRead() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean serializable() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean removed(IgniteTxKey key) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public long remainingTime() throws IgniteTxTimeoutCheckedException {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public Collection<GridCacheVersion> alternateVersions() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean needsCompletedVersions() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public void completedVersions(GridCacheVersion base, Collection committed, Collection rolledback) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean internal() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean onePhaseCommit() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean hasTransforms() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public TransactionProxy proxy() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public IgniteUuid timeoutId() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public long endTime() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return this == o || o instanceof IgniteInternalTx && xid.equals(((IgniteInternalTx)o).xid());
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return xid.hashCode();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TxShadow.class, this);
        }
    }
}
