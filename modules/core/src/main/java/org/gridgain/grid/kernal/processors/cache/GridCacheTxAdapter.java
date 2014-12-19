/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static org.apache.ignite.events.IgniteEventType.*;
import static org.gridgain.grid.kernal.processors.cache.GridCacheTxEx.FinalizationStatus.*;
import static org.gridgain.grid.kernal.processors.cache.GridCacheUtils.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;
import static org.gridgain.grid.cache.GridCacheTxState.*;
import static org.gridgain.grid.kernal.processors.cache.GridCacheOperation.*;

/**
 * Managed transaction adapter.
 */
public abstract class GridCacheTxAdapter<K, V> extends GridMetadataAwareAdapter
    implements GridCacheTxEx<K, V>, Externalizable {
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
    protected GridCacheSharedContext<K, V> cctx;

    /**
     * End version (a.k.a. <tt>'tnc'</tt> or <tt>'transaction number counter'</tt>)
     * assigned to this transaction at the end of write phase.
     */
    @GridToStringInclude
    protected GridCacheVersion endVer;

    /** Isolation. */
    @GridToStringInclude
    protected GridCacheTxIsolation isolation = READ_COMMITTED;

    /** Concurrency. */
    @GridToStringInclude
    protected GridCacheTxConcurrency concurrency = PESSIMISTIC;

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
    private AtomicReference<FinalizationStatus> finalizing = new AtomicReference<>(NONE);

    /** Preparing flag. */
    private AtomicBoolean preparing = new AtomicBoolean();

    /** */
    private Set<Integer> invalidParts = new GridLeanSet<>();

    /** Recover writes. */
    private Collection<GridCacheTxEntry<K, V>> recoveryWrites;

    /**
     * Transaction state. Note that state is not protected, as we want to
     * always use {@link #state()} and {@link #state(GridCacheTxState)}
     * methods.
     */
    @GridToStringInclude
    private volatile GridCacheTxState state = ACTIVE;

    /** Timed out flag. */
    private volatile boolean timedOut;

    /** */
    protected int txSize;

    /** Group lock key, if any. */
    protected GridCacheTxKey grpLockKey;

    /** */
    @GridToStringExclude
    private AtomicReference<GridFutureAdapter<GridCacheTx>> finFut = new AtomicReference<>();

    /** Topology version. */
    private AtomicLong topVer = new AtomicLong(-1);

    /** Mutex. */
    private final Lock lock = new ReentrantLock();

    /** Lock condition. */
    private final Condition cond = lock.newCondition();

    /** Subject ID initiated this transaction. */
    protected UUID subjId;

    /** Task name hash code. */
    protected int taskNameHash;

    /** Task name. */
    protected String taskName;

    /** Store used flag. */
    protected boolean storeEnabled = true;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    protected GridCacheTxAdapter() {
        // No-op.
    }

    /**
     * @param cctx Cache registry.
     * @param xidVer Transaction ID.
     * @param implicit Implicit flag.
     * @param implicitSingle Implicit with one key flag.
     * @param loc Local flag.
     * @param sys System transaction flag.
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @param timeout Timeout.
     * @param txSize Transaction size.
     * @param grpLockKey Group lock key if this is group-lock transaction.
     */
    protected GridCacheTxAdapter(
        GridCacheSharedContext<K, V> cctx,
        GridCacheVersion xidVer,
        boolean implicit,
        boolean implicitSingle,
        boolean loc,
        boolean sys,
        GridCacheTxConcurrency concurrency,
        GridCacheTxIsolation isolation,
        long timeout,
        boolean invalidate,
        boolean storeEnabled,
        int txSize,
        @Nullable GridCacheTxKey grpLockKey,
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
        this.concurrency = concurrency;
        this.isolation = isolation;
        this.timeout = timeout;
        this.invalidate = invalidate;
        this.storeEnabled = storeEnabled;
        this.txSize = txSize;
        this.grpLockKey = grpLockKey;
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
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @param timeout Timeout.
     * @param txSize Transaction size.
     * @param grpLockKey Group lock key if this is group-lock transaction.
     */
    protected GridCacheTxAdapter(
        GridCacheSharedContext<K, V> cctx,
        UUID nodeId,
        GridCacheVersion xidVer,
        GridCacheVersion startVer,
        long threadId,
        boolean sys,
        GridCacheTxConcurrency concurrency,
        GridCacheTxIsolation isolation,
        long timeout,
        int txSize,
        @Nullable GridCacheTxKey grpLockKey,
        @Nullable UUID subjId,
        int taskNameHash
    ) {
        this.cctx = cctx;
        this.nodeId = nodeId;
        this.threadId = threadId;
        this.xidVer = xidVer;
        this.startVer = startVer;
        this.sys = sys;
        this.concurrency = concurrency;
        this.isolation = isolation;
        this.timeout = timeout;
        this.txSize = txSize;
        this.grpLockKey = grpLockKey;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;

        implicit = false;
        implicitSingle = false;
        loc = false;

        log = U.logger(cctx.kernalContext(), logRef, this);
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
    protected boolean updateNearCache(GridCacheContext<K, V> cacheCtx, K key, long topVer) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCacheTxEntry<K, V>> optimisticLockEntries() {
        assert optimistic();

        if (!groupLock())
            return writeEntries();
        else {
            if (!F.isEmpty(invalidParts)) {
                assert invalidParts.size() == 1 : "Only one partition expected for group lock transaction " +
                    "[tx=" + this + ", invalidParts=" + invalidParts + ']';
                assert groupLockEntry() == null : "Group lock key should be rejected " +
                    "[tx=" + this + ", groupLockEntry=" + groupLockEntry() + ']';
                assert F.isEmpty(writeMap()) : "All entries should be rejected for group lock transaction " +
                    "[tx=" + this + ", writes=" + writeMap() + ']';

                return Collections.emptyList();
            }

            GridCacheTxEntry<K, V> grpLockEntry = groupLockEntry();

            assert grpLockEntry != null || (near() && !local()):
                "Group lock entry was not enlisted into transaction [tx=" + this +
                ", grpLockKey=" + groupLockKey() + ']';

            return grpLockEntry == null ?
                Collections.<GridCacheTxEntry<K,V>>emptyList() :
                Collections.singletonList(grpLockEntry);
        }
    }

    /**
     * @param recoveryWrites Recover write entries.
     */
    public void recoveryWrites(Collection<GridCacheTxEntry<K, V>> recoveryWrites) {
        this.recoveryWrites = recoveryWrites;
    }

    /**
     * @return Recover write entries.
     */
    @Override public Collection<GridCacheTxEntry<K, V>> recoveryWrites() {
        return recoveryWrites;
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
    @Override public boolean storeUsed() {
        return storeEnabled() && store() != null;
    }

    /**
     * Store manager for current transaction.
     *
     * @return Store manager.
     */
    protected GridCacheStoreManager<K, V> store() {
        if (!activeCacheIds().isEmpty()) {
            int cacheId = F.first(activeCacheIds());

            GridCacheStoreManager<K, V> store = cctx.cacheContext(cacheId).store();

            return store.configured() ? store : null;
        }

        return null;
    }

    /**
     * This method uses unchecked assignment to cast group lock key entry to transaction generic signature.
     *
     * @return Group lock tx entry.
     */
    @SuppressWarnings("unchecked")
    public GridCacheTxEntry<K, V> groupLockEntry() {
        return ((GridCacheTxAdapter)this).entry(groupLockKey());
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
    @Override public long topologyVersion() {
        long res = topVer.get();

        if (res == -1)
            return cctx.exchange().topologyVersion();

        return res;
    }

    /** {@inheritDoc} */
    @Override public long topologyVersion(long topVer) {
        this.topVer.compareAndSet(-1, topVer);

        return this.topVer.get();
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
                res = finalizing.compareAndSet(NONE, USER_FINISH);

                break;

            case RECOVERY_WAIT:
                finalizing.compareAndSet(NONE, RECOVERY_WAIT);

                FinalizationStatus cur = finalizing.get();

                res = cur == RECOVERY_WAIT || cur == RECOVERY_FINISH;

                break;

            case RECOVERY_FINISH:
                FinalizationStatus old = finalizing.get();

                res = old != USER_FINISH && finalizing.compareAndSet(old, status);

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
    @Override public boolean groupLock() {
        return grpLockKey != null;
    }

    /** {@inheritDoc} */
    @Override public GridCacheTxKey groupLockKey() {
        return grpLockKey;
    }

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
    @Override public Set<Integer> invalidPartitions() {
        return invalidParts;
    }

    /** {@inheritDoc} */
    @Override public void addInvalidPartition(GridCacheContext<K, V> cacheCtx, int part) {
        invalidParts.add(part);

        if (log.isDebugEnabled())
            log.debug("Added invalid partition for transaction [part=" + part + ", tx=" + this + ']');
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion ownedVersion(GridCacheTxKey<K> key) {
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
    @Override public GridCacheTxIsolation isolation() {
        return isolation;
    }

    /** {@inheritDoc} */
    @Override public GridCacheTxConcurrency concurrency() {
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
    @Override public boolean ownsLock(GridCacheEntryEx<K, V> entry) throws GridCacheEntryRemovedException {
        GridCacheContext<K, V> cacheCtx = entry.context();

        GridCacheTxEntry<K, V> txEntry = entry(entry.txKey());

        GridCacheVersion explicit = txEntry == null ? null : txEntry.explicitVersion();

        assert !txEntry.groupLockEntry() || groupLock() : "Can not have group-locked tx entries in " +
            "non-group-lock transactions [txEntry=" + txEntry + ", tx=" + this + ']';

        return local() && !cacheCtx.isDht() ?
            entry.lockedByThread(threadId()) || (explicit != null && entry.lockedBy(explicit)) :
            // If candidate is not there, then lock was explicit.
            // Otherwise, check if entry is owned by version.
            !entry.hasLockCandidate(xidVersion()) || entry.lockedBy(xidVersion());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("SimplifiableIfStatement")
    @Override public boolean ownsLockUnsafe(GridCacheEntryEx<K, V> entry) {
        GridCacheContext<K, V> cacheCtx = entry.context();

        GridCacheTxEntry<K, V> txEntry = entry(entry.txKey());

        GridCacheVersion explicit = txEntry == null ? null : txEntry.explicitVersion();

        assert !txEntry.groupLockEntry() || groupLock() : "Can not have group-locked tx entries in " +
            "non-group-lock transactions [txEntry=" + txEntry + ", tx=" + this + ']';

        return local() && !cacheCtx.isDht() ?
            entry.lockedByThreadUnsafe(threadId()) || (explicit != null && entry.lockedByUnsafe(explicit)) :
            // If candidate is not there, then lock was explicit.
            // Otherwise, check if entry is owned by version.
            !entry.hasLockCandidateUnsafe(xidVersion()) || entry.lockedByUnsafe(xidVersion());
    }

    /** {@inheritDoc} */
    @Override public GridCacheTxState state() {
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
        GridCacheTxState state = state();

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
    protected boolean checkInternal(GridCacheTxKey<K> key) {
        if (key.key() instanceof GridCacheInternal) {
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
    @Override public boolean state(GridCacheTxState state) {
        return state(state, false);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ExternalizableWithoutPublicNoArgConstructor")
    @Override public IgniteFuture<GridCacheTx> finishFuture() {
        GridFutureAdapter<GridCacheTx> fut = finFut.get();

        if (fut == null) {
            fut = new GridFutureAdapter<GridCacheTx>(cctx.kernalContext()) {
                @Override public String toString() {
                    return S.toString(GridFutureAdapter.class, this, "tx", GridCacheTxAdapter.this);
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

    /**
     *
     * @param state State to set.
     * @param timedOut Timeout flag.
     * @return {@code True} if state changed.
     */
    @SuppressWarnings({"TooBroadScope"})
    private boolean state(GridCacheTxState state, boolean timedOut) {
        boolean valid = false;

        GridCacheTxState prev;

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
            GridFutureAdapter<GridCacheTx> fut = finFut.get();

            if (fut != null)
                fut.onDone(this);
        }

        if (valid) {
            // Seal transactions maps.
            if (state != ACTIVE)
                seal();

            cctx.tm().onTxStateChange(prev, state, this);
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
        return null;
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
    protected GridTuple3<GridCacheOperation, V, byte[]> applyTransformClosures(GridCacheTxEntry<K, V> txEntry,
        boolean metrics) throws GridCacheEntryRemovedException, IgniteCheckedException {
        GridCacheContext cacheCtx = txEntry.context();

        assert cacheCtx != null;

        if (isSystemInvalidate())
            return F.t(cacheCtx.isStoreEnabled() ? RELOAD : DELETE, null, null);
        if (F.isEmpty(txEntry.transformClosures()))
            return F.t(txEntry.op(), txEntry.value(), txEntry.valueBytes());
        else {
            try {
                boolean recordEvt = cctx.gridEvents().isRecordable(EVT_CACHE_OBJECT_READ);

                V val = txEntry.hasValue() ? txEntry.value() :
                    txEntry.cached().innerGet(this,
                        /*swap*/false,
                        /*read through*/false,
                        /*fail fast*/true,
                        /*unmarshal*/true,
                        /*metrics*/metrics,
                        /*event*/recordEvt,
                        /*temporary*/true,
                        /*subjId*/subjId,
                        /**closure name */recordEvt ? F.first(txEntry.transformClosures()) : null,
                        resolveTaskName(),
                        CU.<K, V>empty(),
                        null);

                try {
                    for (IgniteClosure<V, V> clos : txEntry.transformClosures())
                        val = clos.apply(val);
                }
                catch (Throwable e) {
                    throw new IgniteException("Transform closure must not throw any exceptions " +
                        "(transaction will be invalidated)", e);
                }

                GridCacheOperation op = val == null ? DELETE : UPDATE;

                return F.t(op, (V)cacheCtx.<V>unwrapTemporary(val), null);
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
     * @param e Transaction entry.
     * @param primaryOnly Flag to include backups into check or not.
     * @return {@code True} if entry is locally mapped as a primary or back up node.
     */
    protected boolean isNearLocallyMapped(GridCacheTxEntry<K, V> e, boolean primaryOnly) {
        GridCacheContext<K, V> cacheCtx = e.context();

        if (!cacheCtx.isNear())
            return false;

        // Try to take either entry-recorded primary node ID,
        // or transaction node ID from near-local transactions.
        UUID nodeId = e.nodeId() == null ? local() ? this.nodeId :  null : e.nodeId();

        if (nodeId != null && nodeId.equals(cctx.localNodeId()))
            return true;

        GridCacheEntryEx<K, V> cached = e.cached();

        int part = cached != null ? cached.partition() : cacheCtx.affinity().partition(e.key());

        List<ClusterNode> affNodes = cacheCtx.affinity().nodes(part, topologyVersion());

        e.locallyMapped(F.contains(affNodes, cctx.localNode()));

        if (primaryOnly) {
            ClusterNode primary = F.first(affNodes);

            if (primary == null && !isAffinityNode(cacheCtx.config()))
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
    protected boolean evictNearEntry(GridCacheTxEntry<K, V> e, boolean primaryOnly) throws IgniteCheckedException {
        assert e != null;

        if (isNearLocallyMapped(e, primaryOnly)) {
            GridCacheEntryEx<K, V> cached = e.cached();

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

        isolation = GridCacheTxIsolation.fromOrdinal(in.read());
        concurrency = GridCacheTxConcurrency.fromOrdinal(in.read());

        state = GridCacheTxState.fromOrdinal(in.read());
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
    @Override public boolean equals(Object o) {
        return o == this || (o instanceof GridCacheTxAdapter && xidVer.equals(((GridCacheTxAdapter)o).xidVer));
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return xidVer.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridCacheTxAdapter.class, this,
            "duration", (U.currentTimeMillis() - startTime) + "ms", "grpLock", groupLock(),
            "onePhaseCommit", onePhaseCommit);
    }

    /**
     * Transaction shadow class to be used for deserialization.
     */
    private static class TxShadow extends GridMetadataAwareAdapter implements GridCacheTx {
        /** */
        private static final long serialVersionUID = 0L;

        /** Xid. */
        private final IgniteUuid xid;

        /** Node ID. */
        private final UUID nodeId;

        /** Thread ID. */
        private final long threadId;

        /** Start time. */
        private final long startTime;

        /** Transaction isolation. */
        private final GridCacheTxIsolation isolation;

        /** Concurrency. */
        private final GridCacheTxConcurrency concurrency;

        /** Invalidate flag. */
        private final boolean invalidate;

        /** Timeout. */
        private final long timeout;

        /** State. */
        private final GridCacheTxState state;

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
        TxShadow(IgniteUuid xid, UUID nodeId, long threadId, long startTime, GridCacheTxIsolation isolation,
            GridCacheTxConcurrency concurrency, boolean invalidate, boolean implicit, long timeout,
            GridCacheTxState state, boolean rollbackOnly) {
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
        @Override public GridCacheTxIsolation isolation() {
            return isolation;
        }

        /** {@inheritDoc} */
        @Override public GridCacheTxConcurrency concurrency() {
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
        @Override public GridCacheTxState state() {
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
        @Override public IgniteFuture<GridCacheTx> commitAsync() {
            throw new IllegalStateException("Deserialized transaction can only be used as read-only.");
        }

        /** {@inheritDoc} */
        @Override public void rollback() {
            throw new IllegalStateException("Deserialized transaction can only be used as read-only.");
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return this == o || o instanceof GridCacheTx && xid.equals(((GridCacheTx)o).xid());
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
