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

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheFilterFailedException;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.lang.GridTuple;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

/**
 * Transaction managed by cache ({@code 'Ex'} stands for external).
 */
public interface IgniteInternalTx {
    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public enum FinalizationStatus {
        /** Transaction was not finalized yet. */
        NONE,

        /** Transaction is being finalized by user. */
        USER_FINISH,

        /** Transaction is being finalized by recovery procedure. */
        RECOVERY_FINISH
    }

    /**
     * @return {@code True} if transaction started on the node initiated cache operation.
     */
    public boolean localResult();

    /**
     * Gets unique identifier for this transaction.
     *
     * @return Transaction UID.
     */
    public IgniteUuid xid();

    /**
     * ID of the node on which this transaction started.
     *
     * @return Originating node ID.
     */
    public UUID nodeId();

    /**
     * ID of the thread in which this transaction started.
     *
     * @return Thread ID.
     */
    public long threadId();

    /**
     * Start time of this transaction.
     *
     * @return Start time of this transaction on this node.
     */
    public long startTime();

    /**
     * Cache transaction isolation level.
     *
     * @return Isolation level.
     */
    public TransactionIsolation isolation();

    /**
     * Cache transaction concurrency mode.
     *
     * @return Concurrency mode.
     */
    public TransactionConcurrency concurrency();

    /**
     * Flag indicating whether transaction was started automatically by the
     * system or not. System will start transactions implicitly whenever
     * any cache {@code put(..)} or {@code remove(..)} operation is invoked
     * outside of transaction.
     *
     * @return {@code True} if transaction was started implicitly.
     */
    public boolean implicit();

    /**
     * Get invalidation flag for this transaction. If set to {@code true}, then
     * remote values will be {@code invalidated} (set to {@code null}) instead
     * of updated.
     * <p>
     * Invalidation messages don't carry new values, so they are a lot lighter
     * than update messages. However, when a value is accessed on a node after
     * it's been invalidated, it must be loaded from persistent store.
     *
     * @return Invalidation flag.
     */
    public boolean isInvalidate();

    /**
     * Gets current transaction state value.
     *
     * @return Current transaction state.
     */
    public TransactionState state();

    /**
     * Gets timeout value in milliseconds for this transaction. If transaction times
     * out prior to it's completion, {@link org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException} will be thrown.
     *
     * @return Transaction timeout value.
     */
    public long timeout();

    /**
     * Sets transaction timeout value. This value can be set only before a first operation
     * on transaction has been performed.
     *
     * @param timeout Transaction timeout value.
     * @return Previous timeout.
     */
    public long timeout(long timeout);

    /**
     * Changes transaction state from COMMITTING to MARKED_ROLLBACK.
     * Must be called only from thread committing transaction.
     */
    public void errorWhenCommitting();

    /**
     * Modify the transaction associated with the current thread such that the
     * only possible outcome of the transaction is to roll back the
     * transaction.
     *
     * @return {@code True} if rollback-only flag was set as a result of this operation,
     *      {@code false} if it was already set prior to this call or could not be set
     *      because transaction is already finishing up committing or rolling back.
     */
    public boolean setRollbackOnly();

    /**
     * If transaction was marked as rollback-only.
     *
     * @return {@code True} if transaction can only be rolled back.
     */
    public boolean isRollbackOnly();

    /**
     * Removes metadata by key.
     *
     * @param key Key of the metadata to remove.
     * @param <T> Type of the value.
     * @return Value of removed metadata or {@code null}.
     */
    @Nullable public <T> T removeMeta(int key);

    /**
     * Gets metadata by key.
     *
     * @param key Metadata key.
     * @param <T> Type of the value.
     * @return Metadata value or {@code null}.
     */
    @Nullable public <T> T meta(int key);

    /**
     * Adds a new metadata.
     *
     * @param key Metadata key.
     * @param val Metadata value.
     * @param <T> Type of the value.
     * @return Metadata previously associated with given name, or
     *      {@code null} if there was none.
     */
    @Nullable public <T> T addMeta(int key, T val);

    /**
     * @return Size of the transaction.
     */
    public int size();

    /**
     * @return {@code True} if transaction is allowed to use store.
     */
    public boolean storeEnabled();

    /**
     * @return {@code True} if transaction is allowed to use store and transactions spans one or more caches with
     *      store enabled.
     */
    public boolean storeWriteThrough();

    /**
     * Checks if this is system cache transaction. System transactions are isolated from user transactions
     * because some of the public API methods may be invoked inside user transactions and internally start
     * system cache transactions.
     *
     * @return {@code True} if transaction is started for system cache.
     */
    public boolean system();

    /**
     * @return Pool where message for the given transaction must be processed.
     */
    public byte ioPolicy();

    /**
     * @return Last recorded topology version.
     */
    public AffinityTopologyVersion topologyVersion();

    /**
     * @return Topology version snapshot.
     */
    public AffinityTopologyVersion topologyVersionSnapshot();

    /**
     * @return Flag indicating whether transaction is implicit with only one key.
     */
    public boolean implicitSingle();

    /**
     * @return Transaction state.
     */
    public IgniteTxState txState();

    /**
     * @return {@code true} or {@code false} if the deployment is enabled or disabled for all active caches involved
     * in this transaction.
     */
    public boolean activeCachesDeploymentEnabled();

    /**
     * @param depEnabled Flag indicating whether deployment is enabled for caches from this transaction or not.
     */
    public void activeCachesDeploymentEnabled(boolean depEnabled);

    /**
     * Attempts to set topology version and returns the current value.
     * If topology version was previously set, then it's value will
     * be returned (but not updated).
     *
     * @param topVer Topology version.
     * @return Recorded topology version.
     */
    public AffinityTopologyVersion topologyVersion(AffinityTopologyVersion topVer);

    /**
     * @return {@code True} if transaction is empty.
     */
    public boolean empty();

    /**
     * @param status Finalization status to set.
     * @return {@code True} if could mark was set.
     */
    public boolean markFinalizing(FinalizationStatus status);

    /**
     * @param cacheCtx Cache context.
     * @param part Invalid partition.
     */
    public void addInvalidPartition(GridCacheContext<?, ?> cacheCtx, int part);

    /**
     * @return Invalid partitions.
     */
    public Map<Integer, Set<Integer>> invalidPartitions();

    /**
     * Gets owned version for near remote transaction.
     *
     * @param key Key to get version for.
     * @return Owned version, if any.
     */
    @Nullable public GridCacheVersion ownedVersion(IgniteTxKey key);

    /**
     * Gets ID of additional node involved. For example, in DHT case, other node is
     * near node ID.
     *
     * @return Parent node IDs.
     */
    @Nullable public UUID otherNodeId();

    /**
     * @return Event node ID.
     */
    public UUID eventNodeId();

    /**
     * Gets node ID which directly started this transaction. In case of DHT local transaction it will be
     * near node ID, in case of DHT remote transaction it will be primary node ID.
     *
     * @return Originating node ID.
     */
    public UUID originatingNodeId();

    /**
     * @return Master node IDs.
     */
    public Collection<UUID> masterNodeIds();

    /**
     * @return Near transaction ID.
     */
    @Nullable public GridCacheVersion nearXidVersion();

    /**
     * @return Transaction nodes mapping (primary node -> related backup nodes).
     */
    @Nullable public Map<UUID, Collection<UUID>> transactionNodes();

    /**
     * @param entry Entry to check.
     * @return {@code True} if lock is owned.
     * @throws GridCacheEntryRemovedException If entry has been removed.
     */
    public boolean ownsLock(GridCacheEntryEx entry) throws GridCacheEntryRemovedException;

    /**
     * @param entry Entry to check.
     * @return {@code True} if lock is owned.
     */
    public boolean ownsLockUnsafe(GridCacheEntryEx entry);

    /**
     * @return {@code True} if near transaction.
     */
    public boolean near();

    /**
     * @return {@code True} if DHT transaction.
     */
    public boolean dht();

    /**
     * @return {@code True} if dht colocated transaction.
     */
    public boolean colocated();

    /**
     * @return {@code True} if transaction is local, {@code false} if it's remote.
     */
    public boolean local();

    /**
     * @return Subject ID initiated this transaction.
     */
    public UUID subjectId();

    /**
     * Task name hash in case if transaction was initiated within task execution.
     *
     * @return Task name hash.
     */
    public int taskNameHash();

    /**
     * @return {@code True} if transaction is user transaction, which means:
     * <ul>
     *     <li>Explicit</li>
     *     <li>Local</li>
     *     <li>Not DHT</li>
     * </ul>
     */
    public boolean user();

    /**
     * @param key Key to check.
     * @return {@code True} if key is present.
     */
    public boolean hasWriteKey(IgniteTxKey key);

    /**
     * @return Read set.
     */
    public Set<IgniteTxKey> readSet();

    /**
     * @return Write set.
     */
    public Set<IgniteTxKey> writeSet();

    /**
     * @return All transaction entries.
     */
    public Collection<IgniteTxEntry> allEntries();

    /**
     * @return Write entries.
     */
    public Collection<IgniteTxEntry> writeEntries();

    /**
     * @return Read entries.
     */
    public Collection<IgniteTxEntry> readEntries();

    /**
     * @return Transaction write map.
     */
    public Map<IgniteTxKey, IgniteTxEntry> writeMap();

    /**
     * @return Transaction read map.
     */
    public Map<IgniteTxKey, IgniteTxEntry> readMap();

    /**
     * Gets a list of entries that needs to be locked on the next step of prepare stage of
     * optimistic transaction.
     *
     * @return List of tx entries for optimistic locking.
     */
    public Collection<IgniteTxEntry> optimisticLockEntries();

    /**
     * Seals transaction for updates.
     */
    public void seal();

    /**
     * @param key Key for the entry.
     * @return Entry for the key (either from write set or read set).
     */
    @Nullable public IgniteTxEntry entry(IgniteTxKey key);

    /**
     * @param ctx Cache context.
     * @param failFast Fail-fast flag.
     * @param key Key to look up.
     * @return Current value for the key within transaction.
     * @throws GridCacheFilterFailedException If filter failed and failFast is {@code true}.
     */
     @Nullable public GridTuple<CacheObject> peek(
         GridCacheContext ctx,
         boolean failFast,
         KeyCacheObject key) throws GridCacheFilterFailedException;

    /**
     * @return Transaction version.
     */
    public GridCacheVersion xidVersion();

    /**
     * @return Version created at commit time.
     */
    public GridCacheVersion commitVersion();

    /**
     * @param commitVer Commit version.
     */
    public void commitVersion(GridCacheVersion commitVer);

    /**
     * @return Future.
     */
    @Nullable public IgniteInternalFuture<?> salvageTx();

    /**
     * @param endVer End version (a.k.a. <tt>'tnc'</tt> or <tt>'transaction number counter'</tt>)
     *      assigned to this transaction at the end of write phase.
     */
    public void endVersion(GridCacheVersion endVer);

    /**
     * @return Transaction write version. For all transactions except DHT transactions, will be equal to
     *      {@link #xidVersion()}.
     */
    public GridCacheVersion writeVersion();

    /**
     * Sets write version.
     *
     * @param ver Write version.
     */
    public void writeVersion(GridCacheVersion ver);

    /**
     * @return Future for transaction completion.
     */
    public IgniteInternalFuture<IgniteInternalTx> finishFuture();

    /**
     * @return Future for transaction prepare if prepare is in progress.
     */
    @Nullable public IgniteInternalFuture<?> currentPrepareFuture();

    /**
     * @param state Transaction state.
     * @return {@code True} if transition was valid, {@code false} otherwise.
     */
    public boolean state(TransactionState state);

    /**
     * @param invalidate Invalidate flag.
     */
    public void invalidate(boolean invalidate);

    /**
     * @param sysInvalidate System invalidate flag.
     */
    public void systemInvalidate(boolean sysInvalidate);

    /**
     * @return System invalidate flag.
     */
    public boolean isSystemInvalidate();

    /**
     * Asynchronously rollback this transaction.
     *
     * @return Rollback future.
     */
    public IgniteInternalFuture<IgniteInternalTx> rollbackAsync();

    /**
     * Asynchronously commits this transaction by initiating {@code two-phase-commit} process.
     *
     * @return Future for commit operation.
     */
    public IgniteInternalFuture<IgniteInternalTx> commitAsync();

    /**
     * Callback invoked whenever there is a lock that has been acquired
     * by this transaction for any of the participating entries.
     *
     * @param entry Cache entry.
     * @param owner Lock candidate that won ownership of the lock.
     * @return {@code True} if transaction cared about notification.
     */
    public boolean onOwnerChanged(GridCacheEntryEx entry, GridCacheMvccCandidate owner);

    /**
     * @return {@code True} if transaction timed out.
     */
    public boolean timedOut();

    /**
     * @return {@code True} if transaction had completed successfully or unsuccessfully.
     */
    public boolean done();

    /**
     * @return {@code True} for OPTIMISTIC transactions.
     */
    public boolean optimistic();

    /**
     * @return {@code True} for PESSIMISTIC transactions.
     */
    public boolean pessimistic();

    /**
     * @return {@code True} if read-committed.
     */
    public boolean readCommitted();

    /**
     * @return {@code True} if repeatable-read.
     */
    public boolean repeatableRead();

    /**
     * @return {@code True} if serializable.
     */
    public boolean serializable();

    /**
     * Gets allowed remaining time for this transaction.
     *
     * @return Remaining time.
     * @throws IgniteTxTimeoutCheckedException If transaction timed out.
     */
    public long remainingTime() throws IgniteTxTimeoutCheckedException;

    /**
     * @return Alternate transaction versions.
     */
    public Collection<GridCacheVersion> alternateVersions();

    /**
     * @return {@code True} if transaction needs completed versions for processing.
     */
    public boolean needsCompletedVersions();

    /**
     * @param base Base for committed versions.
     * @param committed Committed transactions relative to base.
     * @param rolledback Rolled back transactions relative to base.
     */
    public void completedVersions(GridCacheVersion base,
        Collection<GridCacheVersion> committed,
        Collection<GridCacheVersion> rolledback);

    /**
     * @return {@code True} if transaction has at least one internal entry.
     */
    public boolean internal();

    /**
     * @return {@code True} if transaction is a one-phase-commit transaction.
     */
    public boolean onePhaseCommit();

    /**
     * @param e Commit error.
     */
    public void commitError(Throwable e);

    /**
     * Returns label of transactions.
     *
     * @return Label of transaction or {@code null} if there was not set.
     */
    @Nullable public String label();

    /**
     * @param mvccSnapshot Mvcc snapshot.
     */
    public void mvccSnapshot(MvccSnapshot mvccSnapshot);

    /**
     * @return Mvcc snapshot.
     */
    public MvccSnapshot mvccSnapshot();

    /**
     * @return Transaction counters.
     * @param createIfAbsent {@code True} if non-null instance is needed.
     */
    @Nullable @Contract("true -> !null;") public TxCounters txCounters(boolean createIfAbsent);
}
