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

package org.apache.ignite.internal.tx;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * A transaction manager.
 */
public interface TxManager extends IgniteComponent {
    /**
     * Starts a transaction coordinated by a local node.
     *
     * @return The transaction.
     */
    InternalTransaction begin();

    /**
     * Returns a transaction state.
     *
     * @param ts The timestamp.
     * @return The state or null if the state is unknown.
     */
    @Nullable TxState state(Timestamp ts);

    /**
     * Atomically changes the state of a transaction.
     *
     * @param ts     The timestamp.
     * @param before Before state.
     * @param after  After state.
     * @return {@code True} if a state was changed.
     */
    boolean changeState(Timestamp ts, @Nullable TxState before, TxState after);

    /**
     * Forgets the transaction state. Intended for cleanup.
     *
     * @param ts The timestamp.
     */
    void forget(Timestamp ts);

    /**
     * Commits a transaction.
     *
     * @param ts The timestamp.
     * @return The future.
     */
    CompletableFuture<Void> commitAsync(Timestamp ts);

    /**
     * Aborts a transaction.
     *
     * @param ts The timestamp.
     * @return The future.
     */
    CompletableFuture<Void> rollbackAsync(Timestamp ts);

    /**
     * Acqures a write lock.
     *
     * @param lockId  Table ID.
     * @param keyData The key data.
     * @param ts      The timestamp.
     * @return The future.
     * @throws LockException When a lock can't be taken due to possible deadlock.
     */
    public CompletableFuture<Void> writeLock(IgniteUuid lockId, ByteBuffer keyData, Timestamp ts);

    /**
     * Acqures a read lock.
     *
     * @param lockId  Lock id.
     * @param keyData The key data.
     * @param ts      The timestamp.
     * @return The future.
     * @throws LockException When a lock can't be taken due to possible deadlock.
     */
    public CompletableFuture<Void> readLock(IgniteUuid lockId, ByteBuffer keyData, Timestamp ts);

    /**
     * Returns a transaction state or starts a new in the PENDING state.
     *
     * @param ts The timestamp.
     * @return @{code null} if a transaction was created, or a current state.
     */
    @Nullable
    TxState getOrCreateTransaction(Timestamp ts);

    /**
     * Finishes a dependant remote transactions.
     *
     * @param ts     The timestamp.
     * @param addr   The address.
     * @param commit {@code True} if a commit requested.
     * @param groups Enlisted partition groups.
     */
    CompletableFuture<Void> finishRemote(NetworkAddress addr, Timestamp ts, boolean commit, Set<String> groups);

    /**
     * Checks if a passed address belongs to a local node.
     *
     * @param addr The address.
     * @return {@code True} if a local node.
     */
    boolean isLocal(NetworkAddress addr);

    /**
     * Sets a thread local transaction.
     *
     * @param tx The thread local transaction.
     * @deprecated Should be removed after table API adjustment TODO IGNITE-15930.
     */
    @Deprecated
    void setTx(InternalTransaction tx);

    /**
     * Returns a thread local transaction.
     *
     * @return The thread local transaction.
     * @throws TransactionException Thrown on illegal access.
     * @deprecated Should be removed after table API adjustment TODO IGNITE-15930.
     */
    @Deprecated
    InternalTransaction tx() throws TransactionException;

    /**
     * Clears thread local transaction.
     */
    @Deprecated
    void clearTx();

    /**
     * Returns a number of finished transactions.
     *
     * @return A number of finished transactions.
     */
    @TestOnly
    int finished();
}
