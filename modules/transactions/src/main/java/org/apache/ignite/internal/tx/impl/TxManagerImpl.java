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

package org.apache.ignite.internal.tx.impl;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.lang.LoggerMessageHelper.format;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.LockException;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.message.TxFinishRequest;
import org.apache.ignite.internal.tx.message.TxFinishResponse;
import org.apache.ignite.internal.tx.message.TxFinishResponseBuilder;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.NetworkMessageHandler;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * A transaction manager implementation.
 *
 * <p>Uses 2PC for atomic commitment and 2PL for concurrency control.
 */
public class TxManagerImpl implements TxManager, NetworkMessageHandler {
    /** Tx messages factory. */
    private static final TxMessagesFactory FACTORY = new TxMessagesFactory();

    /** Tx finish timeout. */
    private static final int TIMEOUT = 5_000;

    /** Cluster service. */
    protected final ClusterService clusterService;

    /** Lock manager. */
    private final LockManager lockManager;

    /**
     * The storage for tx states.
     *
     * <p>TODO IGNITE-15931 use Storage for states, implement max size, implement replication.
     */
    private final ConcurrentHashMap<Timestamp, TxState> states = new ConcurrentHashMap<>();

    /**
     * The storage for locks acquired by transactions. Each key is mapped to lock type where true is for read.
     *
     * <p>TODO IGNITE-15932 use Storage for locks. Introduce limits, deny lock operation if the limit is exceeded.
     */
    private final ConcurrentHashMap<Timestamp, Map<LockKey, Boolean>> locks = new ConcurrentHashMap<>();

    /**
     * TODO IGNITE-15930.
     */
    private ThreadLocal<InternalTransaction> threadCtx = new ThreadLocal<>();

    /**
     * The constructor.
     *
     * @param clusterService Cluster service.
     * @param lockManager Lock manager.
     */
    public TxManagerImpl(ClusterService clusterService, LockManager lockManager) {
        this.clusterService = clusterService;
        this.lockManager = lockManager;
    }

    /** {@inheritDoc} */
    @Override
    public InternalTransaction begin() {
        Timestamp ts = Timestamp.nextVersion();

        states.put(ts, TxState.PENDING);

        return new TransactionImpl(this, ts, clusterService.topologyService().localMember().address());
    }

    /** {@inheritDoc} */
    @Override
    public TxState state(Timestamp ts) {
        return states.get(ts);
    }

    /** {@inheritDoc} */
    @Override
    public void forget(Timestamp ts) {
        states.remove(ts);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> commitAsync(Timestamp ts) {
        if (changeState(ts, TxState.PENDING, TxState.COMMITED) || state(ts) == TxState.COMMITED) {
            unlockAll(ts);

            return completedFuture(null);
        }

        return failedFuture(new TransactionException(format("Failed to commit a transaction [ts={}, state={}]", ts, state(ts))));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> rollbackAsync(Timestamp ts) {
        if (changeState(ts, TxState.PENDING, TxState.ABORTED) || state(ts) == TxState.ABORTED) {
            unlockAll(ts);

            return completedFuture(null);
        }

        return failedFuture(new TransactionException(format("Failed to rollback a transaction [ts={}, state={}]", ts, state(ts))));
    }

    /**
     * Unlocks all locks for the timestamp.
     *
     * @param ts The timestamp.
     */
    private void unlockAll(Timestamp ts) {
        Map<LockKey, Boolean> locks = this.locks.remove(ts);

        if (locks == null) {
            return;
        }

        for (Map.Entry<LockKey, Boolean> lock : locks.entrySet()) {
            try {
                if (lock.getValue()) {
                    lockManager.tryReleaseShared(lock.getKey(), ts);
                } else {
                    lockManager.tryRelease(lock.getKey(), ts);
                }
            } catch (LockException e) {
                assert false; // This shouldn't happen during tx finish.
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean changeState(Timestamp ts, TxState before, TxState after) {
        return states.replace(ts, before, after);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> writeLock(IgniteUuid lockId, ByteBuffer keyData, Timestamp ts) {
        // TODO IGNITE-15933 process tx messages in striped fasion to avoid races. But locks can be acquired from any thread !
        TxState state = state(ts);

        if (state != null && state != TxState.PENDING) {
            return failedFuture(new TransactionException(
                    "The operation is attempted for completed transaction"));
        }

        // Should rollback tx on lock error.
        LockKey key = new LockKey(lockId, keyData);

        return lockManager.tryAcquire(key, ts)
                .thenAccept(ignored -> recordLock(key, ts, Boolean.FALSE));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> readLock(IgniteUuid lockId, ByteBuffer keyData, Timestamp ts) {
        TxState state = state(ts);

        if (state != null && state != TxState.PENDING) {
            return failedFuture(new TransactionException(
                    "The operation is attempted for completed transaction"));
        }

        LockKey key = new LockKey(lockId, keyData);

        return lockManager.tryAcquireShared(key, ts)
                .thenAccept(ignored -> recordLock(key, ts, Boolean.TRUE));
    }

    /**
     * Records the acquired lock for further unlocking.
     *
     * @param key The key.
     * @param timestamp The tx timestamp.
     * @param read Read lock.
     */
    private void recordLock(LockKey key, Timestamp timestamp, Boolean read) {
        locks.compute(timestamp,
                new BiFunction<Timestamp, Map<LockKey, Boolean>, Map<LockKey, Boolean>>() {
                    @Override
                    public Map<LockKey, Boolean> apply(Timestamp timestamp,
                            Map<LockKey, Boolean> map) {
                        if (map == null) {
                            map = new HashMap<>();
                        }

                        Boolean mode = map.get(key);

                        if (mode == null) {
                            map.put(key, read);
                        } else if (read == Boolean.FALSE && mode == Boolean.TRUE) { // Override read lock.
                            map.put(key, Boolean.FALSE);
                        }

                        return map;
                    }
                });
    }

    /** {@inheritDoc} */
    @Override
    public TxState getOrCreateTransaction(Timestamp ts) {
        return states.putIfAbsent(ts, TxState.PENDING);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> finishRemote(
            NetworkAddress addr,
            Timestamp ts,
            boolean commit,
            Set<String> groups
    ) {
        assert groups != null && !groups.isEmpty();

        TxFinishRequest req = FACTORY.txFinishRequest().timestamp(ts).groups(groups)
                .commit(commit).build();

        CompletableFuture<NetworkMessage> fut = clusterService.messagingService()
                .invoke(addr, req, TIMEOUT);

        // Submit response to a dedicated pool to avoid deadlocks. TODO: IGNITE-15389
        return fut.thenApplyAsync(resp -> ((TxFinishResponse) resp).errorMessage()).thenCompose(msg ->
                msg == null ? completedFuture(null) : failedFuture(new TransactionException(msg)));
    }

    /** {@inheritDoc} */
    @Override
    public boolean isLocal(NetworkAddress node) {
        return clusterService.topologyService().localMember().address().equals(node);
    }

    /** {@inheritDoc} */
    @Override
    public void setTx(InternalTransaction tx) {
        threadCtx.set(tx);
    }

    /** {@inheritDoc} */
    @Override
    @Nullable
    public InternalTransaction tx() throws TransactionException {
        InternalTransaction tx = threadCtx.get();

        if (tx != null && tx.thread() != Thread.currentThread()) {
            throw new TransactionException("Transactional operation is attempted from another thread");
        }

        return tx;
    }

    /** {@inheritDoc} */
    @Override
    public void clearTx() {
        threadCtx.remove();
    }

    /** {@inheritDoc} */
    @Override
    public int finished() {
        return states.size();
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        clusterService.messagingService().addMessageHandler(TxMessageGroup.class, this);
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        // No-op.
    }

    /**
     * Lock key.
     */
    private static class LockKey {
        /** The id. */
        private final IgniteUuid id;

        /** The key. */
        private final ByteBuffer key;

        /**
         * The constructor.
         *
         * @param id The id.
         * @param key The key.
         */
        LockKey(IgniteUuid id, ByteBuffer key) {
            this.id = id;
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            LockKey key1 = (LockKey) o;
            return id.equals(key1.id) && key.equals(key1.key);
        }

        /** {@inheritDoc} */
        @Override
        public int hashCode() {
            return Objects.hash(id, key);
        }
    }

    /**
     * Returns a lock manager.
     *
     * @return The lock manager.
     */
    @TestOnly
    public LockManager getLockManager() {
        return lockManager;
    }

    /**
     * Finishes a transaction for a group.
     *
     * @param groupId Group id.
     * @param ts The timestamp.
     * @param commit {@code True} to commit, false to abort.
     * @return The future.
     */
    protected CompletableFuture<?> finish(String groupId, Timestamp ts, boolean commit) {
        return CompletableFuture.completedFuture(null);
    }

    /** {@inheritDoc} */
    @Override
    public void onReceived(NetworkMessage message, NetworkAddress senderAddr,
            String correlationId) {
        // Support raft and transactions interop.
        if (message instanceof TxFinishRequest) {
            TxFinishRequest req = (TxFinishRequest) message;

            Set<String> groups = req.groups();

            CompletableFuture[] futs = new CompletableFuture[groups.size()];

            int i = 0;

            // Finish a tx for enlisted groups.
            for (String grp : groups) {
                futs[i++] = finish(grp, req.timestamp(), req.commit());
            }

            CompletableFuture.allOf(futs).thenCompose(ignored -> req.commit()
                    ? commitAsync(req.timestamp()) :
                    rollbackAsync(req.timestamp()))
                    .handle(new BiFunction<Void, Throwable, Void>() {
                        @Override
                        public Void apply(Void ignored, Throwable err) {
                            TxFinishResponseBuilder resp = FACTORY.txFinishResponse();

                            if (err != null) {
                                resp.errorMessage(err.getMessage());
                            }

                            clusterService.messagingService()
                                    .send(senderAddr, resp.build(), correlationId);

                            return null;
                        }
                    });
        }
    }
}
