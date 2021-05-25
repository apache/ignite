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

package org.apache.ignite.internal.metastorage;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.internal.ConfigurationManager;
import org.apache.ignite.configuration.schemas.runner.NodeConfiguration;
import org.apache.ignite.internal.metastorage.client.MetaStorageServiceImpl;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageCommandListener;
import org.apache.ignite.internal.metastorage.watch.AggregatedWatch;
import org.apache.ignite.internal.metastorage.watch.KeyCriterion;
import org.apache.ignite.internal.metastorage.watch.WatchAggregator;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.metastorage.client.CompactedException;
import org.apache.ignite.metastorage.client.Condition;
import org.apache.ignite.metastorage.client.Entry;
import org.apache.ignite.metastorage.client.MetaStorageService;
import org.apache.ignite.metastorage.client.Operation;
import org.apache.ignite.metastorage.client.OperationTimeoutException;
import org.apache.ignite.metastorage.client.WatchListener;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * MetaStorage manager is responsible for:
 * <ul>
 *     <li>Handling cluster init message.</li>
 *     <li>Managing meta storage lifecycle including instantiation meta storage raft group.</li>
 *     <li>Providing corresponding meta storage service proxy interface</li>
 * </ul>
 */
// TODO: IGNITE-14586 Remove @SuppressWarnings when implementation provided.
@SuppressWarnings("unused")
public class MetaStorageManager {
    /** Meta storage raft group name. */
    private static final String METASTORAGE_RAFT_GROUP_NAME = "metastorage_raft_group";

    /** Vault manager in order to commit processed watches with corresponding applied revision. */
    private final VaultManager vaultMgr;

    /** Cluster network service that is used in order to handle cluster init message. */
    private final ClusterService clusterNetSvc;

    /** Raft manager that is used for metastorage raft group handling. */
    private final Loza raftMgr;

    /** Meta storage service. */
    private CompletableFuture<MetaStorageService> metaStorageSvcFut;

    /**
     * Aggregator of multiple watches to deploy them as one batch.
     *
     * @see WatchAggregator
     */
    private final WatchAggregator watchAggregator;

    /**
     * Future which will be completed with {@link IgniteUuid},
     * when aggregated watch will be successfully deployed.
     * Can be resolved to {@link Optional#empty()} if no watch deployed at the moment.
     */
    private CompletableFuture<Optional<IgniteUuid>> deployFut;

    /**
     * If true - all new watches will be deployed immediately.
     *
     * If false - all new watches will be aggregated to one batch
     * for further deploy by {@link MetaStorageManager#deployWatches()}
     */
    private boolean deployed;

    /**
     * The constructor.
     *
     * @param vaultMgr Vault manager.
     * @param clusterNetSvc Cluster network service.
     * @param raftMgr Raft manager.
     */
    public MetaStorageManager(
        VaultManager vaultMgr,
        ConfigurationManager locCfgMgr,
        ClusterService clusterNetSvc,
        Loza raftMgr
    ) {
        this.vaultMgr = vaultMgr;
        this.clusterNetSvc = clusterNetSvc;
        this.raftMgr = raftMgr;
        watchAggregator = new WatchAggregator();
        deployFut = new CompletableFuture<>();

        String locNodeName = locCfgMgr.configurationRegistry().getConfiguration(NodeConfiguration.KEY)
            .name().value();

        String[] metastorageNodes = locCfgMgr.configurationRegistry().getConfiguration(NodeConfiguration.KEY)
            .metastorageNodes().value();

        Predicate<ClusterNode> metaStorageNodesContainsLocPred =
            clusterNode -> Arrays.asList(metastorageNodes).contains(clusterNode.name());

        if (metastorageNodes.length > 0) {
            this.metaStorageSvcFut = CompletableFuture.completedFuture(new MetaStorageServiceImpl(
                    raftMgr.startRaftGroup(
                        METASTORAGE_RAFT_GROUP_NAME,
                        clusterNetSvc.topologyService().allMembers().stream().filter(
                            metaStorageNodesContainsLocPred).
                            collect(Collectors.toList()),
                        new MetaStorageCommandListener(new SimpleInMemoryKeyValueStorage())
                    )
                )
            );
        }
        else
            this.metaStorageSvcFut = new CompletableFuture<>();

        // TODO: IGNITE-14088: Uncomment and use real serializer factory
//        Arrays.stream(MetaStorageMessageTypes.values()).forEach(
//            msgTypeInstance -> net.registerMessageMapper(
//                msgTypeInstance.msgType(),
//                new DefaultMessageMapperProvider()
//            )
//        );

        // TODO: IGNITE-14414 Cluster initialization flow. Here we should complete metaStorageServiceFuture.
        clusterNetSvc.messagingService().addMessageHandler((message, sender, correlationId) -> {});
    }

    /**
     * Deploy all registered watches.
     */
    public synchronized void deployWatches() {
        try {
            var watch = watchAggregator.watch(
                vaultMgr.appliedRevision() + 1,
                this::storeEntries
            );

            if (watch.isEmpty())
                deployFut.complete(Optional.empty());
            else
                dispatchAppropriateMetaStorageWatch(watch.get()).thenAccept(id -> deployFut.complete(Optional.of(id))).join();
        }
        catch (IgniteInternalCheckedException e) {
            throw new IgniteInternalException("Couldn't receive applied revision during deploy watches", e);
        }

        deployed = true;
    }

    /**
     * Register watch listener by key.
     *
     * @param key The target key.
     * @param lsnr Listener which will be notified for each update.
     * @return Subscription identifier. Could be used in {@link #unregisterWatch} method in order to cancel
     * subscription
     */
    public synchronized CompletableFuture<Long> registerWatch(
        @Nullable ByteArray key,
        @NotNull WatchListener lsnr
    ) {
        return waitForReDeploy(watchAggregator.add(key, lsnr));
    }

    /**
     * Register watch listener by key prefix.
     *
     * @param key Prefix to listen.
     * @param lsnr Listener which will be notified for each update.
     * @return Subscription identifier. Could be used in {@link #unregisterWatch} method in order to cancel
     * subscription
     */
    public synchronized CompletableFuture<Long> registerWatchByPrefix(
        @Nullable ByteArray key,
        @NotNull WatchListener lsnr
    ) {
        return waitForReDeploy(watchAggregator.addPrefix(key, lsnr));
    }

    /**
     * Register watch listener by collection of keys.
     *
     * @param keys Collection listen.
     * @param lsnr Listener which will be notified for each update.
     * @return Subscription identifier. Could be used in {@link #unregisterWatch} method in order to cancel
     * subscription
     */
    public synchronized CompletableFuture<Long> registerWatch(
        @NotNull Collection<ByteArray> keys,
        @NotNull WatchListener lsnr
    ) {
        return waitForReDeploy(watchAggregator.add(keys, lsnr));
    }

    /**
     * Register watch listener by range of keys.
     *
     * @param from Start key of range.
     * @param to End key of range (exclusively).
     * @param lsnr Listener which will be notified for each update.
     * @return future with id of registered watch.
     */
    public synchronized CompletableFuture<Long> registerWatch(
        @NotNull ByteArray from,
        @NotNull ByteArray to,
        @NotNull WatchListener lsnr
    ) {
        return waitForReDeploy(watchAggregator.add(from, to, lsnr));
    }

    /**
     * Unregister watch listener by id.
     *
     * @param id of watch to unregister.
     * @return future, which will be completed when unregister finished.
     */
    public synchronized CompletableFuture<Void> unregisterWatch(long id) {
        watchAggregator.cancel(id);
        if (deployed)
            return updateWatches().thenAccept(v -> {});
        else
            return deployFut.thenAccept(uuid -> {});
    }

    /**
     * @see MetaStorageService#get(ByteArray)
     */
    public @NotNull CompletableFuture<Entry> get(@NotNull ByteArray key) {
        return metaStorageSvcFut.thenCompose(svc -> svc.get(key));
    }

    /**
     * @see MetaStorageService#get(ByteArray, long)
     */
    public @NotNull CompletableFuture<Entry> get(@NotNull ByteArray key, long revUpperBound) {
        return metaStorageSvcFut.thenCompose(svc -> svc.get(key, revUpperBound));
    }

    /**
     * @see MetaStorageService#getAll(Set)
     */
    public @NotNull CompletableFuture<Map<ByteArray, Entry>> getAll(Set<ByteArray> keys) {
        return metaStorageSvcFut.thenCompose(svc -> svc.getAll(keys));
    }

    /**
     * @see MetaStorageService#getAll(Set, long)
     */
    public @NotNull CompletableFuture<Map<ByteArray, Entry>> getAll(Set<ByteArray> keys, long revUpperBound) {
        return metaStorageSvcFut.thenCompose(svc -> svc.getAll(keys, revUpperBound));
    }

    /**
     * @see MetaStorageService#put(ByteArray, byte[])
     */
    public @NotNull CompletableFuture<Void> put(@NotNull ByteArray key, byte[] val) {
        return metaStorageSvcFut.thenCompose(svc -> svc.put(key, val));
    }

    /**
     * @see MetaStorageService#getAndPut(ByteArray, byte[])
     */
    public @NotNull CompletableFuture<Entry> getAndPut(@NotNull ByteArray key, byte[] val) {
        return metaStorageSvcFut.thenCompose(svc -> svc.getAndPut(key, val));
    }

    /**
     * @see MetaStorageService#putAll(Map)
     */
    public @NotNull CompletableFuture<Void> putAll(@NotNull Map<ByteArray, byte[]> vals) {
        return metaStorageSvcFut.thenCompose(svc -> svc.putAll(vals));
    }

    /**
     * @see MetaStorageService#getAndPutAll(Map)
     */
    public @NotNull CompletableFuture<Map<ByteArray, Entry>> getAndPutAll(@NotNull Map<ByteArray, byte[]> vals) {
        return metaStorageSvcFut.thenCompose(svc -> svc.getAndPutAll(vals));
    }

    /**
     * @see MetaStorageService#remove(ByteArray)
     */
    public @NotNull CompletableFuture<Void> remove(@NotNull ByteArray key) {
        return metaStorageSvcFut.thenCompose(svc -> svc.remove(key));
    }

    /**
     * @see MetaStorageService#getAndRemove(ByteArray)
     */
    public @NotNull CompletableFuture<Entry> getAndRemove(@NotNull ByteArray key) {
        return metaStorageSvcFut.thenCompose(svc -> svc.getAndRemove(key));
    }

    /**
     * @see MetaStorageService#removeAll(Set)
     */
    public @NotNull CompletableFuture<Void> removeAll(@NotNull Set<ByteArray> keys) {
        return metaStorageSvcFut.thenCompose(svc -> svc.removeAll(keys));
    }

    /**
     * @see MetaStorageService#getAndRemoveAll(Set)
     */
    public @NotNull CompletableFuture<Map<ByteArray, Entry>> getAndRemoveAll(@NotNull Set<ByteArray> keys) {
        return metaStorageSvcFut.thenCompose(svc -> svc.getAndRemoveAll(keys));
    }

    /**
     * Invoke with single success/failure operation.
     *
     * @see MetaStorageService#invoke(Condition, Operation, Operation)
     */
    public @NotNull CompletableFuture<Boolean> invoke(
        @NotNull Condition cond,
        @NotNull Operation success,
        @NotNull Operation failure
    ) {
        return metaStorageSvcFut.thenCompose(svc -> svc.invoke(cond, success, failure));
    }

    /**
     * @see MetaStorageService#invoke(Condition, Collection, Collection)
     */
    public @NotNull CompletableFuture<Boolean> invoke(
            @NotNull Condition cond,
            @NotNull Collection<Operation> success,
            @NotNull Collection<Operation> failure
    ) {
        return metaStorageSvcFut.thenCompose(svc -> svc.invoke(cond, success, failure));
    }

    /**
     * @see MetaStorageService#range(ByteArray, ByteArray, long)
     */
    public @NotNull Cursor<Entry> range(@NotNull ByteArray keyFrom, @Nullable ByteArray keyTo, long revUpperBound) {
        return new CursorWrapper<>(
                metaStorageSvcFut,
                metaStorageSvcFut.thenApply(svc -> svc.range(keyFrom, keyTo, revUpperBound))
        );
    }

    /**
     * Retrieves entries for the given key range in lexicographic order.
     * Entries will be filtered out by the current applied revision as an upper bound.
     * Applied revision is a revision of the last successful vault update.
     *
     * @param keyFrom Start key of range (inclusive). Couldn't be {@code null}.
     * @param keyTo End key of range (exclusive). Could be {@code null}.
     * @return Cursor built upon entries corresponding to the given range and applied revision.
     * @throws OperationTimeoutException If the operation is timed out.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction.
     * @see ByteArray
     * @see Entry
     */
    public @NotNull Cursor<Entry> rangeWithAppliedRevision(@NotNull ByteArray keyFrom, @Nullable ByteArray keyTo) {
        return new CursorWrapper<>(
            metaStorageSvcFut,
            metaStorageSvcFut.thenApply(svc -> {
                try {
                    return svc.range(keyFrom, keyTo, vaultMgr.appliedRevision());
                }
                catch (IgniteInternalCheckedException e) {
                    throw new IgniteInternalException(e);
                }
            })
        );
    }

    /**
     * @see MetaStorageService#range(ByteArray, ByteArray)
     */
    public @NotNull Cursor<Entry> range(@NotNull ByteArray keyFrom, @Nullable ByteArray keyTo) {
        return new CursorWrapper<>(
            metaStorageSvcFut,
            metaStorageSvcFut.thenApply(svc -> svc.range(keyFrom, keyTo))
        );
    }

    /**
     * @see MetaStorageService#compact()
     */
    public @NotNull CompletableFuture<Void> compact() {
        return metaStorageSvcFut.thenCompose(MetaStorageService::compact);
    }

    /**
     * Stop current batch of consolidated watches and register new one from current {@link WatchAggregator}.
     *
     * @return Ignite UUID of new consolidated watch.
     */
    private CompletableFuture<Optional<IgniteUuid>> updateWatches() {
        Long revision;
        try {
            revision = vaultMgr.appliedRevision() + 1;
        }
        catch (IgniteInternalCheckedException e) {
            throw new IgniteInternalException("Couldn't receive applied revision during watch redeploy", e);
        }

        final var finalRevision = revision;

        deployFut = deployFut
            .thenCompose(idOpt -> idOpt.map(id -> metaStorageSvcFut.thenCompose(svc -> svc.stopWatch(id))).
                orElse(CompletableFuture.completedFuture(null))
            .thenCompose(r -> {
                var watch = watchAggregator.watch(finalRevision, this::storeEntries);

                if (watch.isEmpty())
                    return CompletableFuture.completedFuture(Optional.empty());
                else
                    return dispatchAppropriateMetaStorageWatch(watch.get()).thenApply(Optional::of);
            }));

        return deployFut;
    }

    /**
     * Store entries with appropriate associated revision.
     *
     * @param entries to store.
     * @param revision associated revision.
     * @return future, which will be completed when store action finished.
     */
    private CompletableFuture<Void> storeEntries(Collection<IgniteBiTuple<ByteArray, byte[]>> entries, long revision) {
        try {
            return vaultMgr.putAll(entries.stream().collect(
                Collectors.toMap(e -> e.getKey(), IgniteBiTuple::getValue)), revision);
        }
        catch (IgniteInternalCheckedException e) {
            throw new IgniteInternalException("Couldn't put entries with considered revision.", e);
        }
    }

    /**
     * @param id of watch to redeploy.
     * @return future, which will be completed after redeploy finished.
     */
    private CompletableFuture<Long> waitForReDeploy(long id) {
        if (deployed)
            return updateWatches().thenApply(uid -> id);
        else
            return deployFut.thenApply(uid -> id);
    }

    /**
     * Checks whether the given node hosts meta storage.
     *
     * @param nodeName Node unique name.
     * @param metastorageMembers Meta storage members names.
     * @return {@code true} if the node has meta storage, {@code false} otherwise.
     */
    public static boolean hasMetastorage(String nodeName, String[] metastorageMembers) {
        boolean isNodeHasMetasorage = false;

        for (String name : metastorageMembers) {
            if (name.equals(nodeName)) {
                isNodeHasMetasorage = true;

                break;
            }
        }

        return isNodeHasMetasorage;
    }

    /**
     * Checks whether the local node hosts meta storage.
     *
     * @param configurationMgr Configuration manager.
     * @return {@code true} if the node has meta storage, {@code false} otherwise.
     */
    public static boolean hasMetastorageLocally(ConfigurationManager configurationMgr) {
        String locNodeName = configurationMgr
            .configurationRegistry()
            .getConfiguration(NodeConfiguration.KEY)
            .name()
            .value();

        String[] metastorageMembers = configurationMgr
            .configurationRegistry()
            .getConfiguration(NodeConfiguration.KEY)
            .metastorageNodes()
            .value();

        return hasMetastorage(locNodeName, metastorageMembers);
    }

    // TODO: IGNITE-14691 Temporally solution that should be removed after implementing reactive watches.
    /** Cursor wrapper. */
    private final class CursorWrapper<T> implements Cursor<T> {
        /** Meta storage service future. */
        private final CompletableFuture<MetaStorageService> metaStorageSvcFut;

        /** Inner cursor future. */
        private final CompletableFuture<Cursor<T>> innerCursorFut;

        /** Inner iterator future. */
        private final CompletableFuture<Iterator<T>> innerIterFut;

        private final InnerIterator it = new InnerIterator();

        /**
         * @param metaStorageSvcFut Meta storage service future.
         * @param innerCursorFut Inner cursor future.
         */
        CursorWrapper(
            CompletableFuture<MetaStorageService> metaStorageSvcFut,
            CompletableFuture<Cursor<T>> innerCursorFut
        ) {
            this.metaStorageSvcFut = metaStorageSvcFut;
            this.innerCursorFut = innerCursorFut;
            this.innerIterFut = innerCursorFut.thenApply(Iterable::iterator);
        }

            /** {@inheritDoc} */
        @Override public void close() throws Exception {
            innerCursorFut.thenCompose(cursor -> {
                try {
                    cursor.close();

                    return null;
                }
                catch (Exception e) {
                    throw new IgniteInternalException(e);
                }
            }).get();
        }

        /** {@inheritDoc} */
        @NotNull @Override public Iterator<T> iterator() {
            return it;
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public T next() {
            return it.next();
        }

        private class InnerIterator implements Iterator<T> {
            @Override public boolean hasNext() {
                try {
                    return innerIterFut.thenApply(Iterator::hasNext).get();
                }
                catch (InterruptedException | ExecutionException e) {
                    throw new IgniteInternalException(e);
                }
            }

            /** {@inheritDoc} */
            @Override public T next() {
                try {
                    return innerIterFut.thenApply(Iterator::next).get();
                }
                catch (InterruptedException | ExecutionException e) {
                    throw new IgniteInternalException(e);
                }
            }
        }
    }

    /**
     * Dispatches appropriate metastorage watch method according to inferred watch criterion.
     *
     * @param aggregatedWatch Aggregated watch.
     * @return Future, which will be completed after new watch registration finished.
     */
    private CompletableFuture<IgniteUuid> dispatchAppropriateMetaStorageWatch(AggregatedWatch aggregatedWatch) {
        if (aggregatedWatch.keyCriterion() instanceof KeyCriterion.CollectionCriterion) {
            var criterion = (KeyCriterion.CollectionCriterion) aggregatedWatch.keyCriterion();

            return metaStorageSvcFut.thenCompose(metaStorageSvc -> metaStorageSvc.watch(
                criterion.keys(),
                aggregatedWatch.revision(),
                aggregatedWatch.listener()));
        }
        else if (aggregatedWatch.keyCriterion() instanceof KeyCriterion.ExactCriterion) {
            var criterion = (KeyCriterion.ExactCriterion) aggregatedWatch.keyCriterion();

            return metaStorageSvcFut.thenCompose(metaStorageSvc -> metaStorageSvc.watch(
                criterion.key(),
                aggregatedWatch.revision(),
                aggregatedWatch.listener()));
        }
        else if (aggregatedWatch.keyCriterion() instanceof KeyCriterion.RangeCriterion) {
            var criterion = (KeyCriterion.RangeCriterion) aggregatedWatch.keyCriterion();

            return metaStorageSvcFut.thenCompose(metaStorageSvc -> metaStorageSvc.watch(
                criterion.from(),
                criterion.to(),
                aggregatedWatch.revision(),
                aggregatedWatch.listener()));
        }
        else
            throw new UnsupportedOperationException("Unsupported type of criterion");
    }
}
