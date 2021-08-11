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
import org.apache.ignite.configuration.schemas.runner.NodeConfiguration;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.client.CompactedException;
import org.apache.ignite.internal.metastorage.client.Condition;
import org.apache.ignite.internal.metastorage.client.Entry;
import org.apache.ignite.internal.metastorage.client.MetaStorageService;
import org.apache.ignite.internal.metastorage.client.MetaStorageServiceImpl;
import org.apache.ignite.internal.metastorage.client.Operation;
import org.apache.ignite.internal.metastorage.client.OperationTimeoutException;
import org.apache.ignite.internal.metastorage.client.WatchListener;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageListener;
import org.apache.ignite.internal.metastorage.watch.AggregatedWatch;
import org.apache.ignite.internal.metastorage.watch.KeyCriterion;
import org.apache.ignite.internal.metastorage.watch.WatchAggregator;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.TopologyEventHandler;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.ByteUtils.bytesToLong;
import static org.apache.ignite.internal.util.ByteUtils.longToBytes;

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
public class MetaStorageManager implements IgniteComponent {
    /** Meta storage raft group name. */
    private static final String METASTORAGE_RAFT_GROUP_NAME = "metastorage_raft_group";

    /**
     * Special key for the vault where the applied revision for {@link MetaStorageManager#storeEntries}
     * operation is stored. This mechanism is needed for committing processed watches to {@link VaultManager}.
     */
    public static final ByteArray APPLIED_REV = ByteArray.fromString("applied_revision");

    /** Vault manager in order to commit processed watches with corresponding applied revision. */
    private final VaultManager vaultMgr;

    /** Configuration manager that handles local configuration. */
    private final ConfigurationManager locCfgMgr;

    /** Cluster network service that is used in order to handle cluster init message. */
    private final ClusterService clusterNetSvc;

    /** Raft manager that is used for metastorage raft group handling. */
    private final Loza raftMgr;

    /** Meta storage service. */
    private volatile CompletableFuture<MetaStorageService> metaStorageSvcFut;

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

    /** Flag indicates if meta storage nodes were set on start */
    private boolean metaStorageNodesOnStart;

    /**
     * The constructor.
     *
     * @param vaultMgr Vault manager.
     * @param locCfgMgr Local configuration manager.
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
        this.locCfgMgr = locCfgMgr;
        this.clusterNetSvc = clusterNetSvc;
        this.raftMgr = raftMgr;
        watchAggregator = new WatchAggregator();
        deployFut = new CompletableFuture<>();
    }

    /** {@inheritDoc} */
    @Override public void start() {
        String[] metastorageNodes = this.locCfgMgr.configurationRegistry().getConfiguration(NodeConfiguration.KEY)
            .metastorageNodes().value();

        Predicate<ClusterNode> metaStorageNodesContainsLocPred =
            clusterNode -> Arrays.asList(metastorageNodes).contains(clusterNode.name());

        if (metastorageNodes.length > 0) {
            metaStorageNodesOnStart = true;

            this.metaStorageSvcFut = CompletableFuture.completedFuture(new MetaStorageServiceImpl(
                    raftMgr.prepareRaftGroup(
                        METASTORAGE_RAFT_GROUP_NAME,
                        clusterNetSvc.topologyService().allMembers().stream().filter(
                            metaStorageNodesContainsLocPred).
                            collect(Collectors.toList()),
                        new MetaStorageListener(new SimpleInMemoryKeyValueStorage())
                    ),
                    clusterNetSvc.topologyService().localMember().id()
                )
            );

            if (hasMetastorageLocally(locCfgMgr)) {
                clusterNetSvc.topologyService().addEventHandler(new TopologyEventHandler() {
                    @Override public void onAppeared(ClusterNode member) {
                        // No-op.
                    }

                    @Override public void onDisappeared(ClusterNode member) {
                        metaStorageSvcFut.thenCompose(svc -> svc.closeCursors(member.id()));
                    }
                });
            }
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
//        clusterNetSvc.messagingService().addMessageHandler((message, senderAddr, correlationId) -> {});
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        // TODO: IGNITE-15161 Implement component's stop.
    }

    /**
     * Deploy all registered watches.
     */
    public synchronized void deployWatches() {
        var watch = watchAggregator.watch(
            appliedRevision() + 1,
            this::storeEntries
        );

        if (watch.isEmpty())
            deployFut.complete(Optional.empty());
        else {
            CompletableFuture<Void> fut =
                dispatchAppropriateMetaStorageWatch(watch.get()).thenAccept(id -> deployFut.complete(Optional.of(id)));

            if (metaStorageNodesOnStart)
                fut.join();
            else {
                // TODO: need to wait for this future in init phase https://issues.apache.org/jira/browse/IGNITE-14414
            }
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
            metaStorageSvcFut.thenApply(svc -> svc.range(keyFrom, keyTo, appliedRevision()))
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
     * Retrieves entries for the given key prefix in lexicographic order.
     * Entries will be filtered out by the current applied revision as an upper bound.
     * Applied revision is a revision of the last successful vault update.
     *
     * Prefix query is a synonym of the range query {@code (prefixKey, nextKey(prefixKey))}.
     *
     * @param keyPrefix Prefix of the key to retrieve the entries. Couldn't be {@code null}.
     * @return Cursor built upon entries corresponding to the given range and applied revision.
     * @throws OperationTimeoutException If the operation is timed out.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction.
     * @see ByteArray
     * @see Entry
     */
    public @NotNull Cursor<Entry> prefixWithAppliedRevision(@NotNull ByteArray keyPrefix) {
        var rangeCriterion = KeyCriterion.RangeCriterion.fromPrefixKey(keyPrefix);

        return new CursorWrapper<>(
            metaStorageSvcFut,
            metaStorageSvcFut.thenApply(svc -> svc.range(rangeCriterion.from(), rangeCriterion.to(), appliedRevision()))
        );
    }

    /**
     * Retrieves entries for the given key prefix in lexicographic order. Short cut for
     * {@link #prefix(ByteArray, long)} where {@code revUpperBound == -1}.
     *
     * @param keyPrefix Prefix of the key to retrieve the entries. Couldn't be {@code null}.
     * @return Cursor built upon entries corresponding to the given range and revision.
     * @throws OperationTimeoutException If the operation is timed out.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction.
     * @see ByteArray
     * @see Entry
     */
    public @NotNull Cursor<Entry> prefix(@NotNull ByteArray keyPrefix) {
        return prefix(keyPrefix, -1);
    }

    /**
     * Retrieves entries for the given key prefix in lexicographic order. Entries will be filtered out by upper bound
     * of given revision number.
     *
     * Prefix query is a synonym of the range query {@code range(prefixKey, nextKey(prefixKey))}.
     *
     * @param keyPrefix Prefix of the key to retrieve the entries. Couldn't be {@code null}.
     * @param revUpperBound  The upper bound for entry revision. {@code -1} means latest revision.
     * @return Cursor built upon entries corresponding to the given range and revision.
     * @throws OperationTimeoutException If the operation is timed out.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction.
     * @see ByteArray
     * @see Entry
     */
    public @NotNull Cursor<Entry> prefix(@NotNull ByteArray keyPrefix, long revUpperBound) {
        var rangeCriterion = KeyCriterion.RangeCriterion.fromPrefixKey(keyPrefix);
        return new CursorWrapper<>(
            metaStorageSvcFut,
            metaStorageSvcFut.thenApply(svc -> svc.range(rangeCriterion.from(), rangeCriterion.to(), revUpperBound))
        );
    }

    /**
     * @see MetaStorageService#compact()
     */
    public @NotNull CompletableFuture<Void> compact() {
        return metaStorageSvcFut.thenCompose(MetaStorageService::compact);
    }

    /**
     * @return Applied revision for {@link VaultManager#putAll} operation.
     */
    private long appliedRevision() {
        byte[] appliedRevision = vaultMgr.get(APPLIED_REV).join().value();

        return appliedRevision == null ? 0L : bytesToLong(appliedRevision);
    }

    /**
     * Stop current batch of consolidated watches and register new one from current {@link WatchAggregator}.
     *
     * @return Ignite UUID of new consolidated watch.
     */
    private CompletableFuture<Optional<IgniteUuid>> updateWatches() {
        long revision = appliedRevision() + 1;

        deployFut = deployFut
            .thenCompose(idOpt ->
                idOpt
                    .map(id -> metaStorageSvcFut.thenCompose(svc -> svc.stopWatch(id)))
                    .orElseGet(() -> CompletableFuture.completedFuture(null))
            )
            .thenCompose(r ->
                watchAggregator.watch(revision, this::storeEntries)
                    .map(watch -> dispatchAppropriateMetaStorageWatch(watch).thenApply(Optional::of))
                    .orElseGet(() -> CompletableFuture.completedFuture(Optional.empty()))
            );

        return deployFut;
    }

    /**
     * Store entries with appropriate associated revision.
     *
     * @param entries to store.
     * @param revision associated revision.
     */
    private void storeEntries(Collection<IgniteBiTuple<ByteArray, byte[]>> entries, long revision) {
        Map<ByteArray, byte[]> batch = IgniteUtils.newHashMap(entries.size() + 1);

        batch.put(APPLIED_REV, longToBytes(revision));

        entries.forEach(e -> batch.put(e.getKey(), e.getValue()));

        byte[] appliedRevisionBytes = vaultMgr.get(APPLIED_REV).join().value();

        long appliedRevision = appliedRevisionBytes == null ? 0L : bytesToLong(appliedRevisionBytes);

        if (revision <= appliedRevision) {
            throw new IgniteInternalException(String.format(
                "Current revision (%d) must be greater than the revision in the Vault (%d)",
                revision, appliedRevision
            ));
        }

        vaultMgr.putAll(batch).join();
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
    public boolean hasMetastorageLocally(ConfigurationManager configurationMgr) {
        String[] metastorageMembers = configurationMgr
            .configurationRegistry()
            .getConfiguration(NodeConfiguration.KEY)
            .metastorageNodes()
            .value();

        return hasMetastorage(vaultMgr.name().join(), metastorageMembers);
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
            innerCursorFut.thenApply(cursor -> {
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

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return it.hasNext();
        }

        /** {@inheritDoc} */
        @Override public T next() {
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
