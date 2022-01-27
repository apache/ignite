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

package org.apache.ignite.internal.table.distributed.storage;

import static java.util.concurrent.CompletableFuture.completedFuture;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.engine.TableStorage;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.distributed.command.DeleteAllCommand;
import org.apache.ignite.internal.table.distributed.command.DeleteCommand;
import org.apache.ignite.internal.table.distributed.command.DeleteExactAllCommand;
import org.apache.ignite.internal.table.distributed.command.DeleteExactCommand;
import org.apache.ignite.internal.table.distributed.command.GetAllCommand;
import org.apache.ignite.internal.table.distributed.command.GetAndDeleteCommand;
import org.apache.ignite.internal.table.distributed.command.GetAndReplaceCommand;
import org.apache.ignite.internal.table.distributed.command.GetAndUpsertCommand;
import org.apache.ignite.internal.table.distributed.command.GetCommand;
import org.apache.ignite.internal.table.distributed.command.InsertAllCommand;
import org.apache.ignite.internal.table.distributed.command.InsertCommand;
import org.apache.ignite.internal.table.distributed.command.ReplaceCommand;
import org.apache.ignite.internal.table.distributed.command.ReplaceIfExistCommand;
import org.apache.ignite.internal.table.distributed.command.UpsertAllCommand;
import org.apache.ignite.internal.table.distributed.command.UpsertCommand;
import org.apache.ignite.internal.table.distributed.command.response.MultiRowsResponse;
import org.apache.ignite.internal.table.distributed.command.response.SingleRowResponse;
import org.apache.ignite.internal.table.distributed.command.scan.ScanCloseCommand;
import org.apache.ignite.internal.table.distributed.command.scan.ScanInitCommand;
import org.apache.ignite.internal.table.distributed.command.scan.ScanRetrieveBatchCommand;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.lang.IgniteUuidGenerator;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Storage of table rows.
 */
public class InternalTableImpl implements InternalTable {
    /** Log. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(InternalTableImpl.class);

    /** IgniteUuid generator. */
    private static final IgniteUuidGenerator UUID_GENERATOR = new IgniteUuidGenerator(UUID.randomUUID(), 0);

    /** Partition map. */
    protected final Int2ObjectMap<RaftGroupService> partitionMap;

    /** Partitions. */
    private final int partitions;

    /** Table name. */
    private final String tableName;

    /** Table identifier. */
    private final IgniteUuid tableId;

    /** Resolver that resolves a network address to node id. */
    private final Function<NetworkAddress, String> netAddrResolver;

    /** Transactional manager. */
    private final TxManager txManager;

    /** Storage for table data. */
    private final TableStorage tableStorage;

    /**
     * Constructor.
     *
     * @param tableName Table name.
     * @param tableId Table id.
     * @param partMap Map partition id to raft group.
     * @param partitions Partitions.
     * @param txManager Transaction manager.
     * @param tableStorage Table storage.
     */
    public InternalTableImpl(
            String tableName,
            IgniteUuid tableId,
            Int2ObjectMap<RaftGroupService> partMap,
            int partitions,
            Function<NetworkAddress, String> netAddrResolver,
            TxManager txManager,
            TableStorage tableStorage
    ) {
        this.tableName = tableName;
        this.tableId = tableId;
        this.partitionMap = partMap;
        this.partitions = partitions;
        this.netAddrResolver = netAddrResolver;
        this.txManager = txManager;
        this.tableStorage = tableStorage;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull TableStorage storage() {
        return tableStorage;
    }

    /** {@inheritDoc} */
    @Override
    public int partitions() {
        return partitionMap.size();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull IgniteUuid tableId() {
        return tableId;
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return tableName;
    }

    /**
     * Enlists multiple rows into a transaction.
     *
     * @param keyRows Rows.
     * @param tx The transaction.
     * @param op Command factory.
     * @param reducer The reducer.
     * @param <R> Reducer's input.
     * @param <T> Reducer's output.
     * @return The future.
     */
    private <R, T> CompletableFuture<T> enlistInTx(
            Collection<BinaryRow> keyRows,
            InternalTransaction tx,
            BiFunction<Collection<BinaryRow>, InternalTransaction, Command> op,
            Function<CompletableFuture<R>[], CompletableFuture<T>> reducer
    ) {
        final boolean implicit = tx == null;

        final InternalTransaction tx0 = implicit ? txManager.begin() : tx;

        Int2ObjectOpenHashMap<List<BinaryRow>> keyRowsByPartition = mapRowsToPartitions(keyRows);

        CompletableFuture<R>[] futures = new CompletableFuture[keyRowsByPartition.size()];

        int batchNum = 0;

        for (Int2ObjectOpenHashMap.Entry<List<BinaryRow>> partToRows : keyRowsByPartition.int2ObjectEntrySet()) {
            CompletableFuture<RaftGroupService> fut = enlist(partToRows.getIntKey(), tx0);

            futures[batchNum++] = fut.thenCompose(svc -> svc.run(op.apply(partToRows.getValue(), tx0)));
        }

        CompletableFuture<T> fut = reducer.apply(futures);

        return postEnlist(fut, implicit, tx0);
    }

    /**
     * Enlists a single row into a transaction.
     *
     * @param row The row.
     * @param tx The transaction.
     * @param op Command factory.
     * @param trans Transform closure.
     * @param <R> Transform input.
     * @param <T> Transform output.
     * @return The future.
     */
    private <R, T> CompletableFuture<T> enlistInTx(
            BinaryRow row,
            InternalTransaction tx,
            Function<InternalTransaction, Command> op,
            Function<R, T> trans
    ) {
        final boolean implicit = tx == null;

        final InternalTransaction tx0 = implicit ? txManager.begin() : tx;

        int partId = partId(row);

        CompletableFuture<T> fut = enlist(partId, tx0).thenCompose(svc -> svc.<R>run(op.apply(tx0)).thenApply(trans::apply));

        return postEnlist(fut, implicit, tx0);
    }

    /**
     * Performs post enlist operation.
     *
     * @param fut The future.
     * @param implicit {@code true} for implicit tx.
     * @param tx0 The transaction.
     * @param <T> Operation return type.
     * @return The future.
     */
    private <T> CompletableFuture<T> postEnlist(CompletableFuture<T> fut, boolean implicit, InternalTransaction tx0) {
        return fut.handle(new BiFunction<T, Throwable, CompletableFuture<T>>() {
            @Override
            public CompletableFuture<T> apply(T r, Throwable e) {
                if (e != null) {
                    return tx0.rollbackAsync().handle((ignored, err) -> {
                        if (err != null) {
                            e.addSuppressed(err);
                        }

                        throw (RuntimeException) e;
                    }); // Preserve failed state.
                } else {
                    return implicit ? tx0.commitAsync().thenApply(ignored -> r) : completedFuture(r);
                }
            }
        }).thenCompose(x -> x);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> get(BinaryRow keyRow, InternalTransaction tx) {
        return enlistInTx(keyRow, tx, tx0 -> new GetCommand(keyRow, tx0.timestamp()), SingleRowResponse::getValue);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<BinaryRow>> getAll(Collection<BinaryRow> keyRows, InternalTransaction tx) {
        return enlistInTx(keyRows, tx, (rows0, tx0) -> new GetAllCommand(rows0, tx0.timestamp()), this::collectMultiRowsResponses);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> upsert(BinaryRow row, InternalTransaction tx) {
        return enlistInTx(row, tx, tx0 -> new UpsertCommand(row, tx0.timestamp()), ignored -> null);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> upsertAll(Collection<BinaryRow> rows, InternalTransaction tx) {
        return enlistInTx(rows, tx, (rows0, tx0) -> new UpsertAllCommand(rows0, tx0.timestamp()), CompletableFuture::allOf);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> getAndUpsert(BinaryRow row, InternalTransaction tx) {
        return enlistInTx(row, tx, tx0 -> new GetAndUpsertCommand(row, tx0.timestamp()), SingleRowResponse::getValue);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> insert(BinaryRow row, InternalTransaction tx) {
        return enlistInTx(row, tx, tx0 -> new InsertCommand(row, tx0.timestamp()), r -> (Boolean) r);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<BinaryRow>> insertAll(Collection<BinaryRow> rows, InternalTransaction tx) {
        return enlistInTx(rows, tx, (rows0, tx0) -> new InsertAllCommand(rows0, tx0.timestamp()), this::collectMultiRowsResponses);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> replace(BinaryRow row, InternalTransaction tx) {
        return enlistInTx(row, tx, tx0 -> new ReplaceIfExistCommand(row, tx0.timestamp()), r -> (Boolean) r);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> replace(BinaryRow oldRow, BinaryRow newRow, InternalTransaction tx) {
        return enlistInTx(oldRow, tx, tx0 -> new ReplaceCommand(oldRow, newRow, tx0.timestamp()), r -> (Boolean) r);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> getAndReplace(BinaryRow row, InternalTransaction tx) {
        return enlistInTx(row, tx, tx0 -> new GetAndReplaceCommand(row, tx0.timestamp()), SingleRowResponse::getValue);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> delete(BinaryRow keyRow, InternalTransaction tx) {
        return enlistInTx(keyRow, tx, tx0 -> new DeleteCommand(keyRow, tx0.timestamp()), r -> (Boolean) r);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> deleteExact(BinaryRow oldRow, InternalTransaction tx) {
        return enlistInTx(oldRow, tx, tx0 -> new DeleteExactCommand(oldRow, tx0.timestamp()), r -> (Boolean) r);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> getAndDelete(BinaryRow row, InternalTransaction tx) {
        return enlistInTx(row, tx, tx0 -> new GetAndDeleteCommand(row, tx0.timestamp()), SingleRowResponse::getValue);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<BinaryRow>> deleteAll(Collection<BinaryRow> rows, InternalTransaction tx) {
        return enlistInTx(rows, tx, (rows0, tx0) -> new DeleteAllCommand(rows0, tx0.timestamp()), this::collectMultiRowsResponses);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<BinaryRow>> deleteAllExact(
            Collection<BinaryRow> rows,
            InternalTransaction tx
    ) {
        return enlistInTx(rows, tx, (rows0, tx0) -> new DeleteExactAllCommand(rows0, tx0.timestamp()), this::collectMultiRowsResponses);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull Publisher<BinaryRow> scan(int p, @Nullable InternalTransaction tx) {
        if (p < 0 || p >= partitions) {
            throw new IllegalArgumentException(
                    IgniteStringFormatter.format(
                            "Invalid partition [partition={}, minValue={}, maxValue={}].",
                            p,
                            0,
                            partitions - 1
                    )
            );
        }

        return new PartitionScanPublisher(partitionMap.get(p));
    }

    /**
     * Map rows to partitions.
     *
     * @param rows Rows.
     * @return Partition -%gt; rows mapping.
     */
    private Int2ObjectOpenHashMap<List<BinaryRow>> mapRowsToPartitions(Collection<BinaryRow> rows) {
        Int2ObjectOpenHashMap<List<BinaryRow>> keyRowsByPartition = new Int2ObjectOpenHashMap<>();

        for (BinaryRow keyRow : rows) {
            keyRowsByPartition.computeIfAbsent(partId(keyRow), k -> new ArrayList<>()).add(keyRow);
        }

        return keyRowsByPartition;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull List<String> assignments() {
        awaitLeaderInitialization();

        return partitionMap.int2ObjectEntrySet().stream()
                .sorted(Comparator.comparingInt(Int2ObjectOpenHashMap.Entry::getIntKey))
                .map(Map.Entry::getValue)
                .map(RaftGroupService::leader)
                .map(Peer::address)
                .map(netAddrResolver)
                .collect(Collectors.toList());
    }

    private void awaitLeaderInitialization() {
        List<CompletableFuture<Void>> futs = new ArrayList<>();

        for (RaftGroupService raftSvc : partitionMap.values()) {
            if (raftSvc.leader() == null) {
                futs.add(raftSvc.refreshLeader());
            }
        }

        CompletableFuture.allOf(futs.toArray(CompletableFuture[]::new)).join();
    }

    /** {@inheritDoc} */
    @TestOnly
    @Override
    public int partition(BinaryRow keyRow) {
        return partId(keyRow);
    }

    /**
     * Get partition id by key row.
     *
     * @param row Key row.
     * @return partition id.
     */
    private int partId(BinaryRow row) {
        int partId = row.hash() % partitions;

        return (partId < 0) ? -partId : partId;
    }

    /**
     * Returns a transaction manager.
     *
     * @return Transaction manager.
     */
    @TestOnly
    public TxManager transactionManager() {
        return txManager;
    }

    /**
     * TODO asch keep the same order as for keys Collects multirow responses from multiple futures into a single collection IGNITE-16004.
     *
     * @param futs Futures.
     * @return Row collection.
     */
    private CompletableFuture<Collection<BinaryRow>> collectMultiRowsResponses(CompletableFuture<?>[] futs) {
        return CompletableFuture.allOf(futs)
                .thenApply(response -> {
                    List<BinaryRow> list = new ArrayList<>(futs.length);

                    for (CompletableFuture<?> future : futs) {
                        MultiRowsResponse ret = (MultiRowsResponse) future.join();

                        List<BinaryRow> values = ret.getValues();

                        if (values != null) {
                            list.addAll(values);
                        }
                    }

                    return list;
                });
    }

    /**
     * Updates internal table raft group service for given partition.
     *
     * @param p Partition.
     * @param raftGrpSvc Raft group service.
     */
    public void updateInternalTableRaftGroupService(int p, RaftGroupService raftGrpSvc) {
        RaftGroupService oldSrvc = partitionMap.put(p, raftGrpSvc);

        if (oldSrvc != null) {
            oldSrvc.shutdown();
        }
    }

    /**
     * Enlists a partition.
     *
     * @param partId Partition id.
     * @param tx     The transaction.
     * @return The enlist future (then will a leader become known).
     */
    protected CompletableFuture<RaftGroupService> enlist(int partId, InternalTransaction tx) {
        RaftGroupService svc = partitionMap.get(partId);

        CompletableFuture<Void> fut0 = svc.leader() == null ? svc.refreshLeader() : completedFuture(null);

        // TODO asch IGNITE-15091 fixme need to map to the same leaseholder.
        // TODO asch a leader race is possible when enlisting different keys from the same partition.
        return fut0.thenAccept(ignored -> tx.enlist(svc)).thenApply(ignored -> svc); // Enlist the leaseholder.
    }

    /**
     * Partition scan publisher.
     */
    private static class PartitionScanPublisher implements Publisher<BinaryRow> {
        /** {@link Publisher} that relatively notifies about partition rows. */
        private final RaftGroupService raftGrpSvc;

        private AtomicBoolean subscribed;

        /**
         * The constructor.
         *
         * @param raftGrpSvc {@link RaftGroupService} to run corresponding raft commands.
         */
        PartitionScanPublisher(RaftGroupService raftGrpSvc) {
            this.raftGrpSvc = raftGrpSvc;
            this.subscribed = new AtomicBoolean(false);
        }

        /** {@inheritDoc} */
        @Override
        public void subscribe(Subscriber<? super BinaryRow> subscriber) {
            if (subscriber == null) {
                throw new NullPointerException("Subscriber is null");
            }

            if (!subscribed.compareAndSet(false, true)) {
                subscriber.onError(new IllegalStateException("Scan publisher does not support multiple subscriptions."));
            }

            PartitionScanSubscription subscription = new PartitionScanSubscription(subscriber);

            subscriber.onSubscribe(subscription);
        }

        /**
         * Partition Scan Subscription.
         */
        private class PartitionScanSubscription implements Subscription {
            private final Subscriber<? super BinaryRow> subscriber;

            private final AtomicBoolean canceled;

            /**
             * Scan id to uniquely identify it on server side.
             */
            private final IgniteUuid scanId;

            /**
             * Scan initial operation that created server cursor.
             */
            private final CompletableFuture<Void> scanInitOp;

            /**
             * The constructor.
             *
             * @param subscriber The subscriber.
             */
            private PartitionScanSubscription(Subscriber<? super BinaryRow> subscriber) {
                this.subscriber = subscriber;
                this.canceled = new AtomicBoolean(false);
                this.scanId = UUID_GENERATOR.randomUuid();
                // TODO: IGNITE-15544 Close partition scans on node left.
                this.scanInitOp = raftGrpSvc.run(new ScanInitCommand("", scanId));
            }

            /** {@inheritDoc} */
            @Override
            public void request(long n) {
                if (n <= 0) {
                    cancel();

                    subscriber.onError(new IllegalArgumentException(IgniteStringFormatter
                            .format("Invalid requested amount of items [requested={}, minValue=1]", n))
                    );
                }

                if (canceled.get()) {
                    return;
                }

                final int internalBatchSize = Integer.MAX_VALUE;

                for (int intBatchCnr = 0; intBatchCnr < (n / internalBatchSize); intBatchCnr++) {
                    scanBatch(internalBatchSize);
                }

                scanBatch((int) (n % internalBatchSize));
            }

            /** {@inheritDoc} */
            @Override
            public void cancel() {
                cancel(true);
            }

            /**
             * Cancels given subscription and closes cursor if necessary.
             *
             * @param closeCursor If {@code true} closes inner storage scan.
             */
            private void cancel(boolean closeCursor) {
                if (!canceled.compareAndSet(false, true)) {
                    return;
                }

                if (closeCursor) {
                    scanInitOp.thenRun(() -> raftGrpSvc.run(new ScanCloseCommand(scanId))).exceptionally(closeT -> {
                        LOG.warn("Unable to close scan.", closeT);

                        return null;
                    });
                }
            }

            /**
             * Requests and processes n requested elements where n is an integer.
             *
             * @param n Requested amount of items.
             */
            private void scanBatch(int n) {
                if (canceled.get()) {
                    return;
                }

                scanInitOp.thenCompose((none) -> raftGrpSvc.<MultiRowsResponse>run(new ScanRetrieveBatchCommand(n, scanId)))
                        .thenAccept(
                                res -> {
                                    if (res.getValues() == null) {
                                        cancel();

                                        subscriber.onComplete();

                                        return;
                                    } else {
                                        res.getValues().forEach(subscriber::onNext);
                                    }

                                    if (res.getValues().size() < n) {
                                        cancel();

                                        subscriber.onComplete();
                                    }
                                })
                        .exceptionally(
                                t -> {
                                    cancel(!scanInitOp.isCompletedExceptionally());

                                    subscriber.onError(t);

                                    return null;
                                });
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws Exception {
        for (RaftGroupService srv : partitionMap.values()) {
            srv.shutdown();
        }
    }
}
