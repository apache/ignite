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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.schema.BinaryRow;
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
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.schema.definition.SchemaManagementMode;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;

/**
 * Storage of table rows.
 */
public class InternalTableImpl implements InternalTable {
    //TODO: IGNITE-15443 Use IntMap structure instead of HashMap.
    /** Partition map. */
    private Map<Integer, RaftGroupService> partitionMap;

    /** Partitions. */
    private int partitions;

    /** Table name. */
    private String tableName;

    /** Table identifier. */
    private IgniteUuid tableId;

    /** Table schema mode. */
    private volatile SchemaManagementMode schemaMode;

    /**
     * @param tableName Table name.
     * @param tableId Table id.
     * @param partMap Map partition id to raft group.
     * @param partitions Partitions.
     */
    public InternalTableImpl(
        String tableName,
        IgniteUuid tableId,
        Map<Integer, RaftGroupService> partMap,
        int partitions
    ) {
        this.tableName = tableName;
        this.tableId = tableId;
        this.partitionMap = partMap;
        this.partitions = partitions;

        this.schemaMode = SchemaManagementMode.STRICT;
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteUuid tableId() {
        return tableId;
    }

    /** {@inheritDoc} */
    @Override public String tableName() {
        return tableName;
    }

    /** {@inheritDoc} */
    @Override public SchemaManagementMode schemaMode() {
        return schemaMode;
    }

    /** {@inheritDoc} */
    @Override public void schema(SchemaManagementMode schemaMode) {
        this.schemaMode = schemaMode;
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<BinaryRow> get(BinaryRow keyRow, Transaction tx) {
        return partitionMap.get(partId(keyRow)).<SingleRowResponse>run(new GetCommand(keyRow))
            .thenApply(SingleRowResponse::getValue);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Collection<BinaryRow>> getAll(Collection<BinaryRow> keyRows, Transaction tx) {
        Map<Integer, Set<BinaryRow>> keyRowsByPartition = mapRowsToPartitions(keyRows);

        CompletableFuture<MultiRowsResponse>[] futures = new CompletableFuture[keyRowsByPartition.size()];

        int batchNum = 0;

        for (Map.Entry<Integer, Set<BinaryRow>> partToRows : keyRowsByPartition.entrySet()) {
            futures[batchNum] = partitionMap.get(partToRows.getKey()).run(new GetAllCommand(partToRows.getValue()));

            batchNum++;
        }

        return collectMultiRowsResponses(futures);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> upsert(BinaryRow row, Transaction tx) {
        return partitionMap.get(partId(row)).run(new UpsertCommand(row));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> upsertAll(Collection<BinaryRow> rows, Transaction tx) {
        Map<Integer, Set<BinaryRow>> keyRowsByPartition = mapRowsToPartitions(rows);

        CompletableFuture<Void>[] futures = new CompletableFuture[keyRowsByPartition.size()];

        int batchNum = 0;

        for (Map.Entry<Integer, Set<BinaryRow>> partToRows : keyRowsByPartition.entrySet()) {
            futures[batchNum] = partitionMap.get(partToRows.getKey()).run(new UpsertAllCommand(partToRows.getValue()));

            batchNum++;
        }

        return CompletableFuture.allOf(futures);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<BinaryRow> getAndUpsert(BinaryRow row, Transaction tx) {
        return partitionMap.get(partId(row)).<SingleRowResponse>run(new GetAndUpsertCommand(row))
            .thenApply(SingleRowResponse::getValue);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> insert(BinaryRow row, Transaction tx) {
        return partitionMap.get(partId(row)).run(new InsertCommand(row));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Collection<BinaryRow>> insertAll(Collection<BinaryRow> rows, Transaction tx) {
        Map<Integer, Set<BinaryRow>> keyRowsByPartition = mapRowsToPartitions(rows);

        CompletableFuture<MultiRowsResponse>[] futures = new CompletableFuture[keyRowsByPartition.size()];

        int batchNum = 0;

        for (Map.Entry<Integer, Set<BinaryRow>> partToRows : keyRowsByPartition.entrySet()) {
            futures[batchNum] = partitionMap.get(partToRows.getKey()).run(new InsertAllCommand(partToRows.getValue()));

            batchNum++;
        }

        return collectMultiRowsResponses(futures);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> replace(BinaryRow row, Transaction tx) {
        return partitionMap.get(partId(row)).<Boolean>run(new ReplaceIfExistCommand(row));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> replace(BinaryRow oldRow, BinaryRow newRow,
        Transaction tx) {
        return partitionMap.get(partId(oldRow)).run(new ReplaceCommand(oldRow, newRow));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<BinaryRow> getAndReplace(BinaryRow row, Transaction tx) {
        return partitionMap.get(partId(row)).<SingleRowResponse>run(new GetAndReplaceCommand(row))
            .thenApply(SingleRowResponse::getValue);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> delete(BinaryRow keyRow, Transaction tx) {
        return partitionMap.get(partId(keyRow)).run(new DeleteCommand(keyRow));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> deleteExact(BinaryRow oldRow, Transaction tx) {
        return partitionMap.get(partId(oldRow)).<Boolean>run(new DeleteExactCommand(oldRow));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<BinaryRow> getAndDelete(BinaryRow row, Transaction tx) {
        return partitionMap.get(partId(row)).<SingleRowResponse>run(new GetAndDeleteCommand(row))
            .thenApply(SingleRowResponse::getValue);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Collection<BinaryRow>> deleteAll(Collection<BinaryRow> rows, Transaction tx) {
        Map<Integer, Set<BinaryRow>> keyRowsByPartition = mapRowsToPartitions(rows);

        CompletableFuture<MultiRowsResponse>[] futures = new CompletableFuture[keyRowsByPartition.size()];

        int batchNum = 0;

        for (Map.Entry<Integer, Set<BinaryRow>> partToRows : keyRowsByPartition.entrySet()) {
            futures[batchNum] = partitionMap.get(partToRows.getKey()).run(new DeleteAllCommand(partToRows.getValue()));

            batchNum++;
        }

        return collectMultiRowsResponses(futures);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Collection<BinaryRow>> deleteAllExact(
        Collection<BinaryRow> rows,
        Transaction tx
    ) {
        Map<Integer, Set<BinaryRow>> keyRowsByPartition = mapRowsToPartitions(rows);

        CompletableFuture<MultiRowsResponse>[] futures = new CompletableFuture[keyRowsByPartition.size()];

        int batchNum = 0;

        for (Map.Entry<Integer, Set<BinaryRow>> partToRows : keyRowsByPartition.entrySet()) {
            futures[batchNum] = partitionMap.get(partToRows.getKey()).run(new DeleteExactAllCommand(partToRows.getValue()));

            batchNum++;
        }

        return collectMultiRowsResponses(futures);
    }

    /**
     * Map rows to partitions.
     *
     * @param rows Rows.
     * @return Partition -%gt; rows mapping.
     */
    private Map<Integer, Set<BinaryRow>> mapRowsToPartitions(Collection<BinaryRow> rows) {
        //TODO: IGNITE-15443 Use IntMap structure instead of HashMap.
        HashMap<Integer, Set<BinaryRow>> keyRowsByPartition = new HashMap<>();

        for (BinaryRow keyRow : rows)
            keyRowsByPartition.computeIfAbsent(partId(keyRow), k -> new HashSet<>()).add(keyRow);

        return keyRowsByPartition;
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
     * Collects multirow responses from multiple futures into a single collection.
     * @param futures Futures.
     * @return Row collection.
     */
    private CompletableFuture<Collection<BinaryRow>> collectMultiRowsResponses(
            CompletableFuture<MultiRowsResponse>[] futures) {
        return CompletableFuture.allOf(futures)
                .thenApply(response -> {
                    List<BinaryRow> list = new ArrayList<>(futures.length);

                    for (CompletableFuture<MultiRowsResponse> future : futures) {
                        Collection<BinaryRow> values = future.join().getValues();

                        if (values != null)
                            list.addAll(values);
                    }

                    return list;
                });
    }
}
