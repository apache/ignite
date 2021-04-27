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

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.distributed.command.DeleteCommand;
import org.apache.ignite.internal.table.distributed.command.GetCommand;
import org.apache.ignite.internal.table.distributed.command.InsertCommand;
import org.apache.ignite.internal.table.distributed.command.ReplaceCommand;
import org.apache.ignite.internal.table.distributed.command.UpsertCommand;
import org.apache.ignite.internal.table.distributed.command.response.KVGetResponse;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.jetbrains.annotations.NotNull;

/**
 * Storage of table rows.
 */
public class InternalTableImpl implements InternalTable {
    /** Partition map. */
    private Map<Integer, RaftGroupService> partitionMap;

    /** Partitions. */
    private int partitions;

    /**
     * @param tableId Table id.
     * @param partMap Map partition id to raft group.
     * @param partitions Partitions.
     */
    public InternalTableImpl(
        UUID tableId,
        Map<Integer, RaftGroupService> partMap,
        int partitions
    ) {
        this.partitionMap = partMap;
        this.partitions = partitions;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<BinaryRow> get(BinaryRow keyRow) {
        return partitionMap.get(keyRow.hash() % partitions).<KVGetResponse>run(new GetCommand(keyRow))
            .thenApply(response -> response.getValue());
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<BinaryRow>> getAll(Collection<BinaryRow> keyRows) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> upsert(BinaryRow row) {
        return partitionMap.get(row.hash() % partitions).<Void>run(new UpsertCommand(row))
            .thenApply(response -> response);
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> upsertAll(Collection<BinaryRow> rows) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<BinaryRow> getAndUpsert(BinaryRow row) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> insert(BinaryRow row) {
        return partitionMap.get(row.hash() % partitions).<Boolean>run(new InsertCommand(row))
            .thenApply(response -> response);
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<BinaryRow>> insertAll(Collection<BinaryRow> rows) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> replace(BinaryRow row) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> replace(BinaryRow oldRow, BinaryRow newRow) {
        return partitionMap.get(oldRow.hash() % partitions).<Boolean>run(new ReplaceCommand(oldRow, newRow))
            .thenApply(response -> response);
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<BinaryRow> getAndReplace(BinaryRow row) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> delete(BinaryRow keyRow) {
        return partitionMap.get(keyRow.hash() % partitions).<Boolean>run(new DeleteCommand(keyRow))
            .thenApply(response -> response);
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> deleteExact(BinaryRow oldRow) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<BinaryRow> getAndDelete(BinaryRow row) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<BinaryRow>> deleteAll(Collection<BinaryRow> rows) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<BinaryRow>> deleteAllExact(Collection<BinaryRow> rows) {
        return null;
    }
}
