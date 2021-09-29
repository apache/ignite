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

package org.apache.ignite.internal.table;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.schema.definition.SchemaManagementMode;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Internal table facade provides low-level methods for table operations.
 * The facade hides TX/replication protocol over table storage abstractions.
 */
public interface InternalTable {
    /**
     * Gets a table id.
     *
     * @return Table id as UUID.
     */
    @NotNull IgniteUuid tableId();

    /**
     * Gets a name of the table.
     *
     * @return Table name.
     */
    @NotNull String tableName();

    /**
     * Gets a schema mode of the table.
     *
     * @return Schema mode.
     */
    @NotNull SchemaManagementMode schemaMode();

    /**
     * Sets schema mode for the table.
     */
    void schema(SchemaManagementMode schemaMode);

    /**
     * Asynchronously gets a row with same key columns values as given one from the table.
     *
     * @param keyRow Row with key columns set.
     * @param tx The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<BinaryRow> get(BinaryRow keyRow, @Nullable Transaction tx);

    /**
     * Asynchronously get rows from the table.
     *
     * @param keyRows Rows with key columns set.
     * @param tx The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Collection<BinaryRow>> getAll(Collection<BinaryRow> keyRows, @Nullable Transaction tx);

    /**
     * Asynchronously inserts a row into the table if does not exist or replaces the existed one.
     *
     * @param row Row to insert into the table.
     * @param tx The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Void> upsert(BinaryRow row, @Nullable Transaction tx);

    /**
     * Asynchronously inserts a row into the table if does not exist or replaces the existed one.
     *
     * @param rows Rows to insert into the table.
     * @param tx The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Void> upsertAll(Collection<BinaryRow> rows, @Nullable Transaction tx);

    /**
     * Asynchronously inserts a row into the table or replaces if exists and return replaced previous row.
     *
     * @param row Row to insert into the table.
     * @param tx The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<BinaryRow> getAndUpsert(BinaryRow row, @Nullable Transaction tx);

    /**
     * Asynchronously inserts a row into the table if not exists.
     *
     * @param row Row to insert into the table.
     * @param tx The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Boolean> insert(BinaryRow row, @Nullable Transaction tx);

    /**
     * Asynchronously insert rows into the table which do not exist, skipping existed ones.
     *
     * @param rows Rows to insert into the table.
     * @param tx The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Collection<BinaryRow>> insertAll(Collection<BinaryRow> rows, @Nullable Transaction tx);

    /**
     * Asynchronously replaces an existed row associated with the same key columns values as the given one has.
     *
     * @param row Row to replace with.
     * @param tx The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Boolean> replace(BinaryRow row, @Nullable Transaction tx);

    /**
     * Asynchronously replaces an expected row in the table with the given new one.
     *
     * @param oldRow Row to replace.
     * @param newRow Row to replace with.
     * @param tx The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Boolean> replace(BinaryRow oldRow, BinaryRow newRow, @Nullable Transaction tx);

    /**
     * Asynchronously gets an existed row associated with the same key columns values as the given one has,
     * then replaces with the given one.
     *
     * @param row Row to replace with.
     * @param tx The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<BinaryRow> getAndReplace(BinaryRow row, @Nullable Transaction tx);

    /**
     * Asynchronously deletes a row with the same key columns values as the given one from the table.
     *
     * @param keyRow Row with key columns set.
     * @param tx The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Boolean> delete(BinaryRow keyRow, @Nullable Transaction tx);

    /**
     * Asynchronously deletes given row from the table.
     *
     * @param oldRow Row to delete.
     * @param tx The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Boolean> deleteExact(BinaryRow oldRow, @Nullable Transaction tx);

    /**
     * Asynchronously gets then deletes a row with the same key columns values from the table.
     *
     * @param row Row with key columns set.
     * @param tx The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<BinaryRow> getAndDelete(BinaryRow row, @Nullable Transaction tx);

    /**
     * Asynchronously remove rows with the same key columns values as the given one has from the table.
     *
     * @param rows Rows with key columns set.
     * @param tx The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Collection<BinaryRow>> deleteAll(Collection<BinaryRow> rows, @Nullable Transaction tx);

    /**
     * Asynchronously remove given rows from the table.
     *
     * @param rows Rows to delete.
     * @param tx The transaction.
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Collection<BinaryRow>> deleteAllExact(Collection<BinaryRow> rows,
        @Nullable Transaction tx);

    /**
     * Scans given partition, providing {@link Publisher<BinaryRow>} that reactively notifies about partition rows.
     *
     * @param p The partition.
     * @param tx The transaction.
     * @return {@link Publisher<BinaryRow>} that reactively notifies about partition rows.
     */
    @NotNull Publisher<BinaryRow> scan(int p, @Nullable Transaction tx);

    //TODO: IGNITE-14488. Add invoke() methods.
}
