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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.schema.SchemaMode;
import org.jetbrains.annotations.NotNull;

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
    @NotNull UUID tableId();

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
    @NotNull SchemaMode schemaMode();

    /**
     * Sets schema mode for the table.
     */
    void schema(SchemaMode schemaMode);

    /**
     * Asynchronously gets a row with same key columns values as given one from the table.
     *
     * @param keyRow Row with key columns set.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<BinaryRow> get(BinaryRow keyRow);

    /**
     * Asynchronously get rows from the table.
     *
     * @param keyRows Rows with key columns set.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Collection<BinaryRow>> getAll(Collection<BinaryRow> keyRows);

    /**
     * Asynchronously inserts a row into the table if does not exist or replaces the existed one.
     *
     * @param row Row to insert into the table.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Void> upsert(BinaryRow row);

    /**
     * Asynchronously inserts a row into the table if does not exist or replaces the existed one.
     *
     * @param rows Rows to insert into the table.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Void> upsertAll(Collection<BinaryRow> rows);

    /**
     * Asynchronously inserts a row into the table or replaces if exists and return replaced previous row.
     *
     * @param row Row to insert into the table.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<BinaryRow> getAndUpsert(BinaryRow row);

    /**
     * Asynchronously inserts a row into the table if not exists.
     *
     * @param row Row to insert into the table.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> insert(BinaryRow row);

    /**
     * Asynchronously insert rows into the table which do not exist, skipping existed ones.
     *
     * @param rows Rows to insert into the table.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Collection<BinaryRow>> insertAll(Collection<BinaryRow> rows);

    /**
     * Asynchronously replaces an existed row associated with the same key columns values as the given one has.
     *
     * @param row Row to replace with.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> replace(BinaryRow row);

    /**
     * Asynchronously replaces an expected row in the table with the given new one.
     *
     * @param oldRow Row to replace.
     * @param newRow Row to replace with.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> replace(BinaryRow oldRow, BinaryRow newRow);

    /**
     * Asynchronously gets an existed row associated with the same key columns values as the given one has,
     * then replaces with the given one.
     *
     * @param row Row to replace with.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<BinaryRow> getAndReplace(BinaryRow row);

    /**
     * Asynchronously deletes a row with the same key columns values as the given one from the table.
     *
     * @param keyRow Row with key columns set.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> delete(BinaryRow keyRow);

    /**
     * Asynchronously deletes given row from the table.
     *
     * @param oldRow Row to delete.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> deleteExact(BinaryRow oldRow);

    /**
     * Asynchronously gets then deletes a row with the same key columns values from the table.
     *
     * @param row Row with key columns set.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<BinaryRow> getAndDelete(BinaryRow row);

    /**
     * Asynchronously remove rows with the same key columns values as the given one has from the table.
     *
     * @param rows Rows with key columns set.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Collection<BinaryRow>> deleteAll(Collection<BinaryRow> rows);

    /**
     * Asynchronously remove given rows from the table.
     *
     * @param rows Rows to delete.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Collection<BinaryRow>> deleteAllExact(Collection<BinaryRow> rows);

    //TODO: IGNTIE-14488. Add invoke() methods.
}
