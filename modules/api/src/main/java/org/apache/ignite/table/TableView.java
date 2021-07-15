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

package org.apache.ignite.table;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Table view interface provides methods to access table records.
 *
 * @param <R> Mapped record type.
 * @apiNote Some methods require a record with the only key columns set. This is not mandatory requirement
 * and value columns will be just ignored.
 */
public interface TableView<R> {
    /**
     * Gets a record with same key columns values as given one from the table.
     *
     * @param keyRec A record with key columns set.
     * The record cannot be {@code null}.
     * @return A record with all columns filled from the table.
     */
    R get(@NotNull R keyRec);

    /**
     * Asynchronously gets a record with same key columns values as given one from the table.
     *
     * @param keyRec A record with key columns set.
     * The record cannot be {@code null}.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<R> getAsync(@NotNull R keyRec);

    /**
     * Get records from the table.
     *
     * @param keyRecs Records with key columns set.
     * The records cannot be {@code null}.
     * @return Records with all columns filled from the table.
     */
    Collection<R> getAll(@NotNull Collection<R> keyRecs);

    /**
     * Asynchronously get records from the table.
     *
     * @param keyRecs Records with key columns set.
     * The records cannot be {@code null}.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Collection<R>> getAllAsync(@NotNull Collection<R> keyRecs);

    /**
     * Inserts a record into the table if does not exist or replaces the existed one.
     *
     * @param rec A record to insert into the table.
     * The record cannot be {@code null}.
     */
    void upsert(@NotNull R rec);

    /**
     * Asynchronously inserts a record into the table if does not exist or replaces the existed one.
     *
     * @param rec A record to insert into the table.
     * The record cannot be {@code null}.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Void> upsertAsync(@NotNull R rec);

    /**
     * Insert records into the table if does not exist or replaces the existed one.
     *
     * @param recs Records to insert into the table.
     * The records cannot be {@code null}.
     */
    void upsertAll(@NotNull Collection<R> recs);

    /**
     * Asynchronously inserts a record into the table if does not exist or replaces the existed one.
     *
     * @param recs Records to insert into the table.
     * The records cannot be {@code null}.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Void> upsertAllAsync(@NotNull Collection<R> recs);

    /**
     * Inserts a record into the table or replaces if exists and return replaced previous record.
     *
     * @param rec A record to insert into the table.
     * The record cannot be {@code null}.
     * @return Replaced record or {@code null} if not existed.
     */
    R getAndUpsert(@NotNull R rec);

    /**
     * Asynchronously inserts a record into the table or replaces if exists and return replaced previous record.
     *
     * @param rec A record to insert into the table.
     * The record cannot be {@code null}.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<R> getAndUpsertAsync(@NotNull R rec);

    /**
     * Inserts a record into the table if not exists.
     *
     * @param rec A record to insert into the table.
     * The record cannot be {@code null}.
     * @return {@code True} if successful, {@code false} otherwise.
     */
    boolean insert(@NotNull R rec);

    /**
     * Asynchronously inserts a record into the table if not exists.
     *
     * @param rec A record to insert into the table.
     * The record cannot be {@code null}.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> insertAsync(@NotNull R rec);

    /**
     * Insert records into the table which do not exist, skipping existed ones.
     *
     * @param recs Records to insert into the table.
     * The records cannot be {@code null}.
     * @return Skipped records.
     */
    Collection<R> insertAll(@NotNull Collection<R> recs);

    /**
     * Asynchronously insert records into the table which do not exist, skipping existed ones.
     *
     * @param recs Records to insert into the table.
     * The records cannot be {@code null}.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Collection<R>> insertAllAsync(@NotNull Collection<R> recs);

    /**
     * Replaces an existed record associated with the same key columns values as the given one has.
     *
     * @param rec A record to replace with.
     * The record cannot be {@code null}.
     * @return {@code True} if old record was found and replaced successfully, {@code false} otherwise.
     */
    boolean replace(@NotNull R rec);

    /**
     * Asynchronously replaces an existed record associated with the same key columns values as the given one has.
     *
     * @param rec A record to replace with.
     * The record cannot be {@code null}.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull R rec);

    /**
     * Replaces an expected record in the table with the given new one.
     *
     * @param oldRec A record to replace.
     * The record cannot be {@code null}.
     * @param newRec A record to replace with.
     * The record cannot be {@code null}.
     * @return {@code True} if the old record replaced successfully, {@code false} otherwise.
     */
    boolean replace(@NotNull R oldRec, @NotNull R newRec);

    /**
     * Asynchronously replaces an expected record in the table with the given new one.
     *
     * @param oldRec A record to replace.
     * The record cannot be {@code null}.
     * @param newRec A record to replace with.
     * The record cannot be {@code null}.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull R oldRec, @NotNull R newRec);

    /**
     * Gets an existed record associated with the same key columns values as the given one has,
     * then replaces with the given one.
     *
     * @param rec A record to replace with.
     * The record cannot be {@code null}.
     * @return Replaced record or {@code null} if not existed.
     */
    R getAndReplace(@NotNull R rec);

    /**
     * Asynchronously gets an existed record associated with the same key columns values as the given one has,
     * then replaces with the given one.
     *
     * @param rec A record to replace with.
     * The record cannot be {@code null}.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<R> getAndReplaceAsync(@NotNull R rec);

    /**
     * Deletes a record with the same key columns values as the given one from the table.
     *
     * @param keyRec A record with key columns set.
     * The record cannot be {@code null}.
     * @return {@code True} if removed successfully, {@code false} otherwise.
     */
    boolean delete(@NotNull R keyRec);

    /**
     * Asynchronously deletes a record with the same key columns values as the given one from the table.
     *
     * @param keyRec A record with key columns set.
     * The record cannot be {@code null}.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> deleteAsync(@NotNull R keyRec);

    /**
     * Deletes the given record from the table.
     *
     * @param rec A record to delete.
     * The record cannot be {@code null}.
     * @return {@code True} if removed successfully, {@code false} otherwise.
     */
    boolean deleteExact(@NotNull R rec);

    /**
     * Asynchronously deletes given record from the table.
     *
     * @param rec A record to delete.
     * The record cannot be {@code null}.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> deleteExactAsync(@NotNull R rec);

    /**
     * Gets then deletes a record with the same key columns values from the table.
     *
     * @param rec A record with key columns set.
     * The record cannot be {@code null}.
     * @return Removed record or {@code null} if not existed.
     */
    R getAndDelete(@NotNull R rec);

    /**
     * Asynchronously gets then deletes a record with the same key columns values from the table.
     *
     * @param rec A record with key columns set.
     * The record cannot be {@code null}.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<R> getAndDeleteAsync(@NotNull R rec);

    /**
     * Remove records with the same key columns values as the given one has from the table.
     *
     * @param recs Records with key columns set.
     * The records cannot be {@code null}.
     * @return Records with key columns set that were not exists.
     */
    Collection<R> deleteAll(@NotNull Collection<R> recs);

    /**
     * Asynchronously remove records with the same key columns values as the given one has from the table.
     *
     * @param recs Records with key columns set.
     * The records cannot be {@code null}.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Collection<R>> deleteAllAsync(@NotNull Collection<R> recs);

    /**
     * Remove given records from the table.
     *
     * @param recs Records to delete.
     * The records cannot be {@code null}.
     * @return Records that were not deleted.
     */
    Collection<R> deleteAllExact(@NotNull Collection<R> recs);

    /**
     * Asynchronously remove given records from the table.
     *
     * @param recs Records to delete.
     * The records cannot be {@code null}.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Collection<R>> deleteAllExactAsync(@NotNull Collection<R> recs);

    /**
     * Executes an InvokeProcessor code against a record with the same key columns values as the given one has.
     *
     * @param keyRec A record with key columns set.
     * The record cannot be {@code null}.
     * @param proc Invoke processor.
     * @param <T> InvokeProcessor result type.
     * @return Results of the processing.
     */
    <T extends Serializable> T invoke(@NotNull R keyRec, InvokeProcessor<R, R, T> proc);

    /**
     * Asynchronously executes an InvokeProcessor code against a record
     * with the same key columns values as the given one has.
     *
     * @param keyRec A record with key columns set.
     * The record cannot be {@code null}.
     * @param proc Invoke processor.
     * @param <T> InvokeProcessor result type.
     * @return Future representing pending completion of the operation.
     */
    @NotNull <T extends Serializable> CompletableFuture<T> invokeAsync(@NotNull R keyRec, InvokeProcessor<R, R, T> proc);

    /**
     * Executes an InvokeProcessor code against records with the same key columns values as the given ones has.
     *
     * @param keyRecs Records with key columns set.
     * The records cannot be {@code null}.
     * @param proc Invoke processor.
     * @param <T> InvokeProcessor result type.
     * @return Results of the processing.
     */
    <T extends Serializable> Map<R, T> invokeAll(@NotNull Collection<R> keyRecs, InvokeProcessor<R, R, T> proc);

    /**
     * Asynchronously executes an InvokeProcessor against records with the same key columns values as the given ones
     * has.
     *
     * @param keyRecs Records with key columns set.
     * The records cannot be {@code null}.
     * @param proc Invoke processor.
     * @param <T> InvokeProcessor result type.
     * @return Results of the processing.
     */
    @NotNull <T extends Serializable> CompletableFuture<Map<R, T>> invokeAllAsync(@NotNull Collection<R> keyRecs,
        InvokeProcessor<R, R, T> proc);

    /**
     * @return Current transaction or null if a table is not enlisted in a transaction.
     */
    @Nullable Transaction transaction();

    /**
     * Enslists a view into the transaction.
     *
     * @param tx The transaction.
     * @return Transactional view.
     */
    TableView<R> withTransaction(Transaction tx);
}
