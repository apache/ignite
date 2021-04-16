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
import org.jetbrains.annotations.NotNull;

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
     * @param keyRec Record with key columns set.
     * @return Record with all columns filled from the table.
     */
    R get(R keyRec);

    /**
     * Asynchronously gets a record with same key columns values as given one from the table.
     *
     * @param keyRec Record with key columns set.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<R> getAsync(R keyRec);

    /**
     * Get records from the table.
     *
     * @param keyRecs Records with key columns set.
     * @return Records with all columns filled from the table.
     */
    Collection<R> getAll(Collection<R> keyRecs);

    /**
     * Asynchronously get records from the table.
     *
     * @param keyRecs Records with key columns set.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Collection<R>> getAllAsync(Collection<R> keyRecs);

    /**
     * Inserts a record into the table if does not exist or replaces the existed one.
     *
     * @param rec Record to insert into the table.
     */
    void upsert(R rec);

    /**
     * Asynchronously inserts a record into the table if does not exist or replaces the existed one.
     *
     * @param rec Record to insert into the table.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Void> upsertAsync(R rec);

    /**
     * Insert records into the table if does not exist or replaces the existed one.
     *
     * @param recs Records to insert into the table.
     */
    void upsertAll(Collection<R> recs);

    /**
     * Asynchronously inserts a record into the table if does not exist or replaces the existed one.
     *
     * @param recs Records to insert into the table.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Void> upsertAllAsync(Collection<R> recs);

    /**
     * Inserts a record into the table or replaces if exists and return replaced previous record.
     *
     * @param rec Record to insert into the table.
     * @return Replaced record or {@code null} if not existed.
     */
    R getAndUpsert(R rec);

    /**
     * Asynchronously inserts a record into the table or replaces if exists and return replaced previous record.
     *
     * @param rec Record to insert into the table.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<R> getAndUpsertAsync(R rec);

    /**
     * Inserts a record into the table if not exists.
     *
     * @param rec Record to insert into the table.
     * @return {@code True} if successful, {@code false} otherwise.
     */
    boolean insert(R rec);

    /**
     * Asynchronously inserts a record into the table if not exists.
     *
     * @param rec Record to insert into the table.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> insertAsync(R rec);

    /**
     * Insert records into the table which do not exist, skipping existed ones.
     *
     * @param recs Records to insert into the table.
     * @return Skipped records.
     */
    Collection<R> insertAll(Collection<R> recs);

    /**
     * Asynchronously insert records into the table which do not exist, skipping existed ones.
     *
     * @param recs Records to insert into the table.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Collection<R>> insertAllAsync(Collection<R> recs);

    /**
     * Replaces an existed record associated with the same key columns values as the given one has.
     *
     * @param rec Record to replace with.
     * @return {@code True} if old record was found and replaced successfully, {@code false} otherwise.
     */
    boolean replace(R rec);

    /**
     * Asynchronously replaces an existed record associated with the same key columns values as the given one has.
     *
     * @param rec Record to replace with.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> replaceAsync(R rec);

    /**
     * Replaces an expected record in the table with the given new one.
     *
     * @param oldRec Record to replace.
     * @param newRec Record to replace with.
     * @return {@code True} if the old record replaced successfully, {@code false} otherwise.
     */
    boolean replace(R oldRec, R newRec);

    /**
     * Asynchronously replaces an expected record in the table with the given new one.
     *
     * @param oldRec Record to replace.
     * @param newRec Record to replace with.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> replaceAsync(R oldRec, R newRec);

    /**
     * Gets an existed record associated with the same key columns values as the given one has,
     * then replaces with the given one.
     *
     * @param rec Record to replace with.
     * @return Replaced record or {@code null} if not existed.
     */
    R getAndReplace(R rec);

    /**
     * Asynchronously gets an existed record associated with the same key columns values as the given one has,
     * then replaces with the given one.
     *
     * @param rec Record to replace with.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<R> getAndReplaceAsync(R rec);

    /**
     * Deletes a record with the same key columns values as the given one from the table.
     *
     * @param keyRec Record with key columns set.
     * @return {@code True} if removed successfully, {@code false} otherwise.
     */
    boolean delete(R keyRec);

    /**
     * Asynchronously deletes a record with the same key columns values as the given one from the table.
     *
     * @param keyRec Record with key columns set.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> deleteAsync(R keyRec);

    /**
     * Deletes the given record from the table.
     *
     * @param rec Record to delete.
     * @return {@code True} if removed successfully, {@code false} otherwise.
     */
    boolean deleteExact(R rec);

    /**
     * Asynchronously deletes given record from the table.
     *
     * @param rec Record to delete.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Boolean> deleteExactAsync(R rec);

    /**
     * Gets then deletes a record with the same key columns values from the table.
     *
     * @param rec Record with key columns set.
     * @return Removed record or {@code null} if not existed.
     */
    R getAndDelete(R rec);

    /**
     * Asynchronously gets then deletes a record with the same key columns values from the table.
     *
     * @param rec Record with key columns set.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<R> getAndDeleteAsync(R rec);

    /**
     * Remove records with the same key columns values as the given one has from the table.
     *
     * @param recs Records with key columns set.
     * @return Records with key columns set that were not exists.
     */
    Collection<R> deleteAll(Collection<R> recs);

    /**
     * Asynchronously remove records with the same key columns values as the given one has from the table.
     *
     * @param recs Records with key columns set.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Collection<R>> deleteAllAsync(Collection<R> recs);

    /**
     * Remove given records from the table.
     *
     * @param recs Records to delete.
     * @return Records that were not deleted.
     */
    Collection<R> deleteAllExact(Collection<R> recs);

    /**
     * Asynchronously remove given records from the table.
     *
     * @param recs Records to delete.
     * @return Future representing pending completion of the operation.
     */
    @NotNull CompletableFuture<Collection<R>> deleteAllExactAsync(Collection<R> recs);

    /**
     * Executes an InvokeProcessor code against a record with the same key columns values as the given one has.
     *
     * @param keyRec Record with key columns set.
     * @param proc Invoke processor.
     * @param <T> InvokeProcessor result type.
     * @return Results of the processing.
     */
    <T extends Serializable> T invoke(R keyRec, InvokeProcessor<R, R, T> proc);

    /**
     * Asynchronously executes an InvokeProcessor code against a record
     * with the same key columns values as the given one has.
     *
     * @param keyRec Record with key columns set.
     * @param proc Invoke processor.
     * @param <T> InvokeProcessor result type.
     * @return Future representing pending completion of the operation.
     */
    @NotNull <T extends Serializable> CompletableFuture<T> invokeAsync(R keyRec, InvokeProcessor<R, R, T> proc);

    /**
     * Executes an InvokeProcessor code against records with the same key columns values as the given ones has.
     *
     * @param keyRecs Records with key columns set.
     * @param proc Invoke processor.
     * @param <T> InvokeProcessor result type.
     * @return Results of the processing.
     */
    <T extends Serializable> Map<R, T> invokeAll(Collection<R> keyRecs, InvokeProcessor<R, R, T> proc);

    /**
     * Asynchronously executes an InvokeProcessor against records with the same key columns values as the given ones
     * has.
     *
     * @param keyRecs Records with key columns set.
     * @param proc Invoke processor.
     * @param <T> InvokeProcessor result type.
     * @return Results of the processing.
     */
    @NotNull <T extends Serializable> CompletableFuture<Map<R, T>> invokeAllAsync(Collection<R> keyRecs,
        InvokeProcessor<R, R, T> proc);
}
