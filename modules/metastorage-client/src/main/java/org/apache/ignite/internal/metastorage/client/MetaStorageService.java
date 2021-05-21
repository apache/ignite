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

package org.apache.ignite.internal.metastorage.client;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Defines interface for access to a meta storage service.
 */
public interface MetaStorageService {
    /**
     * Retrieves an entry for the given key.
     *
     * @param key Key. Couldn't be {@code null}.
     * @return An entry for the given key. Couldn't be {@code null}.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @see ByteArray
     * @see Entry
     */
    @NotNull
    CompletableFuture<Entry> get(@NotNull ByteArray key);

    /**
     * Retrieves an entry for the given key and the revision upper bound.
     *
     * @param key The key. Couldn't be {@code null}.
     * @param revUpperBound The upper bound for entry revisions. Must be positive.
     * @return An entry for the given key and maximum revision limited by {@code revUpperBound}.
     * Couldn't be {@code null}.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction.
     * Will be thrown on getting future result.
     * @see ByteArray
     * @see Entry
     */
    @NotNull
    CompletableFuture<Entry> get(@NotNull ByteArray key, long revUpperBound);

    /**
     * Retrieves entries for given keys.
     *
     * @param keys The set of keys. Couldn't be {@code null} or empty.
     *             Set elements couldn't be {@code null}.
     * @return A map of entries for given keys. Couldn't be {@code null}.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @see ByteArray
     * @see Entry
     */
    @NotNull
    CompletableFuture<Map<ByteArray, Entry>> getAll(Set<ByteArray> keys);

    /**
     * Retrieves entries for given keys and the revision upper bound.
     *
     * @param keys The set of keys. Couldn't be {@code null} or empty.
     *             Set elements couldn't be {@code null}.
     * @param revUpperBound The upper bound for entry revisions. Must be positive.
     * @return A map of entries for given keys and maximum revision limited by {@code revUpperBound}.
     * Couldn't be {@code null}.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction.
     * Will be thrown on getting future result.
     * @see ByteArray
     * @see Entry
     */
    @NotNull
    CompletableFuture<Map<ByteArray, Entry>> getAll(Set<ByteArray> keys, long revUpperBound);

    /**
     * Inserts or updates an entry with the given key and the given value.
     *
     * @param key The key. Couldn't be {@code null}.
     * @param value The value. Couldn't be {@code null}.
     * @return Completed future.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @see ByteArray
     * @see Entry
     */
    @NotNull
    CompletableFuture<Void> put(@NotNull ByteArray key, @NotNull byte[] value);

    /**
     * Inserts or updates an entry with the given key and the given value and
     * retrieves a previous entry for the given key.
     *
     * @param key The key. Couldn't be {@code null}.
     * @param value The value. Couldn't be {@code null}.
     * @return A previous entry for the given key. Couldn't be {@code null}.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @see ByteArray
     * @see Entry
     */
    @NotNull
    CompletableFuture<Entry> getAndPut(@NotNull ByteArray key, @NotNull byte[] value);

    /**
     * Inserts or updates entries with given keys and given values.
     *
     * @param vals The map of keys and corresponding values. Couldn't be {@code null} or empty.
     * @return Completed future.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @see ByteArray
     * @see Entry
     */
    @NotNull
    CompletableFuture<Void> putAll(@NotNull Map<ByteArray, byte[]> vals);

    /**
     * Inserts or updates entries with given keys and given values and
     * retrieves a previous entries for given keys.
     *
     * @param vals The map of keys and corresponding values. Couldn't be {@code null} or empty.
     * @return A map of entries for given keys. Couldn't be {@code null}.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @see ByteArray
     * @see Entry
     */
    @NotNull
    CompletableFuture<Map<ByteArray, Entry>> getAndPutAll(@NotNull Map<ByteArray, byte[]> vals);

    /**
     * Removes an entry for the given key.
     *
     * @param key The key. Couldn't be {@code null}.
     * @return Completed future.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @see ByteArray
     * @see Entry
     */
    @NotNull
    CompletableFuture<Void> remove(@NotNull ByteArray key);

    /**
     * Removes an entry for the given key.
     *
     * @param key The key. Couldn't be {@code null}.
     * @return A previous entry for the given key. Couldn't be {@code null}.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @see ByteArray
     * @see Entry
     */
    @NotNull
    CompletableFuture<Entry> getAndRemove(@NotNull ByteArray key);

    /**
     * Removes entries for given keys.
     *
     * @param keys The keys set. Couldn't be {@code null} or empty.
     * @return Completed future.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @see ByteArray
     * @see Entry
     */
    @NotNull
    CompletableFuture<Void> removeAll(@NotNull Set<ByteArray> keys);

    /**
     * Removes entries for given keys and retrieves previous entries.
     *
     * @param keys The keys collection. Couldn't be {@code null}.
     * @return A map of previous entries for given keys..
     * The order of entries in the result list corresponds to the traversal order of {@code keys} collection.
     * Couldn't be {@code null}.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @see ByteArray
     * @see Entry
     */
    @NotNull
    CompletableFuture<Map<ByteArray, Entry>> getAndRemoveAll(@NotNull Set<ByteArray> keys);

    /**
     * Updates an entry for the given key conditionally.
     *
     * <p>Conditional update could be treated as <i>if(condition)-then(success)-else(failure)</i> expression.</p>
     *
     * @param condition The condition.
     * @param success The update which will be applied in case of condition evaluation yields {@code true}.
     * @param failure The update which will be applied in case of condition evaluation yields {@code false}.
     * @return Future result {@code true} if {@code success} update was applied, otherwise {@code false}.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @see ByteArray
     * @see Entry
     * @see Condition
     * @see Operation
     */
    // TODO: https://issues.apache.org/jira/browse/IGNITE-14269: will be replaced by conditional multi update.
    @NotNull
    CompletableFuture<Boolean> invoke(@NotNull Condition condition,
                                      @NotNull Operation success, @NotNull Operation failure);

    /**
     * Updates an entry for the given key conditionally.
     *
     * <p>Conditional update could be treated as <i>if(condition)-then(success)-else(failure)</i> expression.</p>
     *
     * @param condition The condition.
     * @param success The updates which will be applied in case of condition evaluation yields {@code true}.
     * @param failure The updates which will be applied in case of condition evaluation yields {@code false}.
     * @return Future result {@code true} if {@code success} update was applied, otherwise {@code false}.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @see ByteArray
     * @see Entry
     * @see Condition
     * @see Operation
     */
    // TODO: https://issues.apache.org/jira/browse/IGNITE-14269: will be replaced by conditional multi update.
    @NotNull
    CompletableFuture<Boolean> invoke(@NotNull Condition condition,
                                      @NotNull Collection<Operation> success, @NotNull Collection<Operation> failure);

    /**
     * Retrieves entries for the given key range in lexicographic order. Entries will be filtered out by upper bound
     * of given revision number.
     *
     * @param keyFrom Start key of range (inclusive). Couldn't be {@code null}.
     * @param keyTo End key of range (exclusive). Could be {@code null}.
     * @param revUpperBound  The upper bound for entry revision. {@code -1} means latest revision.
     * @return Cursor built upon entries corresponding to the given range and revision.
     * @throws OperationTimeoutException If the operation is timed out.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction.
     * @see ByteArray
     * @see Entry
     */
    @NotNull
    Cursor<Entry> range(@NotNull ByteArray keyFrom, @Nullable ByteArray keyTo, long revUpperBound);

    /**
     * Retrieves entries for the given key range in lexicographic order. Short cut for
     * {@link #range(ByteArray, ByteArray, long)} where {@code revUpperBound == -1}.
     *
     * @param keyFrom Start key of range (inclusive). Couldn't be {@code null}.
     * @param keyTo End key of range (exclusive). Could be {@code null}.
     * @return Cursor built upon entries corresponding to the given range and revision.
     * @throws OperationTimeoutException If the operation is timed out.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction.
     * @see ByteArray
     * @see Entry
     */
    @NotNull
    Cursor<Entry> range(@NotNull ByteArray keyFrom, @Nullable ByteArray keyTo);

    /**
     * Subscribes on meta storage updates matching the parameters.
     *
     * @param keyFrom Start key of range (inclusive). Could be {@code null}.
     * @param keyTo End key of range (exclusive). Could be {@code null}.
     * @param revision Start revision inclusive. {@code 0} - all revisions,
     * {@code -1} - latest revision (accordingly to current meta storage state).
     * @param lsnr Listener which will be notified for each update.
     * @return Subscription identifier. Could be used in {@link #stopWatch} method in order to cancel subscription.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction.
     * Will be thrown on getting future result.
     * @see ByteArray
     * @see Entry
     */
    @NotNull
    CompletableFuture<IgniteUuid> watch(@Nullable ByteArray keyFrom, @Nullable ByteArray keyTo,
                                  long revision, @NotNull WatchListener lsnr);

    /**
     * Subscribes on meta storage updates for the given key.
     *
     * @param key The target key. Couldn't be {@code null}.
     * @param revision Start revision inclusive. {@code 0} - all revisions,
     * {@code -1} - latest revision (accordingly to current meta storage state).
     * @param lsnr Listener which will be notified for each update.
     * @return Subscription identifier. Could be used in {@link #stopWatch} method in order to cancel subscription.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction.
     * Will be thrown on getting future result.
     * @see ByteArray
     * @see Entry
     */
    @NotNull
    CompletableFuture<IgniteUuid> watch(@NotNull ByteArray key, long revision, @NotNull WatchListener lsnr);

    /**
     * Subscribes on meta storage updates for given keys.
     *
     * @param keys Set of target keys. Couldn't be {@code null} or empty.
     * @param revision Start revision inclusive. {@code 0} - all revision,
     * {@code -1} - latest revision (accordingly to current meta storage state).
     * @param lsnr Listener which will be notified for each update.
     * @return Subscription identifier. Could be used in {@link #stopWatch} method in order to cancel subscription.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     * @throws CompactedException If the desired revisions are removed from the storage due to a compaction.
     * Will be thrown on getting future result.
     * @see ByteArray
     * @see Entry
     */
    @NotNull
    CompletableFuture<IgniteUuid> watch(@NotNull Set<ByteArray> keys, long revision, @NotNull WatchListener lsnr);

    /**
     * Cancels subscription for the given identifier.
     *
     * @param id Subscription identifier.
     * @return Completed future in case of operation success. Couldn't be {@code null}.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     */
    @NotNull
    CompletableFuture<Void> stopWatch(@NotNull IgniteUuid id);

    /**
     * Compacts meta storage (removes all tombstone entries and old entries except of entries with latest revision).
     *
     * @return Completed future. Couldn't be {@code null}.
     * @throws OperationTimeoutException If the operation is timed out. Will be thrown on getting future result.
     */
    @NotNull
    CompletableFuture<Void> compact();
}

