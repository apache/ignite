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

package org.apache.ignite.internal.vault;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.ByteArray;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Defines interface for accessing to a vault service.
 */
public interface VaultService extends AutoCloseable {
    /**
     * Retrieves an entry for the given key.
     *
     * @param key Key. Couldn't be {@code null}.
     * @return An entry for the given key. Couldn't be {@code null}. If there is no mapping for the provided {@code key},
     * then {@code Entry} with value that equals to null will be returned.
     */
    @NotNull CompletableFuture<VaultEntry> get(@NotNull ByteArray key);

    /**
     * Write value with key to vault. If value is equal to null, then previous value with key will be deleted if there
     * was any mapping.
     *
     * @param key Vault key. Couldn't be {@code null}.
     * @param val Value. If value is equal to null, then previous value with key will be deleted if there was any mapping.
     * @return Future representing pending completion of the operation. Couldn't be {@code null}.
     */
    @NotNull CompletableFuture<Void> put(@NotNull ByteArray key, byte @Nullable [] val);

    /**
     * Remove value with key from vault.
     *
     * @param key Vault key. Couldn't be {@code null}.
     * @return Future representing pending completion of the operation. Couldn't be {@code null}.
     */
    @NotNull CompletableFuture<Void> remove(@NotNull ByteArray key);

    /**
     * Returns a view of the portion of vault whose keys range from fromKey, inclusive, to toKey, exclusive.
     *
     * @param fromKey Start key of range (inclusive). Couldn't be {@code null}.
     * @param toKey End key of range (exclusive). Could be {@code null}.
     * @return Iterator built upon entries corresponding to the given range.
     */
    @NotNull Cursor<VaultEntry> range(@NotNull ByteArray fromKey, @NotNull ByteArray toKey);

    /**
     * Inserts or updates entries with given keys and given values. If the given value in {@code vals} is null,
     * then corresponding value with key will be deleted if there was any mapping.
     *
     * @param vals The map of keys and corresponding values. Couldn't be {@code null} or empty.
     * @return Future representing pending completion of the operation. Couldn't be {@code null}.
     */
    @NotNull CompletableFuture<Void> putAll(@NotNull Map<ByteArray, byte[]> vals);
}
