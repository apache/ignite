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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.vault.common.Entry;
import org.apache.ignite.internal.vault.common.VaultWatch;
import org.apache.ignite.internal.vault.service.VaultService;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.NotNull;

/**
 * VaultManager is responsible for handling {@link VaultService} lifecycle
 * and providing interface for managing local keys.
 */
public class VaultManager {
    /** Special key for vault where applied revision for {@code putAll} operation is stored. */
    private static ByteArray APPLIED_REV = ByteArray.fromString("applied_revision");

    /** Mutex. */
    private final Object mux = new Object();

    /** Instance of vault */
    private VaultService vaultService;

    /** Default constructor.
     *
     * @param vaultService Instance of vault.
     */
    public VaultManager(VaultService vaultService) {
        this.vaultService = vaultService;
    }

    /**
     * @return {@code true} if VaultService beneath given VaultManager was bootstrapped with data
     * either from PDS or from user initial bootstrap configuration.
     *
     * TODO: implement when IGNITE-14408 will be ready
     */
    public boolean bootstrapped() {
        return false;
    }

    /**
     * See {@link VaultService#get(ByteArray)}
     *
     * @param key Key. Couldn't be {@code null}.
     * @return An entry for the given key. Couldn't be {@code null}.
     */
    @NotNull public CompletableFuture<Entry> get(@NotNull ByteArray key) {
        return vaultService.get(key);
    }

    /**
     * See {@link VaultService#put(ByteArray, byte[])}
     *
     * @param key Vault key. Couldn't be {@code null}.
     * @param val Value. Couldn't be {@code null}.
     * @return Future representing pending completion of the operation.
     */
    @NotNull public CompletableFuture<Void> put(@NotNull ByteArray key, @NotNull byte[] val) {
        return vaultService.put(key, val);
    }

    /**
     * See {@link VaultService#remove(ByteArray)}
     *
     * @param key Vault key. Couldn't be {@code null}.
     * @return Future representing pending completion of the operation.
     */
    @NotNull public CompletableFuture<Void> remove(@NotNull ByteArray key) {
        return vaultService.remove(key);
    }

    /**
     * See {@link VaultService#range(ByteArray, ByteArray)}
     *
     * @param fromKey Start key of range (inclusive). Couldn't be {@code null}.
     * @param toKey End key of range (exclusive). Could be {@code null}.
     * @return Iterator built upon entries corresponding to the given range.
     */
    @NotNull public Iterator<Entry> range(@NotNull ByteArray fromKey, @NotNull ByteArray toKey) {
        return vaultService.range(fromKey, toKey);
    }

    /**
     * Inserts or updates entries with given keys and given values and non-negative revision.
     *
     * @param vals The map of keys and corresponding values. Couldn't be {@code null} or empty.
     * @param revision Revision for entries. Must be positive.
     * @return Future representing pending completion of the operation.
     * @throws IgniteInternalCheckedException If revision is inconsistent with applied revision from vault or
     * if couldn't get applied revision from vault.
     */
    public CompletableFuture<Void> putAll(@NotNull Map<ByteArray, byte[]> vals, long revision) throws IgniteInternalCheckedException {
        synchronized (mux) {
            byte[] appliedRevBytes;

            try {
                appliedRevBytes = vaultService.get(APPLIED_REV).get().value();
            }
            catch (InterruptedException | ExecutionException e) {
               throw new IgniteInternalCheckedException("Error occurred when getting applied revision", e);
            }

            long appliedRevision = appliedRevBytes != null ? ByteUtils.bytesToLong(appliedRevBytes, 0) : 0L;

            if (revision < appliedRevision)
                throw new IgniteInternalCheckedException("Inconsistency between applied revision from vault and the current revision");

            HashMap<ByteArray, byte[]> mergedMap = new HashMap<>(vals);

            mergedMap.put(APPLIED_REV, ByteUtils.longToBytes(revision));

            return vaultService.putAll(mergedMap);
        }

    }

    /**
     * @return Applied revision for {@link VaultManager#putAll} operation.
     * @throws IgniteInternalCheckedException If couldn't get applied revision from vault.
     */
    @NotNull public Long appliedRevision() throws IgniteInternalCheckedException {
        byte[] appliedRevision;

        synchronized (mux) {
            try {
                appliedRevision = vaultService.get(APPLIED_REV).get().value();
            }
            catch (InterruptedException | ExecutionException e) {
                throw new IgniteInternalCheckedException("Error occurred when getting applied revision", e);
            }

            return appliedRevision == null ? 0L : ByteUtils.bytesToLong(appliedRevision, 0);
        }
    }

    /**
     * See {@link VaultService#watch(VaultWatch)}
     *
     * @param vaultWatch Watch which will notify for each update.
     * @return Subscription identifier. Could be used in {@link #stopWatch} method in order to cancel subscription.
     */
    @NotNull public CompletableFuture<Long> watch(@NotNull VaultWatch vaultWatch) {
        return vaultService.watch(vaultWatch);
    }

    /**
     * See {@link VaultService#stopWatch(Long)}
     *
     * @param id Subscription identifier.
     * @return Completed future in case of operation success. Couldn't be {@code null}.
     */
    @NotNull public CompletableFuture<Void> stopWatch(@NotNull Long id) {
        return vaultService.stopWatch(id);
    }
}
