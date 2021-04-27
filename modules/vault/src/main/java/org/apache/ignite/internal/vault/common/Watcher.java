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

package org.apache.ignite.internal.vault.common;

import java.util.concurrent.CompletableFuture;
import org.jetbrains.annotations.NotNull;

/**
 * Vault watcher.
 *
 * Watches for vault entries updates.
 */
public interface Watcher {
    /**
     * Registers watch for vault entries updates.
     *
     * @param vaultWatch Vault watch.
     * @return Id of registered watch.
     */
    CompletableFuture<Long> register(@NotNull VaultWatch vaultWatch);

    /**
     * Notifies watcher that vault entry {@code val} was changed.
     *
     * @param val Vault entry.
     */
    void notify(@NotNull Entry val);

    /**
     * Cancels watch with specified {@code id}.
     *
     * @param id Id of watch.
     */
    void cancel(@NotNull Long id);
}
