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

package org.apache.ignite.internal.processors.metastorage;

import java.io.Serializable;
import org.apache.ignite.IgniteCheckedException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * API for distributed data storage with the ability to write into it.
 *
 * @see ReadableDistributedMetaStorage
 */
public interface DistributedMetaStorage extends ReadableDistributedMetaStorage {
    /**
     * Write value into distributed metastorage.
     *
     * @param key The key.
     * @param val Value to write. Must not be null.
     * @throws IgniteCheckedException If cluster is in deactivated state.
     */
    void write(@NotNull String key, @NotNull Serializable val) throws IgniteCheckedException;

    /**
     * Remove value from distributed metastorage.
     *
     * @param key The key.
     * @throws IgniteCheckedException If cluster is in deactivated state.
     */
    void remove(@NotNull String key) throws IgniteCheckedException;

    /**
     * Write value into distributed metastorage but only if current value matches the expected one.
     *
     * @param key The key.
     * @param expVal Expected value. Might be null.
     * @param newVal Value to write. Must not be null.
     * @throws IgniteCheckedException If cluster is in deactivated state.
     * @return {@code True} if expected value matched the actual one and write was completed successfully.
     *      {@code False} otherwise.
     */
    boolean compareAndSet(
        @NotNull String key,
        @Nullable Serializable expVal,
        @NotNull Serializable newVal
    ) throws IgniteCheckedException;

    /**
     * Remove value from distributed metastorage but only if current value matches the expected one.
     *
     * @param key The key.
     * @param expVal Expected value. Must not be null.
     * @throws IgniteCheckedException If cluster is in deactivated state.
     * @return {@code True} if expected value matched the actual one and remove was completed successfully.
     *      {@code False} otherwise.
     */
    boolean compareAndRemove(@NotNull String key, @NotNull Serializable expVal) throws IgniteCheckedException;
}
