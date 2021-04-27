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

import org.jetbrains.annotations.NotNull;

/**
 * Vault storage listener for changes.
 */
//TODO: need to generify with metastorage WatchListener https://issues.apache.org/jira/browse/IGNITE-14653
public interface VaultListener {
    /**
     * The method will be called on each vault update.
     *
     * @param entries A single entry or a batch.
     * @return {@code True} if listener must continue event handling. If returns {@code false} then the listener and
     * corresponding watch will be unregistered.
     */
    boolean onUpdate(@NotNull Iterable<Entry> entries);

    /**
     * The method will be called in case of an error occurred. The listener and corresponding watch will be
     * unregistered.
     *
     * @param e Exception.
     */
    void onError(@NotNull Throwable e);
}
