/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.mvcc;

import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * MVCC snapshot which holds the following information:
 * - Current MVCC version which should be used for visibility checks
 * - List of active transactions which should not be visible to current transaction
 * - Cleanup version which is used to help vacuum process.
 */
public interface MvccSnapshot extends MvccVersion, Message {
    /**
     * @return Active transactions.
     */
    public MvccLongList activeTransactions();

    /**
     * @return Cleanup version (all smaller versions are safe to remove).
     */
    public long cleanupVersion();

    /**
     * @return Version without active transactions.
     */
    public MvccSnapshot withoutActiveTransactions();

    /**
     * Increments operation counter.
     */
    public void incrementOperationCounter();
}
