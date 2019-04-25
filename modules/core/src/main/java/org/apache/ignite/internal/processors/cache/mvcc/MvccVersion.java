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

import org.jetbrains.annotations.NotNull;

/**
 * MVCC version. This is unique version allowing to order all reads and writes within a cluster. Consists of two parts:
 * - coordinator version - number which increases on every coordinator change;
 * - counter - local coordinator counter which is increased on every update.
 */
public interface MvccVersion extends Comparable<MvccVersion> {
    /**
     * @return Coordinator version.
     */
    public long coordinatorVersion();

    /**
     * @return Local counter.
     */
    public long counter();

    /**
     * @return Operation id in scope of current transaction.
     */
    public int operationCounter();

    /** {@inheritDoc} */
    @Override default int compareTo(@NotNull MvccVersion another) {
        return MvccUtils.compare(coordinatorVersion(), counter(), operationCounter(),
            another.coordinatorVersion(), another.counter(), another.operationCounter());
    }
}
