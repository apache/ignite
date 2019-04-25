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

package org.apache.ignite.internal.sql.optimizer.affinity;

import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * Common node of partition tree.
 */
public interface PartitionNode {

    /**
     * Get partitions.
     *
     * @param cliCtx Thin client context (optional).
     * @param args Query arguments.
     * @return Partitions.
     * @throws IgniteCheckedException If failed.
     */
    Collection<Integer> apply(@Nullable PartitionClientContext cliCtx, Object... args) throws IgniteCheckedException;

    /**
     * @return Join group for the given node.
     */
    int joinGroup();


    /**
     * @return First met cache name of an any <code>PartitionSingleNode</code>
     * during <code>PartitionNode</code> tree traversal. This method is intended to be used within the Jdbc thin client.
     */
    String cacheName();

    /**
     * Try optimizing partition nodes into a simpler form.
     *
     * @return Optimized node or {@code this} if optimization failed.
     */
    default PartitionNode optimize() {
        return this;
    }
}
