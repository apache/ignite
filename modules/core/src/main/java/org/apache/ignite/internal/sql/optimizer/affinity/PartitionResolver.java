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

import org.apache.ignite.IgniteCheckedException;

/**
 * Partition resolver interface. Takes argument, data type and cache name, returns partition.
 * The only purpose of this methods is to allow partition pruning classes to be located in core module.
 */
public interface PartitionResolver {
    /**
     * Resolve partition.
     *
     * @param arg Argument.
     * @param dataType Data type.
     * @param cacheName Cache name.
     * @return Partition.
     * @throws IgniteCheckedException If failed.
     */
    int partition(Object arg, int dataType, String cacheName) throws IgniteCheckedException;
}
