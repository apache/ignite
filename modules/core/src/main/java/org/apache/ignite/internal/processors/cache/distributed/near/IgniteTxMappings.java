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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxMapping;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public interface IgniteTxMappings {
    /**
     * Clears this mappings.
     */
    public void clear();

    /**
     * @return {@code True} if there are no mappings.
     */
    public boolean empty();

    /**
     * @param nodeId Node ID.
     * @return Node mapping.
     */
    @Nullable public GridDistributedTxMapping get(UUID nodeId);

    /**
     * @param mapping Mapping.
     */
    public void put(GridDistributedTxMapping mapping);

    /**
     * @param nodeId Node ID.
     * @return Removed mapping.
     */
    @Nullable public GridDistributedTxMapping remove(UUID nodeId);

    /**
     * @return Mapping for local node.
     */
    @Nullable public GridDistributedTxMapping localMapping();

    /**
     * @return Non null instance if this mappings contain only one mapping.
     */
    @Nullable public GridDistributedTxMapping singleMapping();

    /**
     * @return All mappings.
     */
    public Collection<GridDistributedTxMapping> mappings();

    /**
     * @return {@code True} if this is single mapping.
     */
    public boolean single();
}
