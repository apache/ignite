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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxMapping;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class IgniteTxMappingsImpl implements IgniteTxMappings {
    /** */
    private final Map<UUID, GridDistributedTxMapping> mappings = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override public void clear() {
        mappings.clear();
    }

    /** {@inheritDoc} */
    @Override public boolean empty() {
        return mappings.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public GridDistributedTxMapping get(UUID nodeId) {
        return mappings.get(nodeId);
    }

    /** {@inheritDoc} */
    @Override public void put(GridDistributedTxMapping mapping) {
        mappings.put(mapping.primary().id(), mapping);
    }

    /** {@inheritDoc} */
    @Override public GridDistributedTxMapping remove(UUID nodeId) {
        return mappings.remove(nodeId);
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridDistributedTxMapping localMapping() {
        for (GridDistributedTxMapping m : mappings.values()) {
            if (m.primary().isLocal())
                return m;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean single() {
        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridDistributedTxMapping singleMapping() {
        assert mappings.size() == 1 : mappings;

        return F.firstValue(mappings);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridDistributedTxMapping> mappings() {
        return mappings.values();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteTxMappingsImpl.class, this);
    }
}
