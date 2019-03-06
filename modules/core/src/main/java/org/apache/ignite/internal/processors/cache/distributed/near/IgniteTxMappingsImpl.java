/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxMapping;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;
import java.util.concurrent.ConcurrentHashMap;

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
