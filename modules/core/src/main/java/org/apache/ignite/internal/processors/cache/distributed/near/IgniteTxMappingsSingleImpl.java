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
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxMapping;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class IgniteTxMappingsSingleImpl implements IgniteTxMappings {
    /** */
    private volatile GridDistributedTxMapping mapping;

    /** {@inheritDoc} */
    @Override public void clear() {
        mapping = null;
    }

    /** {@inheritDoc} */
    @Override public boolean empty() {
        return mapping == null;
    }

    /** {@inheritDoc} */
    @Override public GridDistributedTxMapping get(UUID nodeId) {
        GridDistributedTxMapping mapping0 = mapping;

        return (mapping0 != null && mapping0.primary().id().equals(nodeId)) ? mapping0 : null;
    }

    /** {@inheritDoc} */
    @Override public void put(GridDistributedTxMapping mapping) {
        assert this.mapping == null;

        this.mapping = mapping;
    }

    /** {@inheritDoc} */
    @Override public GridDistributedTxMapping remove(UUID nodeId) {
        GridDistributedTxMapping mapping0 = mapping;

        if (mapping0 != null && mapping0.primary().id().equals(nodeId)) {
            this.mapping = null;

            return mapping0;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridDistributedTxMapping localMapping() {
        GridDistributedTxMapping mapping0 = mapping;

        if (mapping0 != null && mapping0.primary().isLocal())
            return mapping0;

        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean single() {
        return true;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridDistributedTxMapping singleMapping() {
        return mapping;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridDistributedTxMapping> mappings() {
        assert false;

        return null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteTxMappingsSingleImpl.class, this);
    }
}
