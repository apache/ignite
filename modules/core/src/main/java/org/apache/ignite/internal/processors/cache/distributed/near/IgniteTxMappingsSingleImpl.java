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
