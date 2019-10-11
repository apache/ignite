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

package org.apache.ignite.internal.sql.optimizer.affinity;

import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 * Node with a single partition.
 */
public abstract class PartitionSingleNode implements PartitionNode {
    /** Table descriptor. */
    @GridToStringExclude
    protected final PartitionTable tbl;

    /**
     * Constructor.
     *
     * @param tbl Table descriptor.
     */
    protected PartitionSingleNode(PartitionTable tbl) {
        this.tbl = tbl;
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> apply(PartitionClientContext cliCtx, Object... args)
        throws IgniteCheckedException {
        Integer part = applySingle(cliCtx, args);

        return part != null ? Collections.singletonList(part) : null;
    }

    /**
     * Apply arguments and get single partition.
     *
     * @param cliCtx Client context.
     * @param args Arguments.
     * @return Partition or {@code null} if failed.
     */
    public abstract Integer applySingle(@Nullable PartitionClientContext cliCtx, Object... args)
        throws IgniteCheckedException;

    /**
     * @return {@code True} if constant, {@code false} if argument.
     */
    public abstract boolean constant();

    /** {@inheritDoc} */
    @Override public int joinGroup() {
        return tbl.joinGroup();
    }

    /**
     * @return Cache name. Should be used only on server side.
     */
    @Override public String cacheName() {
        assert tbl != null;

        return tbl.cacheName();
    }

    /**
     * @return Partition for constant node, index for argument node.
     */
    public abstract int value();

    /**
     * @return Underlying table.
     */
    public PartitionTable table() {
        return tbl;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int hash = (constant() ? 1 : 0);

        hash = 31 * hash + value();

        if (tbl != null)
            hash = 31 * hash + tbl.alias().hashCode();

        return hash;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (obj == null)
            return false;

        if (obj.getClass() != getClass())
            return false;

        PartitionSingleNode other = (PartitionSingleNode)obj;

        return F.eq(constant(), other.constant()) && F.eq(value(), other.value()) &&
            F.eq(tbl.alias(), other.tbl.alias());
    }
}
