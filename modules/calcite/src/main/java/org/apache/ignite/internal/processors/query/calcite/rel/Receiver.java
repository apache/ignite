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

package org.apache.ignite.internal.processors.query.calcite.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.splitter.Fragment;
import org.apache.ignite.internal.processors.query.calcite.util.RelImplementor;

/**
 *
 */
public final class Receiver extends AbstractRelNode implements IgniteRel {
    private final Fragment sourceFragment;
    private final NodesMapping sourceMapping;

    /**
     * @param cluster Cluster this relational expression belongs to
     * @param traits Trait set.
     */
    public Receiver(RelOptCluster cluster, RelTraitSet traits, RelDataType rowType, Fragment sourceFragment) {
        super(cluster, traits);
        this.rowType = rowType;
        this.sourceFragment = sourceFragment;

        sourceMapping = null;
    }

    /**
     * @param cluster Cluster this relational expression belongs to
     * @param traits Trait set.
     */
    public Receiver(RelOptCluster cluster, RelTraitSet traits, RelDataType rowType, NodesMapping sourceMapping) {
        super(cluster, traits);
        this.rowType = rowType;
        this.sourceMapping = sourceMapping;

        sourceFragment = null;
    }

    /** {@inheritDoc} */
    @Override public <T> T implement(RelImplementor<T> implementor) {
        return implementor.implement(this);
    }

    public Fragment sourceFragment() {
        return sourceFragment;
    }

    public NodesMapping sourceMapping() {
        if (sourceFragment != null)
            return sourceFragment.mapping();

        assert sourceMapping != null;

        return sourceMapping;
    }
}
