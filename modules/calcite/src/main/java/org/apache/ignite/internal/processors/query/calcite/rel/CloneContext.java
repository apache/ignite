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

import java.util.IdentityHashMap;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;

/**
 *
 */
public final class CloneContext {
    private final RelOptCluster cluster;
    private final IdentityHashMap<IgniteRel, IgniteRel> mapping = new IdentityHashMap<>();

    public CloneContext(RelOptCluster cluster) {
        this.cluster = cluster;
    }

    public RelOptCluster getCluster() {
        return cluster;
    }

    public <T extends IgniteRel> T clone(RelNode src) {
        try {
            return (T) mapping.computeIfAbsent((IgniteRel) src, this::clone0);
        }
        catch (ClassCastException e) {
            throw new IllegalStateException("Unexpected node type: " + src.getClass());
        }
    }

    private IgniteRel clone0(IgniteRel src) {
        return src.clone(this);
    }
}
