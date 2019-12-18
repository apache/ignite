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

package org.apache.ignite.internal.processors.query.calcite.serialize.relation;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.splitter.RelTarget;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Describes {@link IgniteSender}.
 */
public class SenderNode extends RelGraphNode {


    private final RelTarget target;

    /**
     *
     * @param traits   Traits of this relational expression
     * @param target   Remote targets information
     */
    private SenderNode(RelTraitSet traits, RelTarget target) {
        super(traits);
        this.target = target;
    }

    /**
     * Factory method.
     *
     * @param rel Sender rel.
     * @return SenderNode.
     */
    public static SenderNode create(IgniteSender rel) {
        return new SenderNode(rel.getTraitSet(), rel.target());
    }

    /** {@inheritDoc} */
    @Override public RelNode toRel(ConversionContext ctx, List<RelNode> children) {
        RelNode input = F.first(children);
        RelOptCluster cluster = input.getCluster();

        return new IgniteSender(cluster, traits.toTraitSet(cluster), input, target);
    }
}
