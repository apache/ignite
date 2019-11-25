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

package org.apache.ignite.internal.processors.query.calcite.serialize.relation;

import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.rel.Sender;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTrait;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public class SenderNode extends RelGraphNode {
    private final DistributionTrait targetDistr;
    private final NodesMapping targetMapping;

    private SenderNode(DistributionTrait targetDistr, NodesMapping targetMapping) {
        this.targetDistr = targetDistr;
        this.targetMapping = targetMapping;
    }

    public static SenderNode create(Sender rel) {
        return new SenderNode(rel.targetDistribution(), rel.targetMapping());
    }

    @Override public RelNode toRel(ConversionContext ctx, List<RelNode> children) {
        return Sender.create(F.first(children), targetDistr, targetMapping);
    }
}
