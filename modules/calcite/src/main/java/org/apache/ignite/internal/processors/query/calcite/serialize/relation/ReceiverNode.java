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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.serialize.type.DataType;
import org.apache.ignite.internal.processors.query.calcite.splitter.RelSource;
import org.apache.ignite.internal.processors.query.calcite.splitter.RelSourceImpl;


/**
 *
 */
public class ReceiverNode extends RelGraphNode {
    private final DataType dataType;
    private final RelSource source;

    private ReceiverNode(RelTraitSet traits, DataType dataType, RelSource source) {
        super(traits);
        this.dataType = dataType;
        this.source = source;
    }

    public static ReceiverNode create(IgniteReceiver rel) {
        RelSource source = new RelSourceImpl(rel.source().exchangeId(), rel.source().mapping());

        return new ReceiverNode(rel.getTraitSet(), DataType.fromType(rel.getRowType()), source);
    }

    @Override public RelNode toRel(ConversionContext ctx, List<RelNode> children) {
        return new IgniteReceiver(ctx.getCluster(),
            traitSet.toTraitSet(ctx.getCluster()),
            dataType.toRelDataType(ctx.getTypeFactory()),
            source);
    }
}
