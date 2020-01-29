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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.serialize.type.DataType;
import org.apache.ignite.internal.processors.query.calcite.serialize.type.Types;
import org.apache.ignite.internal.processors.query.calcite.splitter.RelSource;
import org.apache.ignite.internal.processors.query.calcite.splitter.RelSourceImpl;


/**
 * Describes {@link IgniteReceiver}.
 */
public class ReceiverNode extends RelGraphNode {
    /** */
    private final DataType dataType;

    /** */
    private final RelSource source;

    /**
     * @param traits   Traits of this relational expression
     * @param dataType Output row type
     * @param source   Remote sources information.
     */
    private ReceiverNode(RelTraitSet traits, DataType dataType, RelSource source) {
        super(traits);
        this.dataType = dataType;
        this.source = source;
    }

    /**
     * Factory method.
     *
     * @param rel Receiver rel.
     * @return ReceiverNode.
     */
    public static ReceiverNode create(IgniteReceiver rel) {
        RelSource source = new RelSourceImpl(rel.source().fragmentId(), rel.source().mapping());

        return new ReceiverNode(rel.getTraitSet(), Types.fromType(rel.getRowType()), source);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel toRel(ConversionContext ctx, List<IgniteRel> children) {
        return new IgniteReceiver(ctx.getCluster(),
            traitSet(ctx.getCluster()),
            dataType.toRelDataType(ctx.getTypeFactory()),
            source);
    }
}
