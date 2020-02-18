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

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteValues;
import org.apache.ignite.internal.processors.query.calcite.serialize.expression.ExpToRexTranslator;
import org.apache.ignite.internal.processors.query.calcite.serialize.expression.Expression;
import org.apache.ignite.internal.processors.query.calcite.serialize.expression.RexToExpTranslator;
import org.apache.ignite.internal.processors.query.calcite.serialize.type.DataType;
import org.apache.ignite.internal.processors.query.calcite.serialize.type.Types;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 *
 */
public class ValuesNode extends RelGraphNode {
    /** */
    private final DataType dataType;

    /** */
    private final List<List<Expression>> tuples;


    /**
     * @param traits Traits of this relational expression.
     */
    private ValuesNode(RelTraitSet traits, DataType dataType, List<List<Expression>> tuples) {
        super(traits);
        this.dataType = dataType;
        this.tuples = tuples;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel toRel(ConversionContext ctx, List<IgniteRel> children) {
        RelOptCluster cluster = ctx.getCluster();
        RelTraitSet traits = traitSet(cluster);
        RelDataType rowType = dataType.toRelDataType((IgniteTypeFactory) ctx.getTypeFactory());
        ImmutableList<ImmutableList<RexLiteral>> tuples = translate(this.tuples, ctx.getExpressionTranslator());

        return new IgniteValues(cluster, rowType, tuples, traits);
    }

    /**
     * Factory method.
     *
     * @param rel Values rel.
     * @param rexTranslator Expression translator.
     * @return ProjectNode.
     */
    public static ValuesNode create(IgniteValues rel, RexToExpTranslator rexTranslator) {
        return new ValuesNode(rel.getTraitSet(), Types.fromType(rel.getRowType()),
            Commons.transform(rel.getTuples(), tuple -> rexTranslator.translate(Commons.cast(tuple))));
    }

    /** */
    private static ImmutableList<ImmutableList<RexLiteral>> translate(List<List<Expression>> tuples, ExpToRexTranslator translator) {
        ImmutableList.Builder<ImmutableList<RexLiteral>> b = ImmutableList.builder();

        for (List<Expression> tuple : tuples) {
            ImmutableList.Builder<RexLiteral> b1 = ImmutableList.builder();

            for (Expression exp : tuple)
                b1.add((RexLiteral) translator.translate(exp));

            b.add(b1.build());
        }

        return b.build();
    }
}
