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

package org.apache.ignite.internal.processors.query.calcite.util;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptNode;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryContext;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;

/**
 *
 */
public final class Commons {
    private Commons(){}

    public static Context convert(QueryContext ctx) {
        return ctx == null ? Contexts.empty() : Contexts.of(ctx.unwrap(Object[].class));
    }

    /** */
    public static Function<RelDataTypeFactory, RelDataType> rowTypeFunction(GridQueryTypeDescriptor desc) {
        return (f) -> {
            RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(f);

            builder.add(QueryUtils.KEY_FIELD_NAME, f.createJavaType(desc.keyClass()));
            builder.add(QueryUtils.VAL_FIELD_NAME, f.createJavaType(desc.valueClass()));

            for (Map.Entry<String, Class<?>> prop : desc.fields().entrySet()) {
                builder.add(prop.getKey(), f.createJavaType(prop.getValue()));
            }
            return builder.build();
        };
    }

    public static RelOptRuleOperand any(Class<? extends RelNode> first, RelTrait trait){
        return RelOptRule.operand(first, trait, RelOptRule.any());
    }

    public static RelOptRuleOperand any(Class<? extends RelNode> first){
        return RelOptRule.operand(first, RelOptRule.any());
    }

    public static RelOptRuleOperand any(Class<? extends RelNode> first, Class<? extends RelNode> second) {
        return RelOptRule.operand(first, RelOptRule.operand(second, RelOptRule.any()));
    }

    public static RelOptRuleOperand some(Class<? extends RelNode> rel, RelOptRuleOperand first, RelOptRuleOperand... rest){
        return RelOptRule.operand(rel, RelOptRule.some(first, rest));
    }

    public static RelOptRuleOperand some(Class<? extends RelNode> rel, RelTrait trait, RelOptRuleOperand first, RelOptRuleOperand... rest){
        return RelOptRule.operand(rel, trait, RelOptRule.some(first, rest));
    }

    public static <T extends RelNode> RelOp<T, Boolean> transformSubset(RelOptRuleCall call, RelNode input, BiFunction<T, RelNode, RelNode> transformFun) {
        return rel -> {
            if (!(input instanceof RelSubset))
                return Boolean.FALSE;

            RelSubset subset = (RelSubset) input;

            Set<RelTraitSet> traits = subset.getRelList().stream()
                .filter(r -> r instanceof IgniteRel)
                .map(RelOptNode::getTraitSet)
                .collect(Collectors.toSet());

            if (traits.isEmpty())
                return Boolean.FALSE;

            Set<RelNode> transformed = Collections.newSetFromMap(new IdentityHashMap<>());

            boolean transform = Boolean.FALSE;

            for (RelTraitSet traitSet: traits) {
                RelNode newRel = RelOptRule.convert(subset, traitSet.simplify());

                if (transformed.add(newRel)) {
                    RelNode out = transformFun.apply(rel, newRel);

                    if (out != null) {
                        call.transformTo(out);

                        transform = Boolean.TRUE;
                    }
                }
            }

            return transform;
        };
    }
}
