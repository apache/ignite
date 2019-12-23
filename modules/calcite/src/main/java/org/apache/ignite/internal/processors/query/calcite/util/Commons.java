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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.type.RowType;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.NotNull;

import static org.apache.calcite.sql.SqlKind.EQUALS;
import static org.apache.calcite.sql.SqlKind.GREATER_THAN;
import static org.apache.calcite.sql.SqlKind.GREATER_THAN_OR_EQUAL;
import static org.apache.calcite.sql.SqlKind.LESS_THAN;
import static org.apache.calcite.sql.SqlKind.LESS_THAN_OR_EQUAL;

/**
 *
 */
public final class Commons {
    public static final Set<SqlKind> TREE_INDEX_COMPARISON =
        EnumSet.of(
            EQUALS,
            LESS_THAN, GREATER_THAN,
            GREATER_THAN_OR_EQUAL, LESS_THAN_OR_EQUAL);

    private Commons(){}

    public static Context convert(QueryContext ctx) {
        return ctx == null ? Contexts.empty() : Contexts.of(ctx.unwrap(Object[].class));
    }

    /** */
    public static RowType rowType(GridQueryTypeDescriptor desc) {
        RowType.Builder b = RowType.builder();

        Map<String, Class<?>> fields = desc.fields();

        b.key(desc.keyClass()).val(desc.valueClass());

        for (Map.Entry<String, Class<?>> entry : fields.entrySet()) {
            GridQueryProperty prop = desc.property(entry.getKey());

            if (prop.key())
                b.keyField(prop.name(), prop.type(), Objects.equals(desc.affinityKey(), prop.name()));
            else
                b.field(prop.name(), prop.type());
        }

        return b.build();
    }

    public static RelOptRuleOperand any(Class<? extends RelNode> first, Class<? extends RelNode> second) {
        return RelOptRule.operand(first, RelOptRule.operand(second, RelOptRule.any()));
    }

    public static <T> List<T> intersect(List<T> left, List<T> right) {
        if (F.isEmpty(left) || F.isEmpty(right))
            return Collections.emptyList();
        else if (left.size() > right.size())
            return intersect0(right, left);
        else
            return intersect0(left, right);
    }

    public static <T> List<T> intersect0(List<T> left, List<T> right) {
        List<T> res = new ArrayList<>(Math.min(left.size(), right.size()));
        HashSet<T> set = new HashSet<>(left);

        for (T t : right) {
            if (set.contains(t))
                res.add(t);
        }

        return res;
    }

    public static <T> List<T> concat(List<T> col, T... elements) {
        ArrayList<T> res = new ArrayList<>(col.size() + elements.length);

        res.addAll(col);
        res.addAll(Arrays.asList(elements));

        return res;
    }

    @SuppressWarnings("unchecked")
    public static <T> List<T> cast(List<?> src) {
        return (List)src;
    }

    public static <T,R> List<R> transform(@NotNull List<T> src, @NotNull Function<T,R> mapFun) {
        List<R> list = new ArrayList<>(src.size());

        for (T t : src)
            list.add(mapFun.apply(t));

        return list;
    }

    public static <T,R> Set<R> transform(@NotNull Set<T> src, @NotNull Function<T,R> mapFun) {
        Set<R> set = new HashSet<>(src.size());

        for (T t : src)
            set.add(mapFun.apply(t));

        return set;
    }

    public static PlannerContext plannerContext(RelNode rel) {
        return plannerContext(rel.getCluster().getPlanner().getContext());
    }

    public static PlannerContext plannerContext(Context ctx) {
        return ctx.unwrap(PlannerContext.class);
    }

    public static IgniteRel igniteRel(RelNode rel) {
        if (rel.getConvention() != IgniteConvention.INSTANCE)
            throw new AssertionError("Unexpected node: " + rel);

        return (IgniteRel) rel;
    }


    public static boolean isBinaryComparison(RexNode exp) {
        return TREE_INDEX_COMPARISON.contains(exp.getKind()) &&
            (exp instanceof RexCall) && // TODO is it possible to be the not RexCall here?
            ((RexCall)exp).getOperands().size() == 2;
    }


}
