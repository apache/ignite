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
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptNode;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.type.RowType;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public final class Commons {
    private static final int[] EMPTY = new int[0];

    private Commons(){}

    public static Context convert(QueryContext ctx) {
        return ctx == null ? Contexts.empty() : Contexts.of(ctx.unwrap(Object[].class));
    }

    public static <T> @Nullable T provided(Context ctx, Class<T> paramType, Supplier<T> paramSrc) {
        T param = ctx.unwrap(paramType);

        if (param != null)
            return null; // Provided by parent context.

        return paramSrc.get();
    }

    public static <T> T contextParam(Context ctx, Class<T> paramType, Supplier<T> paramSrc) {
        T param = ctx.unwrap(paramType);

        if (param != null)
            return param;

        return paramSrc.get();
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

    public static int[] intersect(int[] left, int[] right) {
        if (F.isEmpty(left) || F.isEmpty(right))
            return EMPTY;

        int[] res = null;

        int i = 0, j = 0, k = 0, size = Math.min(left.length, right.length);

        while (i < left.length && j < right.length) {
            if (left[i] < right[j])
                i++;
            else if (right[j] < left[i])
                j++;
            else {
                if (res == null)
                    res = new int[size];

                res[k++] = left[i];

                i++;
                j++;
            }
        }

        if (k == 0)
            return EMPTY;

        return res.length == k ? res : Arrays.copyOf(res, k);
    }

    public static <T> List<T> intersect(List<T> left, List<T> right) {
        if (F.isEmpty(left) || F.isEmpty(right))
            return Collections.emptyList();

        HashSet<T> set = new HashSet<>(right);

        return left.stream().filter(set::contains).collect(Collectors.toList());
    }

    public static <T> List<T> union(List<T> left, List<T> right) {
        Set<T> set = U.newHashSet(left.size() + right.size());

        set.addAll(left);
        set.addAll(right);

        return new ArrayList<>(set);
    }

    public static int[] union(int[] left, int[] right) {
        if (F.isEmpty(left) && F.isEmpty(right))
            return EMPTY;

        int min = Math.min(left.length, right.length);
        int max = left.length + right.length;
        int expected = Math.max(min, (int) (max * 1.5));

        int[] res = new int[U.ceilPow2(expected)];

        int i = 0, j = 0, k = 0;

        while (i < left.length && j < right.length) {
            res = ensureSize(res, k + 1);

            if (left[i] < right[j])
                res[k++] = left[i++];
            else if (right[j] < left[i])
                res[k++] = right[j++];
            else {
                res[k++] = left[i];

                i++;
                j++;
            }
        }

        if (k == 0)
            return EMPTY;

        return res.length == k ? res : Arrays.copyOf(res, k);
    }

    private static int[] ensureSize(int[] array, int size) {
        return size < array.length ? array : Arrays.copyOf(array, U.ceilPow2(size));
    }

    public static <T> List<T> concat(List<T> col, T... elements) {
        ArrayList<T> res = new ArrayList<>(col.size() + elements.length);

        res.addAll(col);
        Collections.addAll(res, elements);

        return res;
    }
}
