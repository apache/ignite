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

package org.apache.ignite.internal.processors.query.calcite.rule;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.schema.TransientTable;
import org.apache.ignite.internal.processors.query.calcite.prepare.BaseQueryContext;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalRecursiveStaticSpool;

/** Utilities shared by recursive CTE converter rules. */
final class RecursiveCteUtils {
    /** */
    private RecursiveCteUtils() {
        // No-op.
    }

    /** Returns whether the table is Calcite's query-local transient table. */
    static boolean isTransient(RelOptTable table) {
        return table != null && table.unwrap(TransientTable.class) != null;
    }

    /** Stable identifier preserved in the serialized physical plan. */
    static String stateId(RelOptPlanner planner, RelOptTable table) {
        BaseQueryContext ctx = planner.getContext().unwrap(BaseQueryContext.class);

        assert ctx != null;

        return ctx.recursiveCteStateId(table);
    }

    /** Counts scans of the recursive transient table. */
    static int referenceCount(RelNode rel, RelOptTable table) {
        rel = original(rel);

        int cnt = isRecursiveScan(rel, table) ? 1 : 0;

        for (RelNode input : rel.getInputs())
            cnt += referenceCount(input, table);

        return cnt;
    }

    /** Materializes maximal iteration subtrees that do not depend on the current delta. */
    static RelNode materializeStaticInputs(RelNode rel, RelOptTable table) {
        rel = original(rel);

        if (isRecursiveScan(rel, table))
            return rel;

        List<RelNode> inputs = rel.getInputs();

        if (inputs.isEmpty())
            return rel;

        List<RelNode> newInputs = new ArrayList<>(inputs.size());

        for (RelNode input : inputs) {
            if (referenceCount(input, table) == 0)
                newInputs.add(new IgniteLogicalRecursiveStaticSpool(input));
            else
                newInputs.add(materializeStaticInputs(input, table));
        }

        return rel.copy(rel.getTraitSet(), newInputs);
    }

    /**
     * Returns the logical expression represented by a Volcano subset. Converter rule inputs can be subsets whose own
     * input list is empty.
     */
    static RelNode original(RelNode rel) {
        while (rel instanceof RelSubset) {
            RelNode original = ((RelSubset)rel).getOriginal();

            if (original == null)
                return rel;

            rel = original;
        }

        return rel;
    }

    /** Returns whether both optimizer tables represent the same transient table instance. */
    static boolean sameTransientTable(RelOptTable first, RelOptTable second) {
        TransientTable firstTable = first == null ? null : first.unwrap(TransientTable.class);
        TransientTable secondTable = second == null ? null : second.unwrap(TransientTable.class);

        return firstTable != null && firstTable == secondTable;
    }

    /** */
    private static boolean isRecursiveScan(RelNode rel, RelOptTable table) {
        return rel instanceof TableScan
            && sameTransientTable(((TableScan)rel).getTable(), table);
    }
}
