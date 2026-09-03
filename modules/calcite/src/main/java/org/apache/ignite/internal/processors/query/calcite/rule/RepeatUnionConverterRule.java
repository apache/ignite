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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.logical.LogicalRepeatUnion;
import org.apache.calcite.rel.logical.LogicalTableSpool;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRepeatUnion;

import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.single;

/** Converts Calcite's logical recursive union to coordinator-side execution. */
public class RepeatUnionConverterRule extends AbstractIgniteConverterRule<LogicalRepeatUnion> {
    /** Instance. */
    public static final RelOptRule INSTANCE = new RepeatUnionConverterRule();

    /** */
    private RepeatUnionConverterRule() {
        super(LogicalRepeatUnion.class, "RecursiveCteConverterRule");
    }

    /** {@inheritDoc} */
    @Override protected PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq, LogicalRepeatUnion rel) {
        RelOptTable table = rel.getTransientTable();

        if (table == null || !RecursiveCteUtils.isTransient(table))
            throw unsupported("a transient table is required");

        // TODO: IGNITE-29012 Support recursive CTE with UNION DISTINCT.
        if (!rel.all)
            throw unsupported("only UNION ALL is supported");

        String stateId = RecursiveCteUtils.stateId(planner, table);
        int iterationLimit = planner.getContext().unwrap(PlanningContext.class).recursiveCteIterationLimit();

        RelNode seed = unwrapSpool(rel.getSeedRel(), table, "seed");
        RelNode iterative = unwrapSpool(rel.getIterativeRel(), table, "recursive term");

        RelOptCluster cluster = rel.getCluster();
        RelTraitSet traits = cluster.traitSetOf(IgniteConvention.INSTANCE).replace(single());
        iterative = RecursiveCteUtils.convertStaticInputs(iterative, table, traits);

        return new IgniteRepeatUnion(
            cluster,
            traits,
            convert(seed, traits),
            convert(iterative, traits),
            stateId,
            iterationLimit
        );
    }

    /** */
    private static RelNode unwrapSpool(RelNode rel, RelOptTable table, String term) {
        rel = RecursiveCteUtils.original(rel);

        if (!(rel instanceof LogicalTableSpool))
            throw unsupported("the " + term + " must use a transient table spool");

        LogicalTableSpool spool = (LogicalTableSpool)rel;

        if (!RecursiveCteUtils.sameTransientTable(spool.getTable(), table)) {
            throw unsupported("the " + term + " must target the recursive transient table");
        }

        if (spool.readType != Spool.Type.LAZY || spool.writeType != Spool.Type.LAZY)
            throw unsupported("only lazy transient table spools are supported");

        return spool.getInput();
    }

    /** */
    private static IgniteSQLException unsupported(String detail) {
        return new IgniteSQLException(
            "Unsupported recursive CTE: " + detail,
            IgniteQueryErrorCode.UNSUPPORTED_OPERATION
        );
    }
}
