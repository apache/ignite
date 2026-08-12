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

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableSpool;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRecursiveTableSpool;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.single;

/** Converts a transient CTE table spool to query-local delta replacement. */
public class RecursiveTableSpoolConverterRule extends AbstractIgniteConverterRule<LogicalTableSpool> {
    /** Instance. */
    public static final RelOptRule INSTANCE = new RecursiveTableSpoolConverterRule();

    /** */
    private RecursiveTableSpoolConverterRule() {
        super(LogicalTableSpool.class, "RecursiveTableSpoolConverterRule");
    }

    /** {@inheritDoc} */
    @Override protected @Nullable PhysicalNode convert(
        RelOptPlanner planner,
        RelMetadataQuery mq,
        LogicalTableSpool rel
    ) {
        if (!RecursiveCteUtils.isTransient(rel.getTable()))
            return null;

        RelTraitSet traits = rel.getCluster().traitSetOf(IgniteConvention.INSTANCE).replace(single());
        RelNode input = convert(rel.getInput(), traits);

        return new IgniteRecursiveTableSpool(
            rel.getCluster(),
            traits,
            input,
            RecursiveCteUtils.stateId(rel.getTable())
        );
    }
}
