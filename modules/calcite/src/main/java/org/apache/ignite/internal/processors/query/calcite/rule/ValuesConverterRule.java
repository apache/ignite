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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteValues;

import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.broadcast;

/**
 *
 */
public class ValuesConverterRule extends AbstractIgniteConverterRule<LogicalValues> {
    /** */
    public static final RelOptRule INSTANCE = new ValuesConverterRule();

    /**
     * Creates a ConverterRule.
     */
    protected ValuesConverterRule() {
        super(LogicalValues.class);
    }

    /** {@inheritDoc} */
    @Override protected PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq, LogicalValues rel) {
        RelOptCluster cluster = rel.getCluster();
        RelTraitSet traits = cluster.traitSetOf(IgniteConvention.INSTANCE).replace(broadcast());

        return new IgniteValues(cluster, rel.getRowType(), rel.getTuples(), traits);
    }
}
