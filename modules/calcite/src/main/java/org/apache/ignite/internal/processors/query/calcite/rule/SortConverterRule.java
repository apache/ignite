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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteLimit;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSort;

/**
 * Converter rule for sort operator.
 */
public class SortConverterRule extends AbstractIgniteConverterRule<LogicalSort> {
    /** */
    public static final RelOptRule INSTANCE = new SortConverterRule();

    /** */
    public SortConverterRule() {
        super(LogicalSort.class);
    }

    /** {@inheritDoc} */
    @Override protected PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq, LogicalSort sort) {
        RelOptCluster cluster = sort.getCluster();
        RelTraitSet outTraits = cluster.traitSetOf(IgniteConvention.INSTANCE).replace(sort.getCollation());
        RelTraitSet inTraits = cluster.traitSetOf(IgniteConvention.INSTANCE);
        RelNode input = convert(sort.getInput(), inTraits);

        IgniteRel res = null;

        if (!sort.getCollation().getFieldCollations().isEmpty())
            input = res = new IgniteSort(cluster, outTraits, input, sort.getCollation(), null, null);

        if (sort.offset != null || sort.fetch != null)
            res = new IgniteLimit(cluster, outTraits, input, sort.offset, sort.fetch);

        assert res != null : "Empty sort node: " + sort;

        return res;
    }
}
