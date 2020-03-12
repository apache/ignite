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

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdDerivedDistribution;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteJoin;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.jetbrains.annotations.NotNull;

/**
 * Ignite Join converter.
 */
public class JoinConverter extends IgniteConverter {
    public static final ConverterRule INSTANCE = new JoinConverter();

    /**
     * Creates a converter.
     */
    public JoinConverter() {
        super(LogicalJoin.class, "JoinConverter");
    }

    /** {@inheritDoc} */
    @Override protected List<RelNode> convert0(@NotNull RelNode rel) {
        LogicalJoin join = (LogicalJoin) rel;

        RelNode left = convert(join.getLeft(), IgniteConvention.INSTANCE);
        RelNode right = convert(join.getRight(), IgniteConvention.INSTANCE);

        RelOptCluster cluster = join.getCluster();
        RelMetadataQuery mq = cluster.getMetadataQuery();

        List<IgniteDistribution> leftTraits = IgniteMdDerivedDistribution.deriveDistributions(left, IgniteConvention.INSTANCE, mq);
        List<IgniteDistribution> rightTraits = IgniteMdDerivedDistribution.deriveDistributions(right, IgniteConvention.INSTANCE, mq);

        List<IgniteDistributions.BiSuggestion> suggestions = IgniteDistributions.suggestJoin(leftTraits, rightTraits, join.analyzeCondition(), join.getJoinType());

        return Commons.transform(suggestions, s -> create(join, left, right, s));
    }

    /** */
    private static RelNode create(LogicalJoin join, RelNode left, RelNode right, IgniteDistributions.BiSuggestion suggest) {
        left = convert(left, suggest.left());
        right = convert(right, suggest.right());

        RelTraitSet traitSet = join.getTraitSet().replace(IgniteConvention.INSTANCE).replace(suggest.out());

        return new IgniteJoin(join.getCluster(), traitSet, left, right, join.getCondition(), join.getVariablesSet(), join.getJoinType());
    }
}
