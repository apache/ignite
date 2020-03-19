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

package org.apache.ignite.internal.processors.query.calcite.metadata;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.schema.DistributedTable;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Implementation class for {@link RelMetadataQuery#distribution(RelNode)} method call.
 */
public class IgniteMdDistribution implements MetadataHandler<BuiltInMetadata.Distribution> {
    /**
     * Metadata provider, responsible for distribution type request. It uses this implementation class under the hood.
     */
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            BuiltInMethod.DISTRIBUTION.method, new IgniteMdDistribution());

    /** {@inheritDoc} */
    @Override public MetadataDef<BuiltInMetadata.Distribution> getDef() {
        return BuiltInMetadata.Distribution.DEF;
    }

    /**
     * Requests actual distribution type of the given relational node.
     * @param rel Relational node.
     * @param mq Metadata query instance. Used to request appropriate metadata from node children.
     * @return Distribution type of the given relational node.
     */
    public IgniteDistribution distribution(RelNode rel, RelMetadataQuery mq) {
        return DistributionTraitDef.INSTANCE.getDefault();
    }

    /**
     * See {@link IgniteMdDistribution#distribution(RelNode, RelMetadataQuery)}
     */
    public IgniteDistribution distribution(IgniteRel rel, RelMetadataQuery mq) {
        return rel.getTraitSet().getTrait(DistributionTraitDef.INSTANCE);
    }

    /**
     * See {@link IgniteMdDistribution#distribution(RelNode, RelMetadataQuery)}
     */
    public IgniteDistribution distribution(LogicalTableModify rel, RelMetadataQuery mq) {
        return IgniteDistributions.single(); // TODO skip reducer
    }

    /**
     * See {@link IgniteMdDistribution#distribution(RelNode, RelMetadataQuery)}
     */
    public IgniteDistribution distribution(LogicalFilter rel, RelMetadataQuery mq) {
        return filter(mq, rel.getInput(), rel.getCondition());
    }

    /**
     * See {@link IgniteMdDistribution#distribution(RelNode, RelMetadataQuery)}
     */
    public IgniteDistribution distribution(LogicalProject rel, RelMetadataQuery mq) {
        return project(mq, rel.getInput(), rel.getProjects());
    }

    /**
     * See {@link IgniteMdDistribution#distribution(RelNode, RelMetadataQuery)}
     */
    public IgniteDistribution distribution(LogicalJoin rel, RelMetadataQuery mq) {
        return join(mq, rel.getLeft(), rel.getRight(), rel.analyzeCondition(), rel.getJoinType());
    }

    /**
     * See {@link IgniteMdDistribution#distribution(RelNode, RelMetadataQuery)}
     */
    public IgniteDistribution distribution(RelSubset rel, RelMetadataQuery mq) {
        return rel.getTraitSet().getTrait(DistributionTraitDef.INSTANCE);
    }

    /**
     * See {@link IgniteMdDistribution#distribution(RelNode, RelMetadataQuery)}
     */
    public IgniteDistribution distribution(LogicalTableScan rel, RelMetadataQuery mq) {
        return rel.getTable().unwrap(DistributedTable.class).distribution();
    }

    /**
     * See {@link IgniteMdDistribution#distribution(RelNode, RelMetadataQuery)}
     */
    public IgniteDistribution distribution(LogicalValues rel, RelMetadataQuery mq) {
        return values(rel.getRowType(), rel.getTuples());
    }

    /**
     * See {@link IgniteMdDistribution#distribution(RelNode, RelMetadataQuery)}
     */
    public IgniteDistribution distribution(LogicalExchange rel, RelMetadataQuery mq) {
        return (IgniteDistribution) rel.distribution;
    }

    /**
     * See {@link IgniteMdDistribution#distribution(RelNode, RelMetadataQuery)}
     */
    public IgniteDistribution distribution(HepRelVertex rel, RelMetadataQuery mq) {
        return _distribution(rel.getCurrentRel(), mq);
    }

    /**
     * @return Values relational node distribution.
     */
    public static IgniteDistribution values(RelDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples) {
        return IgniteDistributions.broadcast();
    }

    /**
     * @return Project relational node distribution calculated on the basis of its input and projections.
     */
    public static IgniteDistribution project(RelMetadataQuery mq, RelNode input, List<? extends RexNode> projects) {
        return project(input.getRowType(), _distribution(input, mq), projects);
    }

    /**
     * @return Project relational node distribution calculated on the basis of its input distribution and projections.
     */
    public static IgniteDistribution project(RelDataType inType, IgniteDistribution inDistr, List<? extends RexNode> projects) {
        return inDistr.apply(Project.getPartialMapping(inType.getFieldCount(), projects));
    }

    /**
     * @return Filter relational node distribution calculated on the basis of its input.
     */
    public static IgniteDistribution filter(RelMetadataQuery mq, RelNode input, RexNode condition) {
        return _distribution(input, mq);
    }

    /**
     * @return Join relational node distribution calculated on the basis of its inputs and join information.
     */
    public static IgniteDistribution join(RelMetadataQuery mq, RelNode left, RelNode right, JoinInfo joinInfo, JoinRelType joinType) {
        return F.first(IgniteDistributions.suggestJoin(_distribution(left, mq), _distribution(right, mq), joinInfo, joinType)).out();
    }

    /**
     * Distribution request entry point.
     * @param rel Relational node.
     * @param mq Metadata query instance.
     * @return Actual distribution of the given relational node.
     */
    public static IgniteDistribution _distribution(RelNode rel, RelMetadataQuery mq) {
        assert mq instanceof RelMetadataQueryEx;

        return (IgniteDistribution) mq.distribution(rel);
    }
}
