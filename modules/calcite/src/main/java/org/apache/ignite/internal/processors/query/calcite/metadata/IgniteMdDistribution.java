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

import java.util.List;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
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
        return rel.distribution();
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
        return _distribution(rel.getInput(), mq);
    }

    /**
     * See {@link IgniteMdDistribution#distribution(RelNode, RelMetadataQuery)}
     */
    public IgniteDistribution distribution(LogicalProject rel, RelMetadataQuery mq) {
        return IgniteDistributions.project(mq, rel.getInput(), rel.getProjects());
    }

    /**
     * See {@link IgniteMdDistribution#distribution(RelNode, RelMetadataQuery)}
     */
    public IgniteDistribution distribution(LogicalJoin rel, RelMetadataQuery mq) {
        IgniteDistribution leftIn = _distribution(rel.getLeft(), mq);
        IgniteDistribution rightIn = _distribution(rel.getRight(), mq);

        List<IgniteDistributions.BiSuggestion> suggestions = IgniteDistributions.suggestJoin(
            leftIn, rightIn, rel.analyzeCondition(), rel.getJoinType());

        return F.first(suggestions).out();
    }

    /**
     * See {@link IgniteMdDistribution#distribution(RelNode, RelMetadataQuery)}
     */
    public IgniteDistribution distribution(LogicalAggregate rel, RelMetadataQuery mq) {
        IgniteDistribution inDistr = _distribution(rel.getInput(), mq);

        List<IgniteDistributions.Suggestion> suggestions =
            IgniteDistributions.suggestAggregate(inDistr, rel.getGroupSet(), rel.getGroupSets());

        return F.first(suggestions).out();
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
        return IgniteDistributions.broadcast();
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
