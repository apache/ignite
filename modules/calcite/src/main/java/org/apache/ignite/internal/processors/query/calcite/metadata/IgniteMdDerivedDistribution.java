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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMetadata.DerivedDistribution;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.BiSuggestion;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.Suggestion;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteMethod;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Implementation class for {@link RelMetadataQueryEx#derivedDistributions(RelNode)} method call.
 */
public class IgniteMdDerivedDistribution implements MetadataHandler<DerivedDistribution> {
    /**
     * Metadata provider, responsible for distribution types derivation. It uses this implementation class under the hood.
     */
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            IgniteMethod.DERIVED_DISTRIBUTIONS.method(), new IgniteMdDerivedDistribution());

    /** {@inheritDoc} */
    @Override public MetadataDef<DerivedDistribution> getDef() {
        return DerivedDistribution.DEF;
    }

    /**
     * Requests possible distribution types of given relational node. In case the node is logical and
     * @param rel Relational node.
     * @param mq Metadata query instance. Used to request appropriate metadata from node children.
     * @return List of distribution types the given relational node may have.
     */
    public List<IgniteDistribution> deriveDistributions(RelNode rel, RelMetadataQuery mq) {
        return F.asList(IgniteMdDistribution._distribution(rel, mq));
    }

    /**
     * See {@link IgniteMdDerivedDistribution#deriveDistributions(RelNode, RelMetadataQuery)}
     */
    public List<IgniteDistribution> deriveDistributions(AbstractConverter rel, RelMetadataQuery mq) {
        return Collections.emptyList();
    }

    /**
     * Here we trying to get physical nodes and request distribution types from them, in case there is no physical
     * nodes, we get logical ones and derive possible distribution types they may satisfy with.
     *
     * For general information see {@link IgniteMdDerivedDistribution#deriveDistributions(RelNode, RelMetadataQuery)}
     */
    public List<IgniteDistribution> deriveDistributions(RelSubset rel, RelMetadataQuery mq) {
        HashSet<IgniteDistribution> res = new HashSet<>();

        RelSubset newSubset = subset(rel, IgniteConvention.INSTANCE);

        for (RelNode rel0 : newSubset.getRels())
            res.addAll(_deriveDistributions(rel0, mq));

        if (!F.isEmpty(res))
            return new ArrayList<>(res);

        newSubset = subset(rel, Convention.NONE);

        for (RelNode rel0 : newSubset.getRels())
            res.addAll(_deriveDistributions(rel0, mq));

        if (!F.isEmpty(res))
            return new ArrayList<>(res);

        return F.asList(IgniteMdDistribution._distribution(rel, mq));
    }

    /**
     * See {@link IgniteMdDerivedDistribution#deriveDistributions(RelNode, RelMetadataQuery)}
     */
    public List<IgniteDistribution> deriveDistributions(HepRelVertex rel, RelMetadataQuery mq) {
        return _deriveDistributions(rel.getCurrentRel(), mq);
    }

    /**
     * See {@link IgniteMdDerivedDistribution#deriveDistributions(RelNode, RelMetadataQuery)}
     */
    public List<IgniteDistribution> deriveDistributions(LogicalFilter rel, RelMetadataQuery mq) {
        return _deriveDistributions(rel.getInput(), mq);
    }

    /**
     * See {@link IgniteMdDerivedDistribution#deriveDistributions(RelNode, RelMetadataQuery)}
     */
    public List<IgniteDistribution> deriveDistributions(LogicalProject rel, RelMetadataQuery mq) {
        HashSet<IgniteDistribution> res = new HashSet<>();

        Mappings.TargetMapping mapping =
            Project.getPartialMapping(rel.getInput().getRowType().getFieldCount(), rel.getProjects());

        for (IgniteDistribution inDistr : _deriveDistributions(rel.getInput(), mq))
            res.add(inDistr.apply(mapping));

        return new ArrayList<>(res);
    }

    /**
     * See {@link IgniteMdDerivedDistribution#deriveDistributions(RelNode, RelMetadataQuery)}
     */
    public List<IgniteDistribution> deriveDistributions(LogicalAggregate rel, RelMetadataQuery mq) {
        List<Suggestion> suggestions = IgniteDistributions.suggestAggregate(
            mq, rel.getInput(), rel.getGroupSet(), rel.getGroupSets());

        return Commons.transform(suggestions, Suggestion::out);
    }

    /**
     * See {@link IgniteMdDerivedDistribution#deriveDistributions(RelNode, RelMetadataQuery)}
     */
    public List<IgniteDistribution> deriveDistributions(LogicalJoin rel, RelMetadataQuery mq) {
        List<BiSuggestion> suggestions = IgniteDistributions.suggestJoin(
            mq, rel.getLeft(), rel.getRight(), rel.analyzeCondition(), rel.getJoinType());

        return Commons.transform(suggestions, BiSuggestion::out);
    }

    /**
     * See {@link IgniteMdDerivedDistribution#deriveDistributions(RelNode, RelMetadataQuery)}
     */
    public List<IgniteDistribution> deriveDistributions(LogicalUnion rel, RelMetadataQuery mq) {
        List<IgniteDistribution> distributions = Commons.transform(
            IgniteDistributions.suggestUnionAll(mq, rel.getInputs()), Suggestion::out);

        if (rel.all)
            return distributions;

        // UNION becomes DISTINCT + UNION ALL where DISTINCT is implemented as an Aggregate
        ImmutableBitSet groupSet = ImmutableBitSet.range(rel.getRowType().getFieldCount());
        List<ImmutableBitSet> groupSets = ImmutableList.of(groupSet);

        return Commons.transform(
            IgniteDistributions.suggestAggregate(distributions, groupSet, groupSets), Suggestion::out);
    }

    /** */
    public static List<IgniteDistribution> _deriveDistributions(RelNode rel, RelMetadataQuery mq) {
        assert mq instanceof RelMetadataQueryEx;

        return ((RelMetadataQueryEx) mq).derivedDistributions(rel);
    }

    /** */
    private static RelSubset subset(RelSubset rel, Convention convention) {
        VolcanoPlanner planner = (VolcanoPlanner) rel.getCluster().getPlanner();
        return planner.getSubset(rel, rel.getCluster().traitSetOf(convention), true);
    }
}
