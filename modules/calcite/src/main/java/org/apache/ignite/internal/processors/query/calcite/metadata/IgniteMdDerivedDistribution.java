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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.VolcanoUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMetadata.DerivedDistribution;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteMethod;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Implementation class for {@link RelMetadataQueryEx#derivedDistributions(RelNode)} method call.
 */
public class IgniteMdDerivedDistribution implements MetadataHandler<DerivedDistribution> {
    /**
     * Holds initially requested convention. In case there is no physical nodes in interested RelSubset we need to discover
     * another RelSubset, which holds logical ones (to calculate possible distribution types instead of actual).
     * On a deeper layer we need to return to initially requested RelSubset because we primarily interested
     * in physical nodes, but logical ones cannot have them as children, so, we use a value from this holder
     * to request RelSubset of possible physical nodes of a logical parent.
     */
    private static final ThreadLocal<Convention> REQUESTED_CONVENTION = ThreadLocal.withInitial(() -> Convention.NONE);

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
     * See {@link IgniteMdDerivedDistribution#deriveDistributions(RelNode, RelMetadataQuery)}
     */
    public List<IgniteDistribution> deriveDistributions(IgniteRel rel, RelMetadataQuery mq) {
        return F.asList(IgniteMdDistribution._distribution(rel, mq));
    }

    /**
     * See {@link IgniteMdDerivedDistribution#deriveDistributions(RelNode, RelMetadataQuery)}
     */
    public List<IgniteDistribution> deriveDistributions(LogicalTableScan rel, RelMetadataQuery mq) {
        return F.asList(IgniteMdDistribution._distribution(rel, mq));
    }

    /**
     * See {@link IgniteMdDerivedDistribution#deriveDistributions(RelNode, RelMetadataQuery)}
     */
    public List<IgniteDistribution> deriveDistributions(LogicalValues rel, RelMetadataQuery mq) {
        return F.asList(IgniteMdDistribution._distribution(rel, mq));
    }

    /**
     * See {@link IgniteMdDerivedDistribution#deriveDistributions(RelNode, RelMetadataQuery)}
     */
    public List<IgniteDistribution> deriveDistributions(LogicalProject rel, RelMetadataQuery mq) {
        Mappings.TargetMapping mapping =
            Project.getPartialMapping(rel.getInput().getRowType().getFieldCount(), rel.getProjects());

        return Commons.transform(_deriveDistributions(rel.getInput(), mq), i -> i.apply(mapping));
    }

    /**
     * See {@link IgniteMdDerivedDistribution#deriveDistributions(RelNode, RelMetadataQuery)}
     */
    public List<IgniteDistribution> deriveDistributions(SingleRel rel, RelMetadataQuery mq) {
        if (rel instanceof IgniteRel)
            return deriveDistributions((IgniteRel)rel, mq);

        return _deriveDistributions(rel.getInput(), mq);
    }

    /**
     * Here we trying to get physical nodes and request distribution types from them, in case there is no physical
     * nodes, we get logical ones and derive possible distribution types they may satisfy with.
     *
     * For general information see {@link IgniteMdDerivedDistribution#deriveDistributions(RelNode, RelMetadataQuery)}
     */
    public List<IgniteDistribution> deriveDistributions(RelSubset rel, RelMetadataQuery mq) {
        rel = VolcanoUtils.subset(rel, rel.getTraitSet().replace(REQUESTED_CONVENTION.get()));

        HashSet<IgniteDistribution> res = new HashSet<>();

        for (RelNode rel0 : rel.getRels())
            res.addAll(_deriveDistributions(rel0, mq));

        if (F.isEmpty(res)) {
            // default traits + NONE convention return a set of all logical rels.
            RelSubset newRel = VolcanoUtils.subset(rel, rel.getCluster().traitSetOf(Convention.NONE));

            if (newRel != rel) {
                for (RelNode rel0 : newRel.getRels())
                    res.addAll(_deriveDistributions(rel0, mq));
            }
        }

        return new ArrayList<>(res);
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
    public List<IgniteDistribution> deriveDistributions(LogicalJoin rel, RelMetadataQuery mq) {
        List<IgniteDistribution> left = _deriveDistributions(rel.getLeft(), mq);
        List<IgniteDistribution> right = _deriveDistributions(rel.getRight(), mq);

        return Commons.transform(IgniteDistributions.suggestJoin(left, right, rel.analyzeCondition(), rel.getJoinType()),
            IgniteDistributions.BiSuggestion::out);
    }

    /**
     * Derivation entry point. Returns actual (or possible) distribution types of given relational node.
     * @param rel Relational node.
     * @param convention Required convention.
     * @param mq Metadata query instance.
     * @return List of distribution types the given relational node may have.
     */
    public static List<IgniteDistribution> deriveDistributions(RelNode rel, Convention convention, RelMetadataQuery mq) {
        try {
            REQUESTED_CONVENTION.set(convention);

            return _deriveDistributions(rel, mq);
        }
        finally {
            REQUESTED_CONVENTION.remove();
        }
    }

    /** */
    private static List<IgniteDistribution> _deriveDistributions(RelNode rel, RelMetadataQuery mq) {
        assert mq instanceof RelMetadataQueryEx;

        return ((RelMetadataQueryEx) mq).derivedDistributions(rel);
    }
}
