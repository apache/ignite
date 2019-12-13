/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
 *
 */
public class IgniteMdDerivedDistribution implements MetadataHandler<DerivedDistribution> {
    /** */
    private static final ThreadLocal<Convention> REQUESTED_CONVENTION = ThreadLocal.withInitial(() -> Convention.NONE);

    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            IgniteMethod.DERIVED_DISTRIBUTIONS.method(), new IgniteMdDerivedDistribution());

    @Override public MetadataDef<DerivedDistribution> getDef() {
        return DerivedDistribution.DEF;
    }

    public List<IgniteDistribution> deriveDistributions(AbstractConverter rel, RelMetadataQuery mq) {
        return Collections.emptyList();
    }

    public List<IgniteDistribution> deriveDistributions(RelNode rel, RelMetadataQuery mq) {
        return F.asList(IgniteMdDistribution._distribution(rel, mq));
    }

    public List<IgniteDistribution> deriveDistributions(IgniteRel rel, RelMetadataQuery mq) {
        return F.asList(IgniteMdDistribution._distribution(rel, mq));
    }

    public List<IgniteDistribution> deriveDistributions(LogicalTableScan rel, RelMetadataQuery mq) {
        return F.asList(IgniteMdDistribution._distribution(rel, mq));
    }

    public List<IgniteDistribution> deriveDistributions(LogicalValues rel, RelMetadataQuery mq) {
        return F.asList(IgniteMdDistribution._distribution(rel, mq));
    }

    public List<IgniteDistribution> deriveDistributions(LogicalProject rel, RelMetadataQuery mq) {
        Mappings.TargetMapping mapping =
            Project.getPartialMapping(rel.getInput().getRowType().getFieldCount(), rel.getProjects());

        return Commons.transform(_deriveDistributions(rel.getInput(), mq), i -> i.apply(mapping));
    }

    public List<IgniteDistribution> deriveDistributions(SingleRel rel, RelMetadataQuery mq) {
        if (rel instanceof IgniteRel)
            return deriveDistributions((IgniteRel)rel, mq);

        return _deriveDistributions(rel.getInput(), mq);
    }

    public List<IgniteDistribution> deriveDistributions(RelSubset rel, RelMetadataQuery mq) {
        rel = VolcanoUtils.subset(rel, rel.getTraitSet().replace(REQUESTED_CONVENTION.get()));

        HashSet<IgniteDistribution> res = new HashSet<>();

        for (RelNode rel0 : rel.getRels())
            res.addAll(_deriveDistributions(rel0, mq));

        if (F.isEmpty(res)) {
            RelSubset newRel = VolcanoUtils.subset(rel, rel.getTraitSet().replace(Convention.NONE));

            if (newRel != rel) {
                for (RelNode rel0 : newRel.getRels())
                    res.addAll(_deriveDistributions(rel0, mq));
            }
        }

        return new ArrayList<>(res);
    }

    public List<IgniteDistribution> deriveDistributions(HepRelVertex rel, RelMetadataQuery mq) {
        return _deriveDistributions(rel.getCurrentRel(), mq);
    }

    public List<IgniteDistribution> deriveDistributions(LogicalFilter rel, RelMetadataQuery mq) {
        return _deriveDistributions(rel.getInput(), mq);
    }

    public List<IgniteDistribution> deriveDistributions(LogicalJoin rel, RelMetadataQuery mq) {
        List<IgniteDistribution> left = _deriveDistributions(rel.getLeft(), mq);
        List<IgniteDistribution> right = _deriveDistributions(rel.getRight(), mq);

        return Commons.transform(IgniteDistributions.suggestJoin(left, right, rel.analyzeCondition(), rel.getJoinType()),
            IgniteDistributions.BiSuggestion::out);
    }

    private static List<IgniteDistribution> _deriveDistributions(RelNode rel, RelMetadataQuery mq) {
        return RelMetadataQueryEx.wrap(mq).derivedDistributions(rel);
    }

    public static List<IgniteDistribution> deriveDistributions(RelNode rel, Convention convention, RelMetadataQuery mq) {
        try {
            REQUESTED_CONVENTION.set(convention);

            return _deriveDistributions(rel, mq);
        }
        finally {
            REQUESTED_CONVENTION.remove();
        }
    }
}
