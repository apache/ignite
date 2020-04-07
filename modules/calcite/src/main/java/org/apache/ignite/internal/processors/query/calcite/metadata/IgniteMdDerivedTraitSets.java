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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteMethod;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.calcite.plan.volcano.VolcanoUtils.deriveSiblings;

/**
 * TODO: Add class description.
 */
public class IgniteMdDerivedTraitSets implements MetadataHandler<IgniteMetadata.DerivedTraitSet> {
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(

            IgniteMethod.DERIVED_TRAIT_SETS.method(), new IgniteMdDerivedTraitSets());
    @Override public MetadataDef<IgniteMetadata.DerivedTraitSet> getDef() {
        return IgniteMetadata.DerivedTraitSet.DEF;
    }

    public Set<RelTraitSet> deriveTraitSets(TableScan scan, RelMetadataQuery mq) {
        return F.asSet(scan.getTraitSet());
    }

    public Set<RelTraitSet> deriveTraitSets(RelNode rel, RelMetadataQuery mq) {
        return ((RelMetadataQueryEx)mq).deriveTraitSets(rel.getInput(0));
    }

    public Set<RelTraitSet> deriveTraitSets(Values rel, RelMetadataQuery mq) {
        return F.asSet(rel.getTraitSet());
    }

    public Set<RelTraitSet> deriveTraitSets(RelSubset rel, RelMetadataQuery mq) {
        List<RelNode> siblings =  deriveSiblings(rel);
        Set<RelTraitSet> traitSets = new HashSet<>();
        for (RelNode sibling : siblings) {
            Set<RelTraitSet> traitSet = ((RelMetadataQueryEx)mq).deriveTraitSets(sibling);
            traitSets.addAll(traitSet);
        }
        return traitSets;
    }
}
