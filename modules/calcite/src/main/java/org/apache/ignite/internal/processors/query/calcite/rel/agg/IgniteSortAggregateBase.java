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

package org.apache.ignite.internal.processors.query.calcite.rel.agg;

import java.util.List;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitsAwareIgniteRel;

import static org.apache.ignite.internal.processors.query.calcite.util.Commons.maxPrefix;

/**
 *
 */
interface IgniteSortAggregateBase extends TraitsAwareIgniteRel {
    /**
     * Returns a bit set of the grouping fields.
     *
     * @return bit set of ordinals of grouping fields
     */
    ImmutableBitSet getGroupSet();

    /** {@inheritDoc} */
    @Override default Pair<RelTraitSet, List<RelTraitSet>> passThroughCollation(
        RelTraitSet nodeTraits, List<RelTraitSet> inputTraits
    ) {
        RelCollation collation = RelCollations.of(ImmutableIntList.copyOf(getGroupSet().asList()));

        return Pair.of(nodeTraits.replace(collation),
            List.of(inputTraits.get(0).replace(collation)));
    }

    /** {@inheritDoc} */
    @Override default List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCollation(
        RelTraitSet nodeTraits, List<RelTraitSet> inputTraits
    ) {
        RelCollation inputCollation = TraitUtils.collation(inputTraits.get(0));

        List<Integer> newCollation = maxPrefix(inputCollation.getKeys(), getGroupSet().asSet());

        if (newCollation.size() < getGroupSet().cardinality())
            return List.of();

        return List.of(Pair.of(
            nodeTraits.replace(RelCollations.of(ImmutableIntList.copyOf(newCollation))),
            inputTraits
        ));
    }
}
