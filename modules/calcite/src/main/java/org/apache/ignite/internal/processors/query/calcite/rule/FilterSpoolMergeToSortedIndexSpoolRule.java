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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Spool;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSortedIndexSpool;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableSpool;
import org.apache.ignite.internal.processors.query.calcite.trait.CorrelationTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.util.IndexConditions;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.util.typedef.F;
import org.immutables.value.Value;

/**
 * Rule that pushes filter into the spool.
 */
@Value.Enclosing
public class FilterSpoolMergeToSortedIndexSpoolRule extends RelRule<FilterSpoolMergeToSortedIndexSpoolRule.Config> {
    /** Instance. */
    public static final RelOptRule INSTANCE = Config.DEFAULT.toRule();

    /** */
    private FilterSpoolMergeToSortedIndexSpoolRule(Config cfg) {
        super(cfg);
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        final IgniteFilter filter = call.rel(0);
        final IgniteTableSpool spool = call.rel(1);

        RelOptCluster cluster = spool.getCluster();

        RelTraitSet trait = spool.getTraitSet();
        CorrelationTrait filterCorr = TraitUtils.correlation(filter);

        if (filterCorr.correlated())
            trait = trait.replace(filterCorr);

        RelNode input = spool.getInput();

        RelCollation inCollation = TraitUtils.collation(input);

        IndexConditions idxCond = RexUtils.buildSortedIndexConditions(
            cluster,
            inCollation,
            filter.getCondition(),
            spool.getRowType(),
            null
        );

        if (F.isEmpty(idxCond.lowerCondition()) && F.isEmpty(idxCond.upperCondition()))
            return;

        List<SearchBounds> bounds = RexUtils.buildSortedSearchBounds(
            cluster,
            inCollation,
            filter.getCondition(),
            spool.getRowType(),
            null
        );

        if (F.isEmpty(bounds))
            return;

        RelCollation traitCollation;
        RelCollation searchCollation;

        if (inCollation == null || inCollation.isDefault()) {
            // Create collation by index condition.
/*
            List<RexNode> lowerBound = idxCond.lowerBound();
            List<RexNode> upperBound = idxCond.upperBound();

            assert lowerBound == null || upperBound == null || lowerBound.size() == upperBound.size();

            int cardinality = lowerBound != null ? lowerBound.size() : upperBound.size();

            List<Integer> equalsFields = new ArrayList<>(cardinality);
            List<Integer> otherFields = new ArrayList<>(cardinality);

            // First, add all equality filters to collation, then add other fields.
            for (int i = 0; i < cardinality; i++) {
                RexNode lowerNode = lowerBound != null ? lowerBound.get(i) : null;
                RexNode upperNode = upperBound != null ? upperBound.get(i) : null;

                if (RexUtils.isNotNull(lowerNode) || RexUtils.isNotNull(upperNode))
                    (F.eq(lowerNode, upperNode) ? equalsFields : otherFields).add(i);
            }

            searchCollation = traitCollation = TraitUtils.createCollation(F.concat(true, equalsFields, otherFields));
*/
            List<Integer> equalsFields = new ArrayList<>(bounds.size());
            List<Integer> otherFields = new ArrayList<>(bounds.size());

            // First, add all equality filters to collation, then add other fields.
            for (int i = 0; i < bounds.size(); i++) {
                if (bounds.get(i) != null)
                    (bounds.get(i).type() == SearchBounds.Type.EXACT ? equalsFields : otherFields).add(i);
            }

            searchCollation = traitCollation = TraitUtils.createCollation(F.concat(true, equalsFields, otherFields));
        }
        else {
            // Create search collation as a prefix of input collation.
            traitCollation = inCollation;

            //Set<Integer> searchKeys = idxCond.keys();
            Set<Integer> searchKeys = Ord.zip(bounds).stream()
                .filter(v -> v.e != null)
                .map(v -> v.i)
                .collect(Collectors.toSet());

            List<RelFieldCollation> collationFields = inCollation.getFieldCollations().subList(0, searchKeys.size());

            assert searchKeys.containsAll(collationFields.stream().map(RelFieldCollation::getFieldIndex)
                .collect(Collectors.toSet())) : "Search condition should be a prefix of collation [searchKeys=" +
                searchKeys + ", collation=" + inCollation + ']';

            searchCollation = RelCollations.of(collationFields);
        }

        RelNode res = new IgniteSortedIndexSpool(
            cluster,
            trait.replace(traitCollation),
            convert(input, input.getTraitSet().replace(traitCollation)),
            searchCollation,
            filter.getCondition(),
            bounds,
            idxCond
        );

        call.transformTo(res);
    }

    /** */
    @SuppressWarnings("ClassNameSameAsAncestorName")
    @Value.Immutable
    public interface Config extends RelRule.Config {
        /** */
        Config DEFAULT = ImmutableFilterSpoolMergeToSortedIndexSpoolRule.Config.of()
            .withDescription("FilterSpoolMergeToSortedIndexSpoolRule")
            .withOperandFor(IgniteFilter.class, IgniteTableSpool.class);

        /** Defines an operand tree for the given classes. */
        default Config withOperandFor(Class<? extends Filter> filterClass, Class<? extends Spool> spoolClass) {
            return withOperandSupplier(
                o0 -> o0.operand(filterClass)
                    .oneInput(o1 -> o1.operand(spoolClass)
                        .anyInputs()
                    )
            )
                .as(Config.class);
        }

        /** {@inheritDoc} */
        @Override default FilterSpoolMergeToSortedIndexSpoolRule toRule() {
            return new FilterSpoolMergeToSortedIndexSpoolRule(this);
        }
    }
}
