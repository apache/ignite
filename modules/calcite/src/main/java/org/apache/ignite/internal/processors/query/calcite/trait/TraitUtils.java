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

package org.apache.ignite.internal.processors.query.calcite.trait;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSort;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTrimExchange;
import org.jetbrains.annotations.Nullable;

import static org.apache.calcite.rel.RelDistribution.Type.BROADCAST_DISTRIBUTED;
import static org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.any;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.single;

/**
 *
 */
public class TraitUtils {
    /** */
    @Nullable public static RelNode enforce(RelNode rel, RelTraitSet toTraits) {
        RelOptPlanner planner = rel.getCluster().getPlanner();
        RelTraitSet fromTraits = rel.getTraitSet();
        if (!fromTraits.satisfies(toTraits)) {
            for (int i = 0; (rel != null) && (i < toTraits.size()); i++) {
                RelTrait fromTrait = rel.getTraitSet().getTrait(i);
                RelTrait toTrait = toTraits.getTrait(i);

                if (fromTrait.satisfies(toTrait))
                    continue;

                RelNode old = rel;
                rel = convertTrait(planner, fromTrait, toTrait, old);

                if (rel != null)
                    rel = planner.register(rel, old);

                assert rel == null || rel.getTraitSet().getTrait(i).satisfies(toTrait);
            }

            assert rel == null || rel.getTraitSet().satisfies(toTraits);
        }

        return rel;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Nullable private static RelNode convertTrait(RelOptPlanner planner, RelTrait fromTrait, RelTrait toTrait, RelNode rel) {
        assert fromTrait.getTraitDef() == toTrait.getTraitDef();

        RelTraitDef converter = fromTrait.getTraitDef();

        if (converter == RelCollationTraitDef.INSTANCE)
            return convertCollation(planner, (RelCollation)toTrait, rel);
        else if (converter == DistributionTraitDef.INSTANCE)
            return convertDistribution(planner, (IgniteDistribution)toTrait, rel);
        else if (converter.canConvert(planner, fromTrait, toTrait))
            return converter.convert(planner, rel, toTrait, true);

        return null;
    }

    /** */
    @Nullable public static RelNode convertCollation(RelOptPlanner planner,
        RelCollation toTrait, RelNode rel) {
        if (toTrait.getFieldCollations().isEmpty())
            return null;

        RelNode result = new IgniteSort(rel.getCluster(), rel.getTraitSet().replace(toTrait),
            rel, toTrait, null, null);

        return planner.register(result, rel);
    }

    /** */
    @Nullable public static RelNode convertDistribution(RelOptPlanner planner,
        IgniteDistribution toTrait, RelNode rel) {

        if (toTrait.getType() == RelDistribution.Type.ANY)
            return null;

        RelNode result;
        IgniteDistribution fromTrait = distribution(rel.getTraitSet());
        if (fromTrait.getType() == BROADCAST_DISTRIBUTED && toTrait.getType() == HASH_DISTRIBUTED)
            result = new IgniteTrimExchange(rel.getCluster(), rel.getTraitSet().replace(toTrait), rel, toTrait);
        else {
            result = new IgniteExchange(rel.getCluster(), rel.getTraitSet().replace(toTrait),
                RelOptRule.convert(rel, any()), toTrait);
        }

        return planner.register(result, rel);
    }

    /** */
    public static RelTraitSet fixTraits(RelTraitSet traits) {
        if (distribution(traits) == any())
            traits = traits.replace(single());

        return traits.replace(IgniteConvention.INSTANCE);
    }

    public static IgniteDistribution distribution(RelTraitSet traits) {
        return traits.getTrait(DistributionTraitDef.INSTANCE);
    }

    public static RelCollation collation(RelTraitSet traits) {
        return traits.getTrait(RelCollationTraitDef.INSTANCE);
    }

    /** */
    public static RelInput changeTraits(RelInput input, RelTrait... traits) {
        RelTraitSet traitSet = input.getTraitSet();

        for (RelTrait trait : traits)
            traitSet = traitSet.replace(trait);

        RelTraitSet traitSet0 = traitSet;

        return new RelInput() {
            @Override public RelOptCluster getCluster() {
                return input.getCluster();
            }

            @Override public RelTraitSet getTraitSet() {
                return traitSet0;
            }

            @Override public RelOptTable getTable(String table) {
                return input.getTable(table);
            }

            @Override public RelNode getInput() {
                return input.getInput();
            }

            @Override public List<RelNode> getInputs() {
                return input.getInputs();
            }

            @Override public RexNode getExpression(String tag) {
                return input.getExpression(tag);
            }

            @Override public ImmutableBitSet getBitSet(String tag) {
                return input.getBitSet(tag);
            }

            @Override public List<ImmutableBitSet> getBitSetList(String tag) {
                return input.getBitSetList(tag);
            }

            @Override public List<AggregateCall> getAggregateCalls(String tag) {
                return input.getAggregateCalls(tag);
            }

            @Override public Object get(String tag) {
                return input.get(tag);
            }

            @Override public String getString(String tag) {
                return input.getString(tag);
            }

            @Override public float getFloat(String tag) {
                return input.getFloat(tag);
            }

            @Override public <E extends Enum<E>> E getEnum(String tag, Class<E> enumClass) {
                return input.getEnum(tag, enumClass);
            }

            @Override public List<RexNode> getExpressionList(String tag) {
                return input.getExpressionList(tag);
            }

            @Override public List<String> getStringList(String tag) {
                return input.getStringList(tag);
            }

            @Override public List<Integer> getIntegerList(String tag) {
                return input.getIntegerList(tag);
            }

            @Override public List<List<Integer>> getIntegerListList(String tag) {
                return input.getIntegerListList(tag);
            }

            @Override public RelDataType getRowType(String tag) {
                return input.getRowType(tag);
            }

            @Override public RelDataType getRowType(String expressionsTag, String fieldsTag) {
                return input.getRowType(expressionsTag, fieldsTag);
            }

            @Override public RelCollation getCollation() {
                return input.getCollation();
            }

            @Override public RelDistribution getDistribution() {
                return input.getDistribution();
            }

            @Override public ImmutableList<ImmutableList<RexLiteral>> getTuples(
                String tag) {
                return input.getTuples(tag);
            }

            @Override public boolean getBoolean(String tag, boolean default_) {
                return input.getBoolean(tag, default_);
            }
        };
    }
}
