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

package org.apache.ignite.internal.processors.query.calcite.rel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.processors.query.calcite.trait.CorrelationTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionFunction;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitsAwareIgniteRel;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

import static org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED;
import static org.apache.calcite.rel.core.JoinRelType.RIGHT;
import static org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdRowCount.joinRowCount;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.broadcast;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.hash;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.random;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.single;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

/** */
public abstract class AbstractIgniteJoin extends Join implements TraitsAwareIgniteRel {
    /** */
    protected AbstractIgniteJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right,
        RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType);
    }

    /** {@inheritDoc} */
    @Override public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .itemIf("variablesSet", Commons.transform(variablesSet.asList(), CorrelationId::getId),
                pw.getDetailLevel() == SqlExplainLevel.ALL_ATTRIBUTES);
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCollation(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        // We preserve left collation since it's translated into a nested loop join with an outer loop
        // over a left edge. The code below checks and projects left collation on an output row type.

        RelTraitSet left = inputTraits.get(0), right = inputTraits.get(1);

        RelCollation collation = TraitUtils.collation(left);

        // If nulls are possible at left we has to check whether NullDirection.LAST flag is set on sorted fields.
        // TODO set NullDirection.LAST for insufficient fields instead of erasing collation.
        if (joinType == RIGHT || joinType == JoinRelType.FULL) {
            for (RelFieldCollation field : collation.getFieldCollations()) {
                if (RelFieldCollation.NullDirection.LAST != field.nullDirection) {
                    collation = RelCollations.EMPTY;
                    break;
                }
            }
        }

        RelTraitSet outTraits = nodeTraits.replace(collation);
        RelTraitSet leftTraits = left.replace(collation);
        RelTraitSet rightTraits = right.replace(RelCollations.EMPTY);

        return List.of(Pair.of(outTraits, List.of(leftTraits, rightTraits)));
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveRewindability(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        // The node is rewindable only if both sources are rewindable.

        RelTraitSet left = inputTraits.get(0), right = inputTraits.get(1);

        RewindabilityTrait leftRewindability = TraitUtils.rewindability(left);
        RewindabilityTrait rightRewindability = TraitUtils.rewindability(right);

        List<Pair<RelTraitSet, List<RelTraitSet>>> pairs = new ArrayList<>();

        pairs.add(Pair.of(nodeTraits.replace(RewindabilityTrait.ONE_WAY),
            List.of(left.replace(RewindabilityTrait.ONE_WAY), right.replace(RewindabilityTrait.ONE_WAY))));

        if (leftRewindability.rewindable() && rightRewindability.rewindable())
            pairs.add(Pair.of(nodeTraits.replace(RewindabilityTrait.REWINDABLE),
                List.of(left.replace(RewindabilityTrait.REWINDABLE), right.replace(RewindabilityTrait.REWINDABLE))));

        return List.copyOf(pairs);
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveDistribution(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        // There are several rules:
        // 1) any join is possible on broadcast or single distribution
        // 2) hash distributed join is possible when join keys are superset of source distribution keys
        // 3) hash and broadcast distributed tables can be joined when join keys equal to hash
        //    distributed table distribution keys and:
        //      3.1) it's a left join and a hash distributed table is at left
        //      3.2) it's a right join and a hash distributed table is at right
        //      3.3) it's an inner join, this case a hash distributed table may be at any side

        RelTraitSet left = inputTraits.get(0), right = inputTraits.get(1);

        List<Pair<RelTraitSet, List<RelTraitSet>>> res = new ArrayList<>();

        IgniteDistribution leftDistr = TraitUtils.distribution(left);
        IgniteDistribution rightDistr = TraitUtils.distribution(right);

        IgniteDistribution left2rightProjectedDistr = leftDistr.apply(buildProjectionMapping(true));
        IgniteDistribution right2leftProjectedDistr = rightDistr.apply(buildProjectionMapping(false));

        RelTraitSet outTraits;
        RelTraitSet leftTraits;
        RelTraitSet rightTraits;

        if (leftDistr == broadcast() && rightDistr == broadcast()) {
            outTraits = nodeTraits.replace(broadcast());
            leftTraits = left.replace(broadcast());
            rightTraits = right.replace(broadcast());
        }
        else {
            outTraits = nodeTraits.replace(single());
            leftTraits = left.replace(single());
            rightTraits = right.replace(single());
        }

        res.add(Pair.of(outTraits, List.of(leftTraits, rightTraits)));

        if (nullOrEmpty(joinInfo.pairs()))
            return List.copyOf(res);

        if (leftDistr.getType() == HASH_DISTRIBUTED && left2rightProjectedDistr != random()) {
            outTraits = nodeTraits.replace(leftDistr);
            leftTraits = left.replace(leftDistr);
            rightTraits = right.replace(left2rightProjectedDistr);

            res.add(Pair.of(outTraits, List.of(leftTraits, rightTraits)));
        }

        if (rightDistr.getType() == HASH_DISTRIBUTED && right2leftProjectedDistr != random()) {
            outTraits = nodeTraits.replace(rightDistr);
            leftTraits = left.replace(right2leftProjectedDistr);
            rightTraits = right.replace(rightDistr);

            res.add(Pair.of(outTraits, List.of(leftTraits, rightTraits)));
        }

        leftTraits = left.replace(hash(joinInfo.leftKeys, DistributionFunction.hash()));
        rightTraits = right.replace(hash(joinInfo.rightKeys, DistributionFunction.hash()));

        outTraits = nodeTraits.replace(hash(joinInfo.leftKeys, DistributionFunction.hash()));
        res.add(Pair.of(outTraits, List.of(leftTraits, rightTraits)));

        outTraits = nodeTraits.replace(hash(joinInfo.rightKeys, DistributionFunction.hash()));
        res.add(Pair.of(outTraits, List.of(leftTraits, rightTraits)));

        return List.copyOf(res);
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCorrelation(RelTraitSet nodeTraits,
        List<RelTraitSet> inTraits) {
        // left correlations
        Set<CorrelationId> corrIds = new HashSet<>(TraitUtils.correlation(inTraits.get(0)).correlationIds());
        // right correlations
        corrIds.addAll(TraitUtils.correlation(inTraits.get(1)).correlationIds());

        return List.of(Pair.of(nodeTraits.replace(CorrelationTrait.correlations(corrIds)), inTraits));
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughCollation(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        // We preserve left collation since it's translated into a nested loop join with an outer loop
        // over a left edge. The code below checks whether a desired collation is possible and requires
        // appropriate collation from the left edge.

        RelCollation collation = TraitUtils.collation(nodeTraits);

        RelTraitSet left = inputTraits.get(0), right = inputTraits.get(1);

        if (collation.equals(RelCollations.EMPTY))
            return Pair.of(nodeTraits,
                List.of(left.replace(RelCollations.EMPTY), right.replace(RelCollations.EMPTY)));

        if (!projectsLeft(collation))
            collation = RelCollations.EMPTY;
        else if (joinType == RIGHT || joinType == JoinRelType.FULL) {
            for (RelFieldCollation field : collation.getFieldCollations()) {
                if (RelFieldCollation.NullDirection.LAST != field.nullDirection) {
                    collation = RelCollations.EMPTY;
                    break;
                }
            }
        }

        return Pair.of(nodeTraits.replace(collation),
            List.of(left.replace(collation), right.replace(RelCollations.EMPTY)));
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughDistribution(
        RelTraitSet nodeTraits,
        List<RelTraitSet> inputTraits
    ) {
        // Tere are several rules:
        // 1) any join is possible on broadcast or single distribution
        // 2) hash distributed join is possible when join keys equal to source distribution keys
        // 3) hash and broadcast distributed tables can be joined when join keys equal to hash
        //    distributed table distribution keys and:
        //      3.1) it's a left join and a hash distributed table is at left
        //      3.2) it's a right join and a hash distributed table is at right
        //      3.3) it's an inner join, this case a hash distributed table may be at any side

        RelTraitSet left = inputTraits.get(0), right = inputTraits.get(1);

        IgniteDistribution distribution = TraitUtils.distribution(nodeTraits);

        RelDistribution.Type distrType = distribution.getType();
        switch (distrType) {
            case BROADCAST_DISTRIBUTED:
            case SINGLETON:
                return Pair.of(nodeTraits, Commons.transform(inputTraits, t -> t.replace(distribution)));

            case HASH_DISTRIBUTED:
            case RANDOM_DISTRIBUTED:
                // Such join may be replaced as a cross join with a filter uppon it.
                // It's impossible to get random or hash distribution from a cross join.
                if (nullOrEmpty(joinInfo.pairs()))
                    break;

                // We cannot provide random distribution without unique constrain on join keys,
                // so, we require hash distribution (wich satisfies random distribution) instead.
                DistributionFunction function = distrType == HASH_DISTRIBUTED
                    ? distribution.function()
                    : DistributionFunction.hash();

                IgniteDistribution outDistr = hash(joinInfo.leftKeys, function);

                if (distrType != HASH_DISTRIBUTED || outDistr.satisfies(distribution)) {
                    return Pair.of(nodeTraits.replace(outDistr),
                        List.of(left.replace(outDistr), right.replace(hash(joinInfo.rightKeys, function))));
                }

                break;

            default:
                // NO-OP
        }

        return Pair.of(nodeTraits.replace(single()), Commons.transform(inputTraits, t -> t.replace(single())));
    }

    /** {@inheritDoc} */
    @Override public double estimateRowCount(RelMetadataQuery mq) {
        return Util.first(joinRowCount(mq, this), 1D);
    }

    /** */
    protected boolean projectsLeft(RelCollation collation) {
        int leftFieldCount = getLeft().getRowType().getFieldCount();
        for (int field : RelCollations.ordinals(collation)) {
            if (field >= leftFieldCount)
                return false;
        }
        return true;
    }

    /** Creates mapping from left join keys to the right and vice versa with regards to {@code left2Right}. */
    protected Mappings.TargetMapping buildProjectionMapping(boolean left2Right) {
        ImmutableIntList sourceKeys = left2Right ? joinInfo.leftKeys : joinInfo.rightKeys;
        ImmutableIntList targetKeys = left2Right ? joinInfo.rightKeys : joinInfo.leftKeys;

        Map<Integer, Integer> keyMap = new HashMap<>();
        for (int i = 0; i < joinInfo.leftKeys.size(); i++)
            keyMap.put(sourceKeys.get(i), targetKeys.get(i));

        return Mappings.target(
            keyMap,
            (left2Right ? left : right).getRowType().getFieldCount(),
            (left2Right ? right : left).getRowType().getFieldCount()
        );
    }
}
