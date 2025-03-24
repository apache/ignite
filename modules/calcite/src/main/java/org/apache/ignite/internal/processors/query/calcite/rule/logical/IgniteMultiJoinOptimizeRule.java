/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.rule.logical;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.LoptMultiJoin;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.calcite.util.mapping.Mappings.TargetMapping;
import org.apache.ignite.internal.processors.query.calcite.hint.HintDefinition;
import org.apache.ignite.internal.util.collection.IntMap;
import org.apache.ignite.internal.util.collection.IntRWHashMap;
import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.IgniteUtils.isPow2;

/**
 * A rule for optimization of flat multi-join using a bushy trees.
 *
 * <p>This is an implementation of subset-driven enumeration algorithm (By T. Neumann. and G. Moerkotte. Analysis of
 * Two Existing and One New Dynamic Programming Algorithm for the Generation of Optimal Bushy Join Trees without Cross Products).
 *
 * <p>The main loop enumerates all relations and guarantees that for every emitted set {@code S} any split of this set
 * will produce subset which have been already processed.
 *
 * <p>The inner loop enumerates all possible splits of given subset {@code S} on disjoint subset
 * {@code lhs} and {@code rhs} such that {@code lhs ∪ rhs = S} (B. Vance and D. Maier. Rapid bushy join-order optimization
 * with cartesian products).
 *
 * <p>Finally, if the initial set is not connected, the algorithm crates cartesian join from the best plan until
 * all the relations are connected.
 *
 * <p>Limitations:
 * <ol>
 *     <li>Only INNER joins are supported</li>
 *     <li>Disjunctive predicate (condition) is not considered as connections.</li>
 *     <li>The maximal number of relations is 20. The value is based on time and memory consumption.</li>
 * </ol>
 */
@Value.Enclosing
public class IgniteMultiJoinOptimizeRule extends RelRule<IgniteMultiJoinOptimizeRule.Config> implements TransformationRule {
    /** */
    public static final IgniteMultiJoinOptimizeRule INSTANCE = new IgniteMultiJoinOptimizeRule(Config.DEFAULT);

    /** */
    private static final int MAX_JOIN_SIZE = 20;

    /** Vertexes comparator. Better vertex incorporate more relations or costs less. */
    private static final Comparator<Vertex> VERTEX_COMPARATOR = Comparator.<Vertex>comparingInt(v -> v.size).reversed()
        .thenComparingDouble(v -> v.cost);

    /** Creates the rule. */
    private IgniteMultiJoinOptimizeRule(Config cfg) {
        super(cfg);
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        MultiJoin multiJoinRel = call.rel(0);

        // Currently, only INNER JOIN is supported.
        if (multiJoinRel.isFullOuterJoin())
            return;

        int relCnt = multiJoinRel.getInputs().size();

        if (relCnt > MAX_JOIN_SIZE)
            return;

        for (JoinRelType joinType : multiJoinRel.getJoinTypes()) {
            if (joinType != JoinRelType.INNER)
                return;
        }

        LoptMultiJoin multiJoin = new LoptMultiJoin(multiJoinRel);

        RexBuilder rexBuilder = multiJoinRel.getCluster().getRexBuilder();
        RelBuilder relBuilder = call.builder();
        RelMetadataQuery callMeta = call.getMetadataQuery();

        List<RexNode> unusedConditions = new ArrayList<>();

        // Edges by vertex (rel.) number in pow2 starting.
        IntMap<List<Edge>> edges = collectEdges(multiJoin, unusedConditions);
        BitSet connections = new BitSet(1 << relCnt);
        IntMap<Vertex> bestPlan = new IntRWHashMap<>();

        int fldMappingOffset = 0;
        int relNumPow2 = 1;

        for (RelNode input : multiJoinRel.getInputs()) {
            TargetMapping fldMapping = Mappings.offsetSource(
                Mappings.createIdentity(input.getRowType().getFieldCount()),
                fldMappingOffset,
                multiJoin.getNumTotalFields()
            );

            bestPlan.put(relNumPow2, new Vertex(relNumPow2, callMeta.getRowCount(input), input, fldMapping));

            connections.set(relNumPow2);

            relNumPow2 <<= 1;

            fldMappingOffset += input.getRowType().getFieldCount();
        }

        Vertex bestPlanSoFar = null;

        for (int s = 0b11, cnt = 1 << relCnt; s < cnt; ++s) {
            // Pow2-value refers to an initial relation. They are already processed at the first phase.
            if (isPow2(s))
                continue;

            int lhs = Integer.lowestOneBit(s);

            while (lhs < (s / 2) + 1) {
                int rhs = s - lhs;

                List<Edge> edges0 = connections.get(lhs) && connections.get(rhs)
                    ? findEdges(lhs, rhs, edges)
                    : Collections.emptyList();

                if (!edges0.isEmpty()) {
                    connections.set(s);

                    Vertex leftPlan = bestPlan.get(lhs);
                    Vertex rightPlan = bestPlan.get(rhs);

                    Vertex newPlan = createJoin(leftPlan, rightPlan, edges0, callMeta, relBuilder, rexBuilder);

                    Vertex curBestPlan = bestPlan.get(s);

                    if (curBestPlan == null || curBestPlan.cost > newPlan.cost) {
                        bestPlan.put(s, newPlan);

                        bestPlanSoFar = best(bestPlanSoFar, newPlan);
                    }

                    aggregateEdges(edges, lhs, rhs);
                }

                lhs = s & (lhs - s);
            }
        }

        int allRelationsMask = (1 << relCnt) - 1;

        Vertex res;

        if (bestPlanSoFar == null || bestPlanSoFar.id != allRelationsMask)
            res = composeCartesianJoin(allRelationsMask, bestPlan, edges, bestPlanSoFar, callMeta, relBuilder, rexBuilder);
        else
            res = bestPlanSoFar;

        RelNode result = relBuilder
            .push(res.rel)
            .filter(RexUtil.composeConjunction(rexBuilder, unusedConditions).accept(new RexPermuteInputsShuttle(res.mapping, res.rel)))
            .project(relBuilder.fields(res.mapping))
            .build();

        call.transformTo(result);
    }

    /** */
    private static void aggregateEdges(IntMap<List<Edge>> edges, int lhs, int rhs) {
        int id = lhs | rhs;

        if (!edges.containsKey(id)) {
            Set<Edge> used = Collections.newSetFromMap(new IdentityHashMap<>());

            List<Edge> union = new ArrayList<>(edges.computeIfAbsent(lhs, k -> Collections.emptyList()));

            used.addAll(union);

            edges.computeIfAbsent(rhs, k -> Collections.emptyList()).forEach(edge -> {
                if (used.add(edge))
                    union.add(edge);
            });

            if (!union.isEmpty())
                edges.put(id, union);
        }
    }

    /** */
    private static Vertex composeCartesianJoin(
        int allRelationsMask,
        IntMap<Vertex> bestPlan,
        IntMap<List<Edge>> edges,
        @Nullable Vertex bestSoFar,
        RelMetadataQuery mq,
        RelBuilder relBuilder,
        RexBuilder rexBuilder
    ) {
        List<Vertex> options;

        if (bestSoFar != null) {
            options = new ArrayList<>();

            for (Vertex option : bestPlan.values()) {
                if ((option.id & bestSoFar.id) == 0)
                    options.add(option);
            }
        }
        else
            options = new ArrayList<>(bestPlan.values());

        options.sort(VERTEX_COMPARATOR);

        Iterator<Vertex> it = options.iterator();

        if (bestSoFar == null)
            bestSoFar = it.next();

        while (it.hasNext() && bestSoFar.id != allRelationsMask) {
            Vertex input = it.next();

            if ((bestSoFar.id & input.id) != 0)
                continue;

            List<Edge> edges0 = findEdges(bestSoFar.id, input.id, edges);

            aggregateEdges(edges, bestSoFar.id, input.id);

            bestSoFar = createJoin(bestSoFar, input, edges0, mq, relBuilder, rexBuilder);
        }

        assert bestSoFar.id == allRelationsMask;

        return bestSoFar;
    }

    /** */
    private static Vertex best(@Nullable Vertex curBest, Vertex candidate) {
        return curBest == null || VERTEX_COMPARATOR.compare(curBest, candidate) > 0
            ? candidate
            : curBest;
    }

    /** */
    private static IntMap<List<Edge>> collectEdges(LoptMultiJoin multiJoin, List<RexNode> unusedConditions) {
        IntMap<List<Edge>> edges = new IntRWHashMap();

        for (RexNode joinCondition : multiJoin.getJoinFilters()) {
            // Involved rel numbers starting from 0.
            int[] joinRelNums = multiJoin.getFactorsRefByJoinFilter(joinCondition).toArray();

            // Skip conditions involving a single table. The main loop looks only for edges connecting two subsets.
            // A condition referring to a single table never meet this requirement. Also, for inner join such conditions must
            // be pushed down already.
            if (joinRelNums.length < 2) {
                unusedConditions.add(joinCondition);

                continue;
            }

            // TODO: support with adoption of IGNITE-24210
            if (joinCondition.isA(SqlKind.OR)) {
                unusedConditions.add(joinCondition);

                continue;
            }

            int connectedInputs = 0;

            for (int i : joinRelNums)
                connectedInputs |= 1 << i;

            Edge edge = new Edge(connectedInputs, joinCondition);

            for (int i : joinRelNums)
                edges.computeIfAbsent(1 << i, k -> new ArrayList<>()).add(edge);
        }

        return edges;
    }

    /** */
    private static Vertex createJoin(
        Vertex lhs,
        Vertex rhs,
        List<Edge> edges,
        RelMetadataQuery metadataQry,
        RelBuilder relBuilder,
        RexBuilder rexBuilder
    ) {
        List<RexNode> conditions = new ArrayList<>();

        edges.forEach(e -> conditions.add(e.condition));

        double leftSize = metadataQry.getRowCount(lhs.rel);
        double rightSize = metadataQry.getRowCount(rhs.rel);

        Vertex majorFactor;
        Vertex minorFactor;

        // Right side will probably be materialized. Let's put bigger input on left side.
        if (leftSize <= rightSize) {
            majorFactor = lhs;
            minorFactor = rhs;
        }
        else {
            majorFactor = rhs;
            minorFactor = lhs;
        }

        TargetMapping fldMapping = Mappings.merge(
            majorFactor.mapping,
            Mappings.offsetTarget(minorFactor.mapping, majorFactor.rel.getRowType().getFieldCount())
        );

        RexNode newCondition = RexUtil.composeConjunction(rexBuilder, conditions)
            .accept(new RexPermuteInputsShuttle(fldMapping, majorFactor.rel, minorFactor.rel));

        RelNode join = relBuilder.push(majorFactor.rel).push(minorFactor.rel).join(JoinRelType.INNER, newCondition).build();

        assert ((Hintable)join).getHints().stream().noneMatch(h -> h.hintName.equals(HintDefinition.ENFORCE_JOIN_ORDER.name()))
            : "Joins with enforced order must not be optimized.";

        double cost = metadataQry.getRowCount(join) + lhs.cost + rhs.cost;

        return new Vertex(lhs.id | rhs.id, cost, join, fldMapping);
    }

    /**
     * Finds all edges which connect given subsets.
     *
     * <p>Returned edges will satisfy two conditions:<ol>
     *     <li>At least one relation from each side will be covered by edge.</li>
     *     <li>No any other relations outside of {@code lhs ∪ rhs} will be covered by edge.</li>
     * </ol>
     *
     * @param lhs Left subtree.
     * @param rhs Right subtree.
     * @param edges All the edges.
     * @return Edges connecting given subtrees.
     */
    private static List<Edge> findEdges(int lhs, int rhs, IntMap<List<Edge>> edges) {
        List<Edge> result = new ArrayList<>();

        List<Edge> fromLeft = edges.computeIfAbsent(lhs, k -> Collections.emptyList());

        for (Edge edge : fromLeft) {
            int requiredInputs = edge.connectedInputs & ~lhs;

            if (requiredInputs == 0 || edge.connectedInputs == requiredInputs)
                continue;

            requiredInputs &= ~rhs;

            if (requiredInputs == 0)
                result.add(edge);
        }

        return result;
    }

    /** */
    private static final class Edge {
        /** Bitmap of all inputs connected by condition. */
        private final int connectedInputs;

        /** Join condition. */
        private final RexNode condition;

        /** */
        private Edge(int connectedInputs, RexNode condition) {
            this.connectedInputs = connectedInputs;

            this.condition = condition;
        }
    }

    /** Root vertex (rel) of a tree. */
    private static class Vertex {
        /** Bitmap of inputs joined together with current vertex. */
        private final int id;

        /** Number of inputs joined together with current vertex. */
        private final byte size;

        /** Total cost of this tree. */
        private final double cost;

        /** Join mapping. */
        private final TargetMapping mapping;

        /** Node of this vertex. */
        private final RelNode rel;

        /** */
        private Vertex(int id, double cost, RelNode rel, TargetMapping mapping) {
            this.id = id;
            this.size = (byte)Integer.bitCount(id);
            this.cost = cost;
            this.rel = rel;
            this.mapping = mapping;
        }
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface Config extends RelRule.Config {
        /** */
        Config DEFAULT = ImmutableIgniteMultiJoinOptimizeRule.Config.of()
            .withOperandSupplier(b -> b.operand(MultiJoin.class).anyInputs());

        /** {@inheritDoc} */
        @Override default IgniteMultiJoinOptimizeRule toRule() {
            return INSTANCE;
        }
    }
}
