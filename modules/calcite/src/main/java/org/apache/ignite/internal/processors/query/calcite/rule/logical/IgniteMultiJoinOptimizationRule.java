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
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.IgniteUtils.isPow2;

/**
 * A rule for optimization of multi-join queries using a bushy join trees.
 *
 * <p>This is an implementation of subset-driven enumeration algorithm (By T. Neumann. and G. Moerkotte. Analysis of
 * Two Existing and One New Dynamic Programming Algorithm for the Generation of Optimal Bushy Join Trees without Cross Products).
 *
 * <p>The main loop enumerates all relation subsets and guarantees that for every emitted set {@code S} any split of this set
 * will produce subset which have been already processed.
 *
 * <p>The inner loop enumerates all possible splits of given subset {@code S} on disjoint subset
 * {@code leftSubTree} and {@code rightSubTree} such that {@code leftSubTree ∪ rightSubTree = S} (B. Vance and D. Maier.
 * Rapid bushy join-order optimization with cartesian products).
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
public class IgniteMultiJoinOptimizationRule extends RelRule<IgniteMultiJoinOptimizationRule.Config> implements TransformationRule {
    /** */
    public static final IgniteMultiJoinOptimizationRule INSTANCE = new IgniteMultiJoinOptimizationRule(Config.DEFAULT);

    /** */
    private static final int MAX_JOIN_SIZE = 20;

    /** Vertexes comparator. Better vertex is the one that incorporate more relations, or costs less. */
    private static final Comparator<Vertex> VERTEX_COMPARATOR = Comparator.<Vertex>comparingInt(v -> v.size).reversed()
        .thenComparingDouble(v -> v.cost);

    /** Creates a MultiJoinOptimizeBushyRule. */
    private IgniteMultiJoinOptimizationRule(Config cfg) {
        super(cfg);
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        MultiJoin multiJoinRel = call.rel(0);

        int relNum = multiJoinRel.getInputs().size();

        if (relNum > MAX_JOIN_SIZE)
            return;

        // Currently, only INNER JOIN is supported.
        if (multiJoinRel.isFullOuterJoin())
            return;

        for (JoinRelType joinType : multiJoinRel.getJoinTypes()) {
            if (joinType != JoinRelType.INNER)
                return;
        }

        IgniteLogger log = Commons.context(multiJoinRel).logger();

        if (log.isDebugEnabled())
            log.debug("Optimizing multi-join " + RelOptUtil.toString(multiJoinRel));

        LoptMultiJoin multiJoin = new LoptMultiJoin(multiJoinRel);

        RexBuilder rexBuilder = multiJoinRel.getCluster().getRexBuilder();
        RelBuilder relBuilder = call.builder();
        RelMetadataQuery mq = call.getMetadataQuery();

        List<RexNode> unusedConditions = new ArrayList<>();

        // Edges by vertex (rel.) number in pow2 starting with 1.
        Map<Integer, List<Edge>> edges = collectEdges(multiJoin, unusedConditions);
        Map<Integer, Vertex> bestPlan = new HashMap<>();
        BitSet connections = new BitSet(1 << relNum);

        // Rel number in pow2.
        int relId = 0b1;

        // Offset in the mapping.
        int fieldOffset = 0;

        for (RelNode input : multiJoinRel.getInputs()) {
            // Only flat multy-joins expected.
            assert !MultiJoin.class.isAssignableFrom(input.getClass());

            TargetMapping mapping = Mappings.offsetSource(
                Mappings.createIdentity(input.getRowType().getFieldCount()),
                fieldOffset,
                multiJoin.getNumTotalFields()
            );

            bestPlan.put(relId, new Vertex(relId, mq.getRowCount(input), input, mapping));

            connections.set(relId);

            relId <<= 1;

            fieldOffset += input.getRowType().getFieldCount();
        }

        Vertex bestSoFar = null;

        for (int set = 0b11, entyreSet = 1 << relNum; set < entyreSet; ++set) {
            // Pow2-value is the initial relations. They are already processed at the first phase.
            if (isPow2(set))
                continue;

            int leftSubVrtxNum = Integer.lowestOneBit(set);

            while (leftSubVrtxNum < (set / 2) + 1) {
                int rightSubVrtxNum = set - leftSubVrtxNum;

                List<Edge> edges0 = connections.get(leftSubVrtxNum) && connections.get(rightSubVrtxNum)
                    ? findEdges(leftSubVrtxNum, rightSubVrtxNum, edges)
                    : List.of();

                if (!edges0.isEmpty()) {
                    connections.set(set);

                    Vertex leftPlan = bestPlan.get(leftSubVrtxNum);
                    Vertex rightPlan = bestPlan.get(rightSubVrtxNum);

                    Vertex newPlan = createJoin(leftPlan, rightPlan, edges0, mq, relBuilder, rexBuilder);

                    Vertex curBestPlan = bestPlan.get(set);

                    if (curBestPlan == null || curBestPlan.cost > newPlan.cost) {
                        bestPlan.put(set, newPlan);

                        bestSoFar = chooseBest(bestSoFar, newPlan);
                    }

                    aggregateEdges(edges, leftSubVrtxNum, rightSubVrtxNum);
                }

                leftSubVrtxNum = set & (leftSubVrtxNum - set);
            }
        }

        int allRelationsMask = (1 << relNum) - 1;

        Vertex best;

        if (bestSoFar == null || bestSoFar.idMask != allRelationsMask)
            best = composeCartesianJoin(allRelationsMask, bestPlan, edges, bestSoFar, mq, relBuilder, rexBuilder);
        else
            best = bestSoFar;

        RelNode result = relBuilder
            .push(best.rel)
            .filter(RexUtil.composeConjunction(rexBuilder, unusedConditions).accept(new RexPermuteInputsShuttle(best.mapping, best.rel)))
            .project(relBuilder.fields(best.mapping))
            .build();

        call.transformTo(result);
    }

    /** */
    private static void aggregateEdges(Map<Integer, List<Edge>> edges, int leftSubTree, int rightSubTree) {
        int idMask = leftSubTree | rightSubTree;

        if (!edges.containsKey(idMask)) {
            Set<Edge> used = Collections.newSetFromMap(new IdentityHashMap<>());

            List<Edge> union = new ArrayList<>(edges.getOrDefault(leftSubTree, List.of()));

            used.addAll(union);

            edges.getOrDefault(rightSubTree, List.of()).forEach(edge -> {
                if (used.add(edge))
                    union.add(edge);
            });

            if (!union.isEmpty())
                edges.put(idMask, union);
        }
    }

    /** */
    private static Vertex composeCartesianJoin(
        int allRelationsMask,
        Map<Integer, Vertex> bestPlan,
        Map<Integer, List<Edge>> edges,
        @Nullable Vertex bestSoFar,
        RelMetadataQuery mq,
        RelBuilder relBuilder,
        RexBuilder rexBuilder
    ) {
        List<Vertex> options;

        if (bestSoFar != null) {
            options = new ArrayList<>();

            for (Vertex option : bestPlan.values()) {
                if ((option.idMask & bestSoFar.idMask) == 0)
                    options.add(option);
            }
        }
        else
            options = new ArrayList<>(bestPlan.values());

        options.sort(VERTEX_COMPARATOR);

        Iterator<Vertex> it = options.iterator();

        if (bestSoFar == null)
            bestSoFar = it.next();

        while (it.hasNext() && bestSoFar.idMask != allRelationsMask) {
            Vertex input = it.next();

            if ((bestSoFar.idMask & input.idMask) != 0)
                continue;

            List<Edge> edges0 = findEdges(bestSoFar.idMask, input.idMask, edges);

            aggregateEdges(edges, bestSoFar.idMask, input.idMask);

            bestSoFar = createJoin(bestSoFar, input, edges0, mq, relBuilder, rexBuilder);
        }

        assert bestSoFar.idMask == allRelationsMask;

        return bestSoFar;
    }

    /** */
    private static Vertex chooseBest(@Nullable Vertex curBest, Vertex candidate) {
        return curBest == null || VERTEX_COMPARATOR.compare(curBest, candidate) > 0
            ? candidate
            : curBest;
    }

    /** */
    private static Map<Integer, List<Edge>> collectEdges(LoptMultiJoin multiJoin, List<RexNode> unusedConditions) {
        Map<Integer, List<Edge>> edges = new HashMap<>();

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

            int inputsMask = 0;

            for (int i : joinRelNums)
                inputsMask |= 1 << i;

            Edge edge = new Edge(inputsMask, joinCondition);

            for (int i : joinRelNums)
                edges.computeIfAbsent(1 << i, k -> new ArrayList<>()).add(edge);
        }

        return edges;
    }

    /** */
    private static Vertex createJoin(
        Vertex leftSubTree,
        Vertex rightSubTree,
        List<Edge> edges,
        RelMetadataQuery metadataQry,
        RelBuilder relBuilder,
        RexBuilder rexBuilder
    ) {
        List<RexNode> joinConditions = new ArrayList<>();

        edges.forEach(e -> joinConditions.add(e.joinCondition));

        double leftSize = metadataQry.getRowCount(leftSubTree.rel);
        double rightSize = metadataQry.getRowCount(rightSubTree.rel);

        Vertex majorFactor;
        Vertex minorFactor;

        // Right side will probably be materialized. Let's put bigger input on left side.
        if (leftSize >= rightSize) {
            majorFactor = leftSubTree;
            minorFactor = rightSubTree;
        }
        else {
            majorFactor = rightSubTree;
            minorFactor = leftSubTree;
        }

        TargetMapping mapping = Mappings.merge(
            majorFactor.mapping,
            Mappings.offsetTarget(minorFactor.mapping, majorFactor.rel.getRowType().getFieldCount())
        );

        RexNode condition = RexUtil.composeConjunction(rexBuilder, joinConditions)
            .accept(new RexPermuteInputsShuttle(mapping, majorFactor.rel, minorFactor.rel));

        RelNode join = relBuilder.push(majorFactor.rel).push(minorFactor.rel).join(JoinRelType.INNER, condition).build();

        double curCost = metadataQry.getRowCount(join);

        return new Vertex(leftSubTree.idMask | rightSubTree.idMask, curCost + leftSubTree.cost + rightSubTree.cost, join, mapping);
    }

    /**
     * Finds all edges which connect given subsets.
     *
     * <p>Returned edges will satisfy two conditions:<ol>
     *     <li>At least one relation from each side will be covered by edge.</li>
     *     <li>No any other relations outside of {@code lhs ∪ rhs} will be covered by edge.</li>
     * </ol>
     *
     * @param leftSubTreeMask Left subtree mask.
     * @param rightSubTreeMask Right subtree mask.
     * @param edges All the edges.
     * @return Edges connecting given subtrees.
     */
    private static List<Edge> findEdges(int leftSubTreeMask, int rightSubTreeMask, Map<Integer, List<Edge>> edges) {
        List<Edge> result = new ArrayList<>();

        List<Edge> fromLeft = edges.getOrDefault(leftSubTreeMask, List.of());

        for (Edge edge : fromLeft) {
            int requiredInputsMask = edge.connectedInputsMask & ~leftSubTreeMask;

            if (requiredInputsMask == 0 || edge.connectedInputsMask == requiredInputsMask)
                continue;

            requiredInputsMask &= ~rightSubTreeMask;

            if (requiredInputsMask == 0)
                result.add(edge);
        }

        return result;
    }

    /** */
    private static final class Edge {
        /** Bitmap of all inputs connected by condition. */
        private final int connectedInputsMask;

        /** Join condition. */
        private final RexNode joinCondition;

        /** */
        private Edge(int connectedInputsMask, RexNode joinCondition) {
            this.connectedInputsMask = connectedInputsMask;

            this.joinCondition = joinCondition;
        }
    }

    /** Root vertex (rel) of a tree. */
    private static class Vertex {
        /** Bitmap of inputs joined together with current vertex. */
        private final int idMask;

        /** Number of inputs joined together with current vertex. */
        private final byte size;

        /** Total cost of this tree. */
        private final double cost;

        /** Join mapping. */
        private final TargetMapping mapping;

        /** Node of this vertex. */
        private final RelNode rel;

        /** */
        private Vertex(int idMask, double cost, RelNode rel, TargetMapping mapping) {
            this.idMask = idMask;
            this.size = (byte)Integer.bitCount(idMask);
            this.cost = cost;
            this.rel = rel;
            this.mapping = mapping;
        }
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface Config extends RelRule.Config {
        /** */
        Config DEFAULT = ImmutableIgniteMultiJoinOptimizationRule.Config.of()
            .withOperandSupplier(b -> b.operand(MultiJoin.class).anyInputs());

        /** {@inheritDoc} */
        @Override default IgniteMultiJoinOptimizationRule toRule() {
            return INSTANCE;
        }
    }
}
