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

package org.apache.ignite.internal.util.lang;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.util.F0;
import org.apache.ignite.internal.util.lang.gridfunc.ContainsNodeIdsPredicate;
import org.apache.ignite.internal.util.lang.gridfunc.EqualsClusterNodeIdPredicate;
import org.apache.ignite.internal.util.lang.gridfunc.EqualsUuidPredicate;
import org.apache.ignite.internal.util.lang.gridfunc.HasEqualIdPredicate;
import org.apache.ignite.internal.util.lang.gridfunc.HasNotEqualIdPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

/**
 * Contains utility methods for {@code ClusterNode}, and {@code BaselineNode}.
 * <p>
 * Most of the methods in this class can be divided into two groups:
 * <ul>
 * <li><b>Factory</b> higher-order methods for predicates.</li>
 * <li><b>Utility</b> higher-order methods for processing collections with predicates.</li>
 * </ul>
 */
public class ClusterNodeFunc {
    /** */
    private static final IgniteClosure<ClusterNode, UUID> NODE2ID = ClusterNode::id;

    /** */
    private static final IgniteClosure<BaselineNode, Object> NODE2CONSISTENTID =
        BaselineNode::consistentId;

    /**
     * Gets predicate that evaluates to {@code true} only for given local node ID.
     *
     * @param locNodeId Local node ID.
     * @param <T> Type of the node.
     * @return Return {@code true} only for the node with given local node ID.
     */
    public static <T extends ClusterNode> IgnitePredicate<T> localNode(final UUID locNodeId) {
        return new HasEqualIdPredicate<>(locNodeId);
    }

    /**
     * Gets predicate that evaluates to {@code false} for given local node ID.
     *
     * @param locNodeId Local node ID.
     * @param <T> Type of the node.
     * @return Return {@code false} for the given local node ID.
     */
    public static <T extends ClusterNode> IgnitePredicate<T> remoteNodes(final UUID locNodeId) {
        return new HasNotEqualIdPredicate<>(locNodeId);
    }

    /**
     * Convenient utility method that returns collection of node IDs for a given
     * collection of grid nodes.
     * <p>
     * Note that this method doesn't create a new collection but simply iterates
     * over the input one.
     *
     * @param nodes Collection of grid nodes.
     * @return Collection of node IDs for given collection of grid nodes.
     */
    public static Collection<UUID> nodeIds(@Nullable Collection<? extends ClusterNode> nodes) {
        if (nodes == null || nodes.isEmpty())
            return Collections.emptyList();

        return F.viewReadOnly(nodes, node2id());
    }

    /**
     * Convenient utility method that returns collection of node consistent IDs for a given
     * collection of grid nodes.
     * <p>
     * Note that this method doesn't create a new collection but simply iterates
     * over the input one.
     *
     * @param nodes Collection of grid nodes.
     * @return Collection of node consistent IDs for given collection of grid nodes.
     */
    public static Collection<Object> nodeConsistentIds(@Nullable Collection<? extends BaselineNode> nodes) {
        if (nodes == null || nodes.isEmpty())
            return Collections.emptyList();

        return F.viewReadOnly(nodes, NODE2CONSISTENTID);
    }

    /**
     * Gets closure which converts node to node ID.
     *
     * @return Closure which converts node to node ID.
     */
    public static IgniteClosure<? super ClusterNode, UUID> node2id() {
        return NODE2ID;
    }

    /**
     * Creates grid node predicate evaluating on the given node ID.
     *
     * @param nodeId Node ID for which returning predicate will evaluate to {@code true}.
     * @return Grid node predicate evaluating on the given node ID.
     * @see ClusterNodeFunc#idForNodeId(UUID)
     * @see #nodeIds(Collection)
     */
    public static <T extends ClusterNode> IgnitePredicate<T> nodeForNodeId(final UUID nodeId) {
        A.notNull(nodeId, "nodeId");

        return new EqualsClusterNodeIdPredicate<>(nodeId);
    }

    /**
     * Creates grid node predicate evaluating on the given node IDs.
     *
     * @param nodeIds Collection of node IDs.
     * @return Grid node predicate evaluating on the given node IDs.
     * @see ClusterNodeFunc#idForNodeId(UUID)
     * @see #nodeIds(Collection)
     */
    public static <T extends ClusterNode> IgnitePredicate<T> nodeForNodeIds(@Nullable final Collection<UUID>
        nodeIds) {
        if (F.isEmpty(nodeIds))
            return F.alwaysFalse();

        return new ContainsNodeIdsPredicate<>(nodeIds);
    }

    /**
     * Creates {@link UUID} predicate evaluating on the given node ID.
     *
     * @param nodeId Node ID for which returning predicate will evaluate to {@code true}.
     * @return {@link UUID} predicate evaluating on the given node ID.
     * @see #nodeForNodeId(UUID)
     * @see #nodeIds(Collection)
     */
    public static IgnitePredicate<UUID> idForNodeId(final UUID nodeId) {
        A.notNull(nodeId, "nodeId");

        return new EqualsUuidPredicate(nodeId);
    }

    /**
     * Compares two {@link org.apache.ignite.cluster.ClusterNode} instances for equality.
     * <p>
     * Since introduction of {@link org.apache.ignite.cluster.ClusterNode} in Apache Ignite 3.0 the semantic of equality between
     * grid nodes has changed. Since rich node wraps thin node instance and in the same time
     * implements {@link org.apache.ignite.cluster.ClusterNode} interface, the proper semantic of comparing two grid node is
     * to ignore their runtime types and compare only by their IDs. This method implements this logic.
     * <p>
     * End users rarely, if ever, need to directly compare two grid nodes for equality. This method is
     * intended primarily for discovery SPI developers that provide implementations of {@link org.apache.ignite.cluster.ClusterNode}
     * interface.
     *
     * @param n1 Grid node 1.
     * @param n2 Grid node 2
     * @return {@code true} if two grid node have the same IDs (ignoring their runtime types),
     *      {@code false} otherwise.
     */
    public static boolean eqNodes(Object n1, Object n2) {
        return n1 == n2 || !(n1 == null || n2 == null) && !(!(n1 instanceof ClusterNode) || !(n2 instanceof ClusterNode))
            && ((ClusterNode)n1).id().equals(((ClusterNode)n2).id());
    }

    /**
     * Check for arrays equality.
     *
     * @param a1 Value 1.
     * @param a2 Value 2.
     * @return {@code True} if arrays equal.
     */
    public static boolean arrayEq(Object a1, Object a2) {
        if (a1 == a2)
            return true;

        if (a1 == null || a2 == null)
            return a1 != null || a2 != null;

        if (a1.getClass() != a2.getClass())
            return false;

        if (a1 instanceof byte[])
            return Arrays.equals((byte[])a1, (byte[])a2);
        else if (a1 instanceof boolean[])
            return Arrays.equals((boolean[])a1, (boolean[])a2);
        else if (a1 instanceof short[])
            return Arrays.equals((short[])a1, (short[])a2);
        else if (a1 instanceof char[])
            return Arrays.equals((char[])a1, (char[])a2);
        else if (a1 instanceof int[])
            return Arrays.equals((int[])a1, (int[])a2);
        else if (a1 instanceof long[])
            return Arrays.equals((long[])a1, (long[])a2);
        else if (a1 instanceof float[])
            return Arrays.equals((float[])a1, (float[])a2);
        else if (a1 instanceof double[])
            return Arrays.equals((double[])a1, (double[])a2);
        else if (BinaryUtils.isBinaryArray(a1))
            return a1.equals(a2);

        return Arrays.deepEquals((Object[])a1, (Object[])a2);
    }
}
