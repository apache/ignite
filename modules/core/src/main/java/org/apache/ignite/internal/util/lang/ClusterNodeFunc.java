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
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.util.F0;
import org.apache.ignite.internal.util.lang.gridfunc.ContainsNodeIdsPredicate;
import org.apache.ignite.internal.util.lang.gridfunc.EqualsClusterNodeIdPredicate;
import org.apache.ignite.internal.util.lang.gridfunc.IsAllPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

/**
 * Contains factory and utility methods for {@code closures}, {@code predicates}, and {@code tuples}.
 * It also contains functional style collection comprehensions.
 * <p>
 * Most of the methods in this class can be divided into two groups:
 * <ul>
 * <li><b>Factory</b> higher-order methods for closures, predicates and tuples, and</li>
 * <li><b>Utility</b> higher-order methods for processing collections with closures and predicates.</li>
 * </ul>
 * Note that contrary to the usual design this class has substantial number of
 * methods (over 200). This design is chosen to simulate a global namespace
 * (like a {@code Predef} in Scala) to provide general utility and functional
 * programming functionality in a shortest syntactical context using {@code F}
 * typedef.
 * <p>
 * Also note, that in all methods with predicates, null predicate has a {@code true} meaning. So does
 * the empty predicate array.
 */
@SuppressWarnings("unchecked")
public class ClusterNodeFunc {
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

        return F.viewReadOnly(nodes, ClusterNode::id);
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

        return F.viewReadOnly(nodes, BaselineNode::consistentId);
    }

    /**
     * Creates grid node predicate evaluating on the given node ID.
     *
     * @param nodeId Node ID for which returning predicate will evaluate to {@code true}.
     * @return Grid node predicate evaluating on the given node ID.
     * @see F#idForNodeId(UUID)
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
     * @see F#idForNodeId(UUID)
     * @see #nodeIds(Collection)
     */
    public static <T extends ClusterNode> IgnitePredicate<T> nodeForNodeIds(@Nullable final Collection<UUID>
        nodeIds) {
        if (F.isEmpty(nodeIds))
            return F.alwaysFalse();

        return new ContainsNodeIdsPredicate<>(nodeIds);
    }

    /**
     * Get a predicate that evaluates to {@code true} if each of its component predicates
     * evaluates to {@code true}. The components are evaluated in order they are supplied.
     * Evaluation will be stopped as soon as first predicate evaluates to {@code false}.
     * Passed in predicates are NOT copied. If no predicates are passed in the returned
     * predicate will always evaluate to {@code false}.
     *
     * @param ps Passed in predicate. If none provided - always-{@code false} predicate is
     *      returned.
     * @param <T> Type of the free variable, i.e. the element the predicate is called on.
     * @return Predicate that evaluates to {@code true} if each of its component predicates
     *      evaluates to {@code true}.
     */
    @SuppressWarnings({"unchecked"})
    public static <T> IgnitePredicate<T> and(@Nullable final IgnitePredicate<? super T>... ps) {
        if (F.isEmpty(ps))
            return F.alwaysTrue();

        if (F.isAlwaysFalse(ps))
            return F.alwaysFalse();

        if (F.isAlwaysTrue(ps))
            return F.alwaysTrue();

        if (F0.isAllNodePredicates(ps)) {
            Set<UUID> ids = new HashSet<>();

            for (IgnitePredicate<? super T> p : ps) {
                if (p != null) {
                    Collection<UUID> list = ((GridNodePredicate)p).nodeIds();

                    if (ids.isEmpty())
                        ids.addAll(list);
                    else
                        ids.retainAll(list);
                }
            }

            // T must be <T extends ClusterNode>.
            return (IgnitePredicate<T>)new GridNodePredicate(ids);
        }
        else
            return new IsAllPredicate<>(ps);
    }

    /**
     * Compares two maps. Unlike {@code java.util.AbstractMap#equals(...)} method this implementation
     * checks not only entry sets, but also the keys. Some optimization checks are also used.
     *
     * @param m1 First map to check.
     * @param m2 Second map to check
     * @return {@code True} is maps are equal, {@code False} otherwise.
     */
    public static <K, V> boolean eqNotOrdered(@Nullable Map<K, V> m1, @Nullable Map<K, V> m2) {
        if (m1 == m2)
            return true;

        if (m1 == null || m2 == null)
            return false;

        if (m1.size() != m2.size())
            return false;

        for (Map.Entry<K, V> e : m1.entrySet()) {
            V v1 = e.getValue();
            V v2 = m2.get(e.getKey());

            if (v1 == v2)
                continue;

            if (v1 == null || v2 == null)
                return false;

            if (v1 instanceof Collection && v2 instanceof Collection) {
                if (!F.eqNotOrdered((Collection)v1, (Collection)v2))
                    return false;
            }
            else {
                if (v1 instanceof Map && v2 instanceof Map) {
                    if (!eqNotOrdered((Map)v1, (Map)v2))
                        return false;
                }
                else {
                    if (!(v1.getClass().isArray() ? arrayEq(v1, v2) : Objects.equals(v1, v2)))
                        return false;
                }
            }
        }

        return true;
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
