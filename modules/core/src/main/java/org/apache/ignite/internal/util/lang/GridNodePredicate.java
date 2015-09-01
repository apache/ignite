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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

/**
 * Convenient node predicate as a separate class. It allows to avoid "dragging" enclosing
 * class's state when predicates are created as anonymous classes in stateful enclosing context.
 * This class is also optimized for evaluation of large number of nodes.
 */
public class GridNodePredicate implements IgnitePredicate<ClusterNode>, Iterable<UUID> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @GridToStringInclude
    private final Set<UUID> ids;

    /** */
    @GridToStringExclude
    private int hash = Integer.MIN_VALUE;

    /**
     * Creates node predicate that evaluates to {@code true} for all
     * provided node IDs. Implementation will not make a defensive copy.
     *
     * @param ids Optional node IDs. If none provided - predicate will always return {@code false}.
     */
    public GridNodePredicate(Set<UUID> ids) {
        assert ids != null;

        this.ids = ids;
    }

    /**
     * Creates node predicate that evaluates to {@code true} for all
     * provided node IDs. Implementation will make a defensive copy.
     *
     * @param ids Optional node IDs. If none provided - predicate will always return {@code false}.
     */
    public GridNodePredicate(@Nullable Collection<UUID> ids) {
        this.ids = F.isEmpty(ids) ? Collections.<UUID>emptySet() : ids.size() == 1 ?
            Collections.singleton(F.first(ids)) : new HashSet<>(ids);
    }

    /**
     * Creates node predicate that evaluates to {@code true} for all
     * provided node IDs. Implementation will make a defensive copy.
     *
     * @param ids Optional node IDs. If none provided - predicate will always return {@code false}.
     */
    public GridNodePredicate(@Nullable UUID... ids) {
        if (F.isEmpty(ids))
            this.ids = Collections.emptySet();
        else if (ids.length == 1)
            this.ids = Collections.singleton(ids[0]);
        else {
            this.ids = U.newHashSet(ids.length);

            Collections.addAll(this.ids, ids);
        }
    }

    /**
     * Creates node predicate that evaluates to {@code true} for all
     * provided nodes. Implementation will make a defensive copy.
     *
     * @param nodes Optional grid nodes. If none provided - predicate
     *      will always return {@code false}.
     */
    public GridNodePredicate(@Nullable ClusterNode... nodes) {
        if (F.isEmpty(nodes))
            ids = Collections.emptySet();
        else if (nodes.length == 1)
            ids = Collections.singleton(nodes[0].id());
        else {
            ids = U.newHashSet(nodes.length);

            for (ClusterNode n : nodes)
                ids.add(n.id());
        }
    }

    /**
     * Gets set of node IDs this predicate is based on. Note that for performance
     * reasons this methods return the internal set that <b>should not</b> be
     * modified by the caller.
     *
     * @return Set of node IDs this predicate is based on.
     */
    public Set<UUID> nodeIds(){
        return ids;
    }

    /** {@inheritDoc} */
    @Override public Iterator<UUID> iterator() {
        return ids.iterator();
    }

    /** {@inheritDoc} */
    @Override public boolean apply(ClusterNode n) {
        assert n != null;

        return ids.contains(n.id());
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        // Allow for multiple hash calculations to avoid
        // synchronization cost. Note that array of IDs don't change.
        if (hash == Integer.MIN_VALUE)
            hash = ids.hashCode();

        return hash;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof GridNodePredicate))
            return false;

        GridNodePredicate it = (GridNodePredicate)o;

        return ids.equals(it.ids);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNodePredicate.class, this);
    }
}