/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.lang;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;
import java.util.*;

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
