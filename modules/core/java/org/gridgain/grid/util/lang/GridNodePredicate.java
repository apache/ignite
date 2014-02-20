// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.lang;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;
import java.util.*;

/**
 * Convenient node predicate as a separate class. It allows to avoid "dragging" enclosing
 * class's state when predicates are created as anonymous classes in stateful enclosing context.
 * This class is also optimized for evaluation of large number of nodes.
 *
 * @author @java.author
 * @version @java.version
 * @param <T> Type of the predicate.
 */
public class GridNodePredicate<T extends GridNode> extends GridPredicate<T> implements Iterable<UUID> {
    /** */
    @GridToStringInclude
    private UUID[] ids;

    /** */
    @GridToStringExclude
    private int hash = Integer.MIN_VALUE;

    /**
     * Creates node predicate that evaluates to {@code true} for all
     * provided node IDs. Implementation will make a defensive copy.
     *
     * @param ids Optional node IDs. If none provided - predicate will always return {@code false}.
     */
    public GridNodePredicate(@Nullable Collection<UUID> ids) {
        if (!F.isEmpty(ids)) {
            assert ids != null;

            // Implicit defensive copy.
            this.ids = ids.toArray(new UUID[ids.size()]);

            dedup();

            Arrays.sort(this.ids);
        }
    }

    /**
     * Creates node predicate that evaluates to {@code true} for all
     * provided node IDs. No defensive copying will be made.
     *
     * @param ids Optional node IDs. If none provided - predicate will always return {@code false}.
     */
    public GridNodePredicate(@Nullable UUID... ids) {
        if (!F.isEmpty(ids)) {
            assert ids != null;

            // No defensive copy.
            this.ids = ids;

            dedup();

            Arrays.sort(this.ids);
        }
    }

    /**
     * Creates node predicate that evaluates to {@code true} for all
     * provided nodes. Implementation will make a defensive copy.
     *
     * @param nodes Optional grid nodes. If none provided - predicate
     *      will always return {@code false}.
     */
    public GridNodePredicate(@Nullable GridNode... nodes) {
        if (!F.isEmpty(nodes)) {
            assert nodes != null;

            // Implicit defensive copy.
            ids = F.nodeIds(Arrays.asList(nodes)).toArray(new UUID[nodes.length]);

            dedup();

            Arrays.sort(ids);
        }
    }

    /**
     * De-dups array of IDs.
     */
    private void dedup() {
        assert ids != null;

        Set<UUID> set = new GridLeanSet<>();

        set.addAll(Arrays.asList(ids));

        ids = set.toArray(new UUID[set.size()]);
    }

    /**
     * Gets array of node IDs this predicate is based on. Note that for performance
     * reasons this methods return the internal array that <b>should not</b> be
     * modified by the caller.
     *
     * @return Array of node IDs this predicate is based on. Returns {@code null}
     *      if predicate has no IDs.
     */
    public UUID[] nodeIds() {
        return ids;
    }

    /** {@inheritDoc} */
    @Override public Iterator<UUID> iterator() {
        return F.isEmpty(ids) ? F.<UUID>emptyIterator() : Collections.unmodifiableList(Arrays.asList(ids)).iterator();
    }

    /** {@inheritDoc} */
    @Override public boolean apply(GridNode n) {
        assert n != null;

        return !F.isEmpty(ids) && Arrays.binarySearch(ids, n.id()) >= 0;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        // Allow for multiple hash calculations to avoid
        // synchronization cost. Note that array of IDs don't change.
        if (hash == Integer.MIN_VALUE && !F.isEmpty(ids))
            hash = Arrays.hashCode(ids);

        return hash;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof GridNodePredicate))
            return false;

        GridNodePredicate it = (GridNodePredicate)o;

        return !(F.isEmpty(ids) && !F.isEmpty(it.ids)) && !(!F.isEmpty(ids) && F.isEmpty(it.ids)) &&
            F.eqArray(ids, it.ids, true, false);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNodePredicate.class, this);
    }
}
