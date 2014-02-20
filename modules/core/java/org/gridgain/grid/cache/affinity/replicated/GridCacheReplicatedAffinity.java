// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.affinity.replicated;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;

import java.util.*;

/**
 * Cache affinity implementation for replicated cache. If filter is provided, then
 * it will be used to further filter the nodes. Otherwise, all cache nodes will be
 * used.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheReplicatedAffinity implements GridCacheAffinity {
    /** Filter. */
    private GridBiPredicate<Integer, GridNode> filter;

    /**
     * Empty constructor.
     */
    public GridCacheReplicatedAffinity() {
        // No-op.
    }

    /**
     * Initializes affinity with given filter. Only nodes that pass the filter will be included.
     *
     * @param filter Affinity filter.
     */
    public GridCacheReplicatedAffinity(GridBiPredicate<Integer, GridNode> filter) {
        this.filter = filter;
    }

    /**
     * Gets optional affinity filter ({@code null} if it has not bee provided).
     *
     * @return Affinity filter.
     */
    public GridBiPredicate<Integer, GridNode> getFilter() {
        return filter;
    }

    /**
     * Sets optional affinity filter.
     *
     * @param filter Affinity filter.
     */
    public void setFilter(GridBiPredicate<Integer, GridNode> filter) {
        this.filter = filter;
    }

    /** {@inheritDoc} */
    @Override public int partition(Object key) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int partitions() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridNode> nodes(final int part, Collection<GridNode> nodes) {
        final GridBiPredicate<Integer, GridNode> filter = this.filter;

        if (filter == null)
            return nodes;

        return F.view(nodes, new GridPredicate<GridNode>() {
            @Override public boolean apply(GridNode n) {
                return filter.apply(part, n);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void removeNode(UUID nodeId) {
        // No-op.
    }
}
