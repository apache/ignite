/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.affinity;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;

/**
 * Node hash resolver which uses generated node ID as node hash value. As new node ID is generated
 * on each node start, this resolver do not provide ability to map keys to the same nodes after restart.
 */
public class GridCacheAffinityNodeIdHashResolver implements GridCacheAffinityNodeHashResolver {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public Object resolve(ClusterNode node) {
        return node.id();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheAffinityNodeIdHashResolver.class, this);
    }
}
